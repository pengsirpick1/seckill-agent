// gateway 包负责 HTTP 入口装配，秒杀业务路由从 M1 开始实现。
package gateway

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"github.com/gin-gonic/gin"
	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/database"
	"github.com/pengtor/seckill-agent/internal/health"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/limiter"
	"github.com/pengtor/seckill-agent/internal/lock"
	"github.com/pengtor/seckill-agent/internal/observability"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/platform/httpserver"
	"github.com/pengtor/seckill-agent/internal/seckill"
	"github.com/pengtor/seckill-agent/pkg/middleware"
	"github.com/pengtor/seckill-agent/pkg/response"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	shutdownTracing, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		_ = shutdownTracing(shutdownContext)
	}()
	db, err := database.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()
	router := gin.New()
	router.Use(gin.Recovery(), middleware.RequestID(), middleware.Tracing(cfg.ServiceName), middleware.Logging(log))
	registry := prometheus.NewRegistry()
	metrics := middleware.NewHTTPMetrics(registry)
	router.Use(metrics.Observe())
	stockMetrics := inventory.NewMetrics(registry)
	seckillMetrics := NewSeckillMetrics(registry)
	limitMetrics := limiter.NewMetrics(registry)
	lockMetrics := lock.NewMetrics(registry)
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = redisClient.Close()
		return err
	}
	defer redisClient.Close()
	locker := lock.NewNoop()
	if cfg.Seckill.LockMode == "etcd" {
		etcdLocker, err := lock.NewEtcd(cfg.Etcd, lockMetrics)
		if err != nil {
			return err
		}
		locker = etcdLocker
	}
	defer locker.Close()
	ids, err := order.NewIDGenerator(int64(cfg.Order.NodeID))
	if err != nil {
		return err
	}
	if cfg.Seckill.LockMode == "etcd" && cfg.Faults.EtcdDelay > 0 {
		locker = lock.DelayLocker{Locker: locker, Delay: cfg.Faults.EtcdDelay}
	}
	if cfg.Faults.RedisDelay > 0 || cfg.Faults.EtcdDelay > 0 {
		log.Warn("development fault injection enabled", "redis_delay_ms", cfg.Faults.RedisDelay.Milliseconds(), "etcd_lock_hold_delay_ms", cfg.Faults.EtcdDelay.Milliseconds())
	}
	service := seckill.NewServiceWithAsyncAndLocker(db, inventory.NewRedisStoreWithFaultDelay(redisClient, stockMetrics, cfg.Faults.RedisDelay), nil, ids, locker)
	rateLimiter := limiter.NewService(limiter.NewLocalBucket(cfg.RateLimit.LocalRate, cfg.RateLimit.LocalBurst), limiter.NewRedisLimiter(redisClient), cfg.RateLimit, limitMetrics)

	probes := health.NewTCPProbes(cfg.MySQL.Address, cfg.Redis.Address, cfg.Kafka.Brokers, cfg.Etcd.Endpoints)
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": cfg.ServiceName})
	})
	router.GET("/readyz", func(c *gin.Context) {
		probeContext, cancel := health.ContextWithTimeout(c.Request.Context(), cfg.DependencyProbe)
		defer cancel()
		result := health.CheckAll(probeContext, probes)
		status := http.StatusOK
		if result.Status != "ok" {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, result)
	})
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
	registerRoutes(router, service, rateLimiter, seckillMetrics, cfg.AppEnv == "development" && cfg.Order.CancelAPIEnabled)

	server := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5e9, IdleTimeout: 60e9}
	log.Info("gateway listening", "address", cfg.HTTPAddr, "environment", cfg.AppEnv)
	return httpserver.Run(ctx, server, cfg.ShutdownTimeout)
}

func registerRoutes(router *gin.Engine, service *seckill.Service, rateLimiter *limiter.Service, metrics *SeckillMetrics, cancelAPIEnabled bool) {
	api := router.Group("/api/v1")
	api.POST("/admin/product", func(c *gin.Context) {
		var input seckill.CreateProductInput
		if err := c.ShouldBindJSON(&input); err != nil {
			response.Error(c, validationError("invalid product payload"))
			return
		}
		product, domainErr := service.CreateProduct(c.Request.Context(), input)
		if domainErr != nil {
			response.Error(c, domainErr)
			return
		}
		response.Success(c, http.StatusCreated, "created", gin.H{"product": product})
	})
	api.POST("/admin/activity", func(c *gin.Context) {
		var input seckill.CreateActivityInput
		if err := c.ShouldBindJSON(&input); err != nil {
			response.Error(c, validationError("invalid activity payload"))
			return
		}
		activity, domainErr := service.CreateActivity(c.Request.Context(), input)
		if domainErr != nil {
			response.Error(c, domainErr)
			return
		}
		response.Success(c, http.StatusCreated, "created", gin.H{"activity": activity})
	})
	api.POST("/admin/activity/:id/warmup", func(c *gin.Context) {
		activityID, err := positiveID(c.Param("id"))
		if err != nil {
			response.Error(c, validationError("activity id must be positive"))
			return
		}
		result, domainErr := service.WarmupActivity(c.Request.Context(), activityID)
		if domainErr != nil {
			response.Error(c, domainErr)
			return
		}
		response.Success(c, http.StatusOK, "warmed", gin.H{"stock": result.Stock, "seeded": result.Seeded})
	})
	api.POST("/seckill", func(c *gin.Context) {
		var input seckill.PurchaseInput
		if err := c.ShouldBindJSON(&input); err != nil {
			response.Error(c, validationError("invalid seckill payload"))
			return
		}
		decision, err := rateLimiter.Allow(c.Request.Context(), input.ActivityID, input.ProductID, input.UserID)
		if err != nil {
			response.Error(c, rateLimitUnavailable(err))
			return
		}
		if !decision.Allowed {
			retryAfter := max(1, int(math.Ceil(decision.RetryAfter.Seconds())))
			c.Header("Retry-After", strconv.Itoa(retryAfter))
			response.Error(c, rateLimitRejected(decision.Scope))
			return
		}
		orderID, domainErr := service.PurchaseAsync(c.Request.Context(), input, c.GetString("request_id"))
		if domainErr != nil {
			metrics.ObserveFailure(domainErr.Code)
			response.Error(c, domainErr)
			return
		}
		metrics.ObserveSuccess()
		// 为兼容已有客户端保留数值字段。Snowflake ID 超出 JavaScript 的精确整数范围，
		// 新 HTTP 客户端可使用无精度损失的字符串形式继续执行订单操作。
		response.Success(c, http.StatusAccepted, "accepted", gin.H{"order_id": orderID, "order_id_string": strconv.FormatInt(orderID, 10)})
	})
	api.GET("/orders/:id", func(c *gin.Context) {
		orderID, err := positiveID(c.Param("id"))
		if err != nil {
			response.Error(c, validationError("order id must be positive"))
			return
		}
		order, domainErr := service.GetOrder(c.Request.Context(), orderID)
		if domainErr != nil {
			response.Error(c, domainErr)
			return
		}
		response.Success(c, http.StatusOK, "ok", gin.H{"order": order})
	})
	// 项目尚无生产级认证中间件。因此该接口默认不注册，只能为本地开发或测试启用。
	// X-Development-User-ID 是明确的测试身份，不是生产认证机制。
	if cancelAPIEnabled {
		api.POST("/orders/:id/cancel", func(c *gin.Context) {
			orderID, err := positiveID(c.Param("id"))
			if err != nil {
				response.Error(c, validationError("order id must be positive"))
				return
			}
			userID, err := positiveID(c.GetHeader("X-Development-User-ID"))
			if err != nil {
				response.Error(c, validationError("X-Development-User-ID must be positive"))
				return
			}
			result, domainErr := service.CancelOrder(c.Request.Context(), orderID, userID)
			if domainErr != nil {
				response.Error(c, domainErr)
				return
			}
			status := http.StatusAccepted
			if result.Status == "cancelled" {
				status = http.StatusOK
			}
			response.Success(c, status, "cancellation_pending", gin.H{"order_id": orderID, "status": result.Status})
		})
	}
	api.GET("/users/:user_id/orders", func(c *gin.Context) {
		userID, err := positiveID(c.Param("user_id"))
		if err != nil {
			response.Error(c, validationError("user id must be positive"))
			return
		}
		orders, domainErr := service.ListUserOrders(c.Request.Context(), userID)
		if domainErr != nil {
			response.Error(c, domainErr)
			return
		}
		response.Success(c, http.StatusOK, "ok", gin.H{"orders": orders})
	})
}

func positiveID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, strconv.ErrSyntax
	}
	return id, nil
}
