package release

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/database"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Run 构建释放 Worker 的进程级依赖，并启动其持久化任务循环。
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	shutdown, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		return err
	}
	defer shutdown(context.WithoutCancel(ctx))

	db, err := database.OpenMySQL(ctx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()

	client := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("ping redis: %w", err)
	}
	defer client.Close()

	registry := prometheus.NewRegistry()
	stopMetrics := observability.RunMetricsServer(ctx, cfg.HTTPAddr, cfg.ServiceName, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), log)
	defer stopMetrics()

	worker := NewWorker(db, inventory.NewRedisStore(client, inventory.NewMetrics(registry)), NewMetrics(registry), cfg.Release.Lease)
	return worker.Run(ctx, cfg.Release.PollInterval)
}
