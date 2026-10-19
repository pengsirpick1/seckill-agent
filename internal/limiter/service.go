package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
)

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Layer      string
	Scope      string
}

type Service struct {
	local   *LocalBucket
	redis   *RedisLimiter
	keys    KeyBuilder
	config  config.RateLimitConfig
	metrics *Metrics
}

func NewService(local *LocalBucket, redis *RedisLimiter, cfg config.RateLimitConfig, metrics *Metrics) *Service {
	return &Service{local: local, redis: redis, config: cfg, metrics: metrics}
}

func (s *Service) Allow(ctx context.Context, activityID, productID, userID int64) (Decision, error) {
	if allowed, retryAfter := s.local.Allow(); !allowed {
		s.metrics.Rejected.WithLabelValues("local", "global").Inc()
		return Decision{RetryAfter: retryAfter, Layer: "local", Scope: "global"}, nil
	}
	checks := []struct {
		scope string
		key   string
		rate  int
		burst int
	}{
		{scope: "global", key: s.keys.Global(), rate: s.config.GlobalRate, burst: s.config.GlobalBurst},
		{scope: "product", key: s.keys.Product(productID), rate: s.config.ProductRate, burst: s.config.ProductBurst},
		{scope: "user", key: s.keys.User(activityID, userID), rate: s.config.UserRate, burst: s.config.UserBurst},
	}
	for _, check := range checks {
		result, err := s.redis.Allow(ctx, check.key, check.rate, check.burst)
		if err != nil {
			return Decision{}, fmt.Errorf("distributed %s limit: %w", check.scope, err)
		}
		if !result.Allowed {
			s.metrics.Rejected.WithLabelValues("redis", check.scope).Inc()
			return Decision{RetryAfter: result.RetryAfter, Layer: "redis", Scope: check.scope}, nil
		}
	}
	return Decision{Allowed: true}, nil
}
