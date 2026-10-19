package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func Run(parent context.Context, cfg config.Config, log *slog.Logger) error {
	shutdownTracing, err := observability.InitTracing(parent, cfg)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.WithoutCancel(parent))
	client := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize})
	if err := client.Ping(parent).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("ping redis: %w", err)
	}
	defer client.Close()
	registry := prometheus.NewRegistry()
	producer, err := kafka.NewProducer(cfg.Kafka, kafka.NewMetrics(registry))
	if err != nil {
		return err
	}
	defer producer.Close()
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopMetrics := observability.RunMetricsServer(ctx, cfg.HTTPAddr, cfg.ServiceName, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), log)
	defer stopMetrics()
	publisher := NewPublisher(client, inventory.NewRedisStore(client, inventory.NewMetrics(registry)), producer, cfg.Outbox, NewMetrics(registry), log)
	return publisher.Run(ctx, cfg.Outbox.PollInterval)
}
