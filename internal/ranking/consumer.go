package ranking

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/inventory"
	eventkafka "github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Metrics struct {
	Processed prometheus.Counter
	Failed    prometheus.Counter
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "rank_consumer_processed_total", Help: "Persisted-order events applied to Redis rankings."}),
		Failed:    prometheus.NewCounter(prometheus.CounterOpts{Name: "rank_consumer_failed_total", Help: "Persisted-order events that failed before offset commit."}),
	}
	registry.MustRegister(metrics.Processed, metrics.Failed)
	return metrics
}

func RunConsumer(parent context.Context, cfg config.Config, log *slog.Logger) error {
	shutdownTracing, err := observability.InitTracing(parent, cfg)
	if err != nil {
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(parent), cfg.ShutdownTimeout)
		defer cancel()
		_ = shutdownTracing(shutdownContext)
	}()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize})
	if err := redisClient.Ping(parent).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	defer redisClient.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...), kgo.ConsumerGroup(cfg.Kafka.RankConsumerGroup), kgo.ConsumeTopics(cfg.Kafka.OrderSucceededTopic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		return fmt.Errorf("create rank consumer: %w", err)
	}
	defer client.Close()
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopMetricsServer := observability.RunMetricsServer(ctx, cfg.HTTPAddr, cfg.ServiceName, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), log)
	defer stopMetricsServer()
	keys := inventory.KeyBuilder{}
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, cfg.Kafka.ConsumerPollRecords)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				log.Error("kafka fetch error", "error", fetchErr.Err)
			}
			continue
		}
		iterator := fetches.RecordIter()
		for !iterator.Done() {
			record := iterator.Next()
			recordContext := extractTraceContext(ctx, record.Headers)
			recordContext, span := otel.Tracer("rank-consumer").Start(recordContext, "kafka.consume order.succeeded", trace.WithSpanKind(trace.SpanKindConsumer))
			if err := Apply(recordContext, redisClient, keys, record.Value); err != nil {
				span.RecordError(err)
				span.End()
				metrics.Failed.Inc()
				log.Error("ranking event processing failed; offset will not be committed", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				break
			}
			span.SetAttributes(attribute.String("messaging.destination.name", record.Topic), attribute.Int64("messaging.kafka.offset", record.Offset))
			if err := client.CommitRecords(ctx, record); err != nil {
				span.RecordError(err)
				span.End()
				log.Error("ranking offset commit failed", "error", err)
				break
			}
			span.End()
			metrics.Processed.Inc()
		}
	}
	return ctx.Err()
}

// Apply 将一笔已持久化订单加入活动排行榜。Redis ZSet 成员唯一，
// 因而重复的 order_id 调用是幂等的。
func Apply(ctx context.Context, client *redis.Client, keys inventory.KeyBuilder, payload []byte) error {
	ctx, span := otel.Tracer("rank-consumer").Start(ctx, "redis.zadd ranking", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	var event eventkafka.OrderSucceededEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		span.RecordError(err)
		return fmt.Errorf("decode order succeeded event: %w", err)
	}
	if event.EventID == "" || event.EventType != eventkafka.OrderSucceededEventType || event.OrderID <= 0 || event.ActivityID <= 0 {
		return fmt.Errorf("invalid order succeeded event")
	}
	if err := client.ZAdd(ctx, keys.Ranking(event.ActivityID), redis.Z{Score: float64(event.CreatedAt.UnixMilli()), Member: strconv.FormatInt(event.OrderID, 10)}).Err(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("zadd ranking: %w", err)
	}
	return nil
}

func extractTraceContext(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	carrier := propagation.MapCarrier{}
	for _, header := range headers {
		if header.Key == "traceparent" {
			carrier.Set("traceparent", string(header.Value))
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
