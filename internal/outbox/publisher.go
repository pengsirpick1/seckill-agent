// outbox 包将 Redis 预占 Stream 条目发布到 Kafka。
// Redis 是该路径可靠的交接点；Kafka 和 MySQL 保持至少一次语义，并使用 event_id 实现幂等。
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const streamPattern = "seckill:reservation-outbox:*"

type Metrics struct {
	Published       prometheus.Counter
	PublishFailures prometheus.Counter
	Blocked         prometheus.Counter
	Acknowledged    prometheus.Counter
	Pending         *prometheus.GaugeVec
	Length          *prometheus.GaugeVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Published:       prometheus.NewCounter(prometheus.CounterOpts{Name: "reservation_outbox_published_total", Help: "Reservation Stream entries acknowledged by Kafka."}),
		PublishFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "reservation_outbox_publish_failures_total", Help: "Kafka publish failures; entries remain pending."}),
		Blocked:         prometheus.NewCounter(prometheus.CounterOpts{Name: "reservation_outbox_blocked_total", Help: "Stream entries blocked because their Redis reservation fact is absent or inconsistent."}),
		Acknowledged:    prometheus.NewCounter(prometheus.CounterOpts{Name: "reservation_outbox_xack_total", Help: "Reservation Stream entries acknowledged after Kafka confirmation."}),
		Pending:         prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "reservation_outbox_pending_entries", Help: "Redis Stream consumer-group pending entries."}, []string{"stream"}),
		Length:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "reservation_outbox_stream_entries", Help: "Retained Redis Stream entries; M9B-0 does not trim unverified history."}, []string{"stream"}),
	}
	registry.MustRegister(m.Published, m.PublishFailures, m.Blocked, m.Acknowledged, m.Pending, m.Length)
	return m
}

type ReservationLookup interface {
	ReservationRecord(context.Context, int64, string) (inventory.ReservationRecord, error)
}

type Publisher struct {
	client    redis.UniversalClient
	lookup    ReservationLookup
	producer  kafka.Producer
	group     string
	consumer  string
	claimIdle time.Duration
	batchSize int64
	metrics   *Metrics
	log       *slog.Logger
}

func NewPublisher(client redis.UniversalClient, lookup ReservationLookup, producer kafka.Producer, cfg config.OutboxConfig, metrics *Metrics, log *slog.Logger) *Publisher {
	return &Publisher{client: client, lookup: lookup, producer: producer, group: cfg.ConsumerGroup, consumer: cfg.ConsumerName, claimIdle: cfg.ClaimIdle, batchSize: int64(cfg.BatchSize), metrics: metrics, log: log}
}

// Run 轮询发现到的所有活动级 Stream。这里刻意扫描：Stream 与预占操作原子创建，
// 不依赖跨槽位的全局注册表。M9B-0 中永不裁剪 Stream 条目。
func (p *Publisher) Run(ctx context.Context, pollInterval time.Duration) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
			p.log.Error("reservation outbox publish pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) RunOnce(ctx context.Context) error {
	streams, err := p.streams(ctx)
	if err != nil {
		return err
	}
	for _, stream := range streams {
		if err := p.RunStreamOnce(ctx, stream); err != nil {
			// 畸形或已无法恢复的历史条目保持 Pending 以便排查，
			// 但不能阻塞本轮中其他活动 Stream 投递有效预占。
			p.log.Error("reservation outbox stream pass failed; entry remains pending", "stream", stream, "error", err)
		}
	}
	return nil
}

// RunStreamOnce 为确定性的恢复测试和运维探测而公开。
// 正常生产执行会先发现活动级 Stream。
func (p *Publisher) RunStreamOnce(ctx context.Context, stream string) error {
	if err := p.ensureGroup(ctx, stream); err != nil {
		return err
	}
	if err := p.claimPending(ctx, stream); err != nil {
		return err
	}
	if err := p.readNew(ctx, stream); err != nil {
		return err
	}
	return p.observeStream(ctx, stream)
}

func (p *Publisher) streams(ctx context.Context) ([]string, error) {
	var cursor uint64
	var streams []string
	for {
		keys, next, err := p.client.Scan(ctx, cursor, streamPattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan reservation outbox streams: %w", err)
		}
		streams = append(streams, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return streams, nil
}

func (p *Publisher) ensureGroup(ctx context.Context, stream string) error {
	err := p.client.XGroupCreateMkStream(ctx, stream, p.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create outbox group: %w", err)
	}
	return nil
}

func (p *Publisher) claimPending(ctx context.Context, stream string) error {
	messages, _, err := p.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: p.group, Consumer: p.consumer, MinIdle: p.claimIdle, Start: "0-0", Count: p.batchSize}).Result()
	if err != nil && !strings.Contains(err.Error(), "NOGROUP") {
		return fmt.Errorf("claim pending stream entries: %w", err)
	}
	return p.publishMessages(ctx, stream, messages)
}

func (p *Publisher) readNew(ctx context.Context, stream string) error {
	entries, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: p.group, Consumer: p.consumer, Streams: []string{stream, ">"}, Count: p.batchSize, Block: time.Millisecond}).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read reservation outbox: %w", err)
	}
	for _, entry := range entries {
		if err := p.publishMessages(ctx, stream, entry.Messages); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) publishMessages(ctx context.Context, stream string, messages []redis.XMessage) error {
	for _, message := range messages {
		event, activityID, err := p.eventFor(message)
		if err != nil {
			p.metrics.Blocked.Inc()
			return fmt.Errorf("validate outbox entry %s: %w", message.ID, err)
		}
		record, err := p.lookup.ReservationRecord(ctx, activityID, event.ReservationID)
		if err != nil || record.State != "reserved" || !sameReservation(record.Reservation, event) {
			p.metrics.Blocked.Inc()
			if err != nil {
				return fmt.Errorf("read reservation for stream entry %s: %w", message.ID, err)
			}
			return fmt.Errorf("reservation state is %q or does not match stream entry", record.State)
		}
		if err := p.producer.ProduceOrderCreated(ctx, event); err != nil {
			p.metrics.PublishFailures.Inc()
			return fmt.Errorf("publish reservation event %s: %w", event.EventID, err)
		}
		p.metrics.Published.Inc()
		if err := p.client.XAck(ctx, stream, p.group, message.ID).Err(); err != nil {
			return fmt.Errorf("ack published outbox entry: %w", err)
		}
		p.metrics.Acknowledged.Inc()
	}
	return nil
}

func (p *Publisher) eventFor(message redis.XMessage) (kafka.OrderCreatedEvent, int64, error) {
	value := func(key string) string {
		if raw, ok := message.Values[key]; ok {
			return fmt.Sprint(raw)
		}
		return ""
	}
	parse := func(key string) (int64, error) { return strconv.ParseInt(value(key), 10, 64) }
	orderID, err := parse("order_id")
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("order_id: %w", err)
	}
	activityID, err := parse("activity_id")
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("activity_id: %w", err)
	}
	productID, err := parse("product_id")
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("product_id: %w", err)
	}
	userID, err := parse("user_id")
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("user_id: %w", err)
	}
	quantity, err := parse("quantity")
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("quantity: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, value("created_at"))
	if err != nil {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("created_at: %w", err)
	}
	event := kafka.OrderCreatedEvent{EventID: value("event_id"), EventType: kafka.OrderCreatedEventType, OrderID: orderID, ActivityID: activityID, ProductID: productID, UserID: userID, Quantity: quantity, Amount: value("amount"), ReservationID: value("reservation_id"), TraceID: value("trace_id"), TraceParent: value("traceparent"), CreatedAt: createdAt}
	if event.EventID == "" || event.ReservationID == "" || event.OrderID <= 0 || event.ActivityID <= 0 || event.ProductID <= 0 || event.UserID <= 0 || event.Quantity != 1 || event.Amount == "" {
		return kafka.OrderCreatedEvent{}, 0, fmt.Errorf("invalid event fields")
	}
	return event, activityID, nil
}

func sameReservation(r inventory.Reservation, event kafka.OrderCreatedEvent) bool {
	return r.ReservationID == event.ReservationID && r.EventID == event.EventID && r.OrderID == event.OrderID && r.ActivityID == event.ActivityID && r.ProductID == event.ProductID && r.UserID == event.UserID && r.Quantity == event.Quantity && r.Amount == event.Amount
}

func (p *Publisher) observeStream(ctx context.Context, stream string) error {
	length, err := p.client.XLen(ctx, stream).Result()
	if err != nil {
		return fmt.Errorf("get outbox stream length: %w", err)
	}
	p.metrics.Length.WithLabelValues(stream).Set(float64(length))
	pending, err := p.client.XPending(ctx, stream, p.group).Result()
	if err != nil && !strings.Contains(err.Error(), "NOGROUP") {
		return fmt.Errorf("get outbox pending count: %w", err)
	}
	// Stream 可能在被发现后、当前读取前因保留策略或运维操作被删除。
	// 此时它已无法发布，应返回零，而非解引用 NOGROUP 返回的 nil 结果。
	if pending == nil {
		p.metrics.Pending.WithLabelValues(stream).Set(0)
		return nil
	}
	p.metrics.Pending.WithLabelValues(stream).Set(float64(pending.Count))
	return nil
}
