package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/database"
	"github.com/pengtor/seckill-agent/internal/fault"
	"github.com/pengtor/seckill-agent/internal/inventory"
	eventkafka "github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/observability"
	"github.com/pengtor/seckill-agent/internal/release"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type EventProcessor struct {
	db      *sqlx.DB
	stock   inventory.Store
	dbDelay time.Duration
	metrics *Metrics
}

type ProcessResult struct {
	Event     eventkafka.OrderCreatedEvent
	Persisted bool
	Outcome   string
}

// lifecycleIdentityMismatch 是确定性的业务拒绝：第二个预占复用了已有 order_id，
// 但不可变事实不同。必须记录并补偿，绝不能让 Redis 库存长期被占用。
type lifecycleIdentityMismatch struct{ orderID int64 }

func (e lifecycleIdentityMismatch) Error() string {
	return fmt.Sprintf("order lifecycle identity mismatch for order %d", e.orderID)
}

func NewEventProcessor(db *sqlx.DB, stock inventory.Store) *EventProcessor {
	return &EventProcessor{db: db, stock: stock}
}

func NewEventProcessorWithFaults(db *sqlx.DB, stock inventory.Store, metrics *Metrics, dbDelay time.Duration) *EventProcessor {
	return &EventProcessor{db: db, stock: stock, metrics: metrics, dbDelay: dbDelay}
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
	db, err := database.OpenMySQL(parent, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize})
	if err := redisClient.Ping(parent).Err(); err != nil {
		_ = redisClient.Close()
		return fmt.Errorf("ping redis: %w", err)
	}
	defer redisClient.Close()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	processor := NewEventProcessorWithFaults(db, inventory.NewRedisStore(redisClient, inventory.NewMetrics(registry)), metrics, cfg.Faults.DBDelay)
	producer, err := eventkafka.NewProducer(cfg.Kafka, eventkafka.NewMetrics(registry))
	if err != nil {
		return err
	}
	defer producer.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...), kgo.ConsumerGroup(cfg.Kafka.ConsumerGroup), kgo.ConsumeTopics(cfg.Kafka.OrderTopic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopMetricsServer := observability.RunMetricsServer(ctx, cfg.HTTPAddr, cfg.ServiceName, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), log)
	defer stopMetricsServer()
	log.Info("order consumer started", "topic", cfg.Kafka.OrderTopic, "group", cfg.Kafka.ConsumerGroup, "max_in_flight", cfg.Kafka.ConsumerPollRecords)
	if cfg.Faults.ConsumerDelay > 0 || cfg.Faults.DBDelay > 0 {
		log.Warn("development fault injection enabled", "consumer_delay_ms", cfg.Faults.ConsumerDelay.Milliseconds(), "db_transaction_delay_ms", cfg.Faults.DBDelay.Milliseconds())
	}
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, cfg.Kafka.ConsumerPollRecords)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				log.Error("kafka fetch error", "error", fetchErr.Err)
			}
			continue
		}
		highWatermarks := make(map[string]int64)
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			highWatermarks[lagKey(partition.Topic, partition.Partition)] = partition.HighWatermark
		})
		iterator := fetches.RecordIter()
		for !iterator.Done() {
			record := iterator.Next()
			recordContext := extractTraceContext(ctx, record.Headers)
			recordContext, span := otel.Tracer("order-consumer").Start(recordContext, "kafka.consume order.created", trace.WithSpanKind(trace.SpanKindConsumer))
			span.SetAttributes(attribute.String("messaging.destination.name", record.Topic), attribute.Int64("messaging.kafka.offset", record.Offset))
			if err := fault.Delay(recordContext, cfg.Faults.ConsumerDelay); err != nil {
				span.RecordError(err)
				span.End()
				return fmt.Errorf("delay before order event processing: %w", err)
			}
			result, err := processor.ProcessResult(recordContext, record.Value)
			if err != nil {
				span.RecordError(err)
				span.End()
				metrics.Failed.Inc()
				log.Error("order event processing failed; offset will not be committed", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				return fmt.Errorf("process order event at offset %d: %w", record.Offset, err)
			}
			if result.Persisted {
				succeeded := eventkafka.OrderSucceededEvent{EventID: result.Event.EventID, EventType: eventkafka.OrderSucceededEventType, OrderID: result.Event.OrderID, ActivityID: result.Event.ActivityID, ProductID: result.Event.ProductID, UserID: result.Event.UserID, TraceID: result.Event.TraceID, CreatedAt: result.Event.CreatedAt}
				if err := producer.ProduceOrderSucceeded(recordContext, succeeded); err != nil {
					span.RecordError(err)
					span.End()
					metrics.Failed.Inc()
					log.Error("order persisted but success event publish failed; offset will not be committed", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
					return fmt.Errorf("publish succeeded event at offset %d: %w", record.Offset, err)
				}
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				span.RecordError(err)
				span.End()
				log.Error("kafka offset commit failed after database commit", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				return fmt.Errorf("commit order event offset %d: %w", record.Offset, err)
			}
			span.End()
			metrics.Processed.WithLabelValues(result.Outcome).Inc()
			if highWatermark, ok := highWatermarks[lagKey(record.Topic, record.Partition)]; ok {
				lag := highWatermark - record.Offset - 1
				if lag < 0 {
					lag = 0
				}
				metrics.Lag.WithLabelValues(cfg.Kafka.ConsumerGroup, record.Topic, strconv.Itoa(int(record.Partition))).Set(float64(lag))
			}
		}
	}
	return ctx.Err()
}

func (p *EventProcessor) Process(ctx context.Context, payload []byte) error {
	_, err := p.ProcessResult(ctx, payload)
	return err
}

func (p *EventProcessor) ProcessResult(ctx context.Context, payload []byte) (ProcessResult, error) {
	var event eventkafka.OrderCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return ProcessResult{}, fmt.Errorf("decode order event: %w", err)
	}
	if event.EventID == "" || event.EventType != eventkafka.OrderCreatedEventType || event.OrderID <= 0 || event.Quantity != 1 || event.ReservationID == "" {
		return ProcessResult{}, fmt.Errorf("invalid order event")
	}
	return p.persist(ctx, event)
}

func (p *EventProcessor) persist(ctx context.Context, event eventkafka.OrderCreatedEvent) (result ProcessResult, retErr error) {
	started := time.Now()
	if p.metrics != nil {
		defer func() {
			p.metrics.MySQLDuration.Observe(time.Since(started).Seconds())
			if retErr != nil {
				p.metrics.MySQLErrors.Inc()
			}
		}()
	}
	ctx, span := otel.Tracer("order-consumer").Start(ctx, "mysql.transaction order.persist", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(attribute.String("db.system", "mysql"), attribute.Int64("order.id", event.OrderID))
	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		span.RecordError(err)
		return ProcessResult{}, fmt.Errorf("begin order transaction: %w", err)
	}
	defer tx.Rollback()
	if err := fault.Delay(ctx, p.dbDelay); err != nil {
		span.RecordError(err)
		return ProcessResult{}, err
	}
	var processed string
	err = tx.GetContext(ctx, &processed, `SELECT event_id FROM processed_events WHERE event_id = ?`, event.EventID)
	if err == nil {
		var exists int
		if err := tx.GetContext(ctx, &exists, `SELECT 1 FROM orders WHERE id = ?`, event.OrderID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ProcessResult{}, fmt.Errorf("check duplicate order: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return ProcessResult{}, err
		}
		return ProcessResult{Event: event, Persisted: exists == 1, Outcome: "duplicate"}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ProcessResult{}, fmt.Errorf("check processed event: %w", err)
	}
	lifecycleStatus, err := lockLifecycle(ctx, tx, event, p.metrics)
	if err != nil {
		var mismatch lifecycleIdentityMismatch
		if errors.As(err, &mismatch) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				return ProcessResult{}, fmt.Errorf("rollback conflicting order lifecycle: %w", rollbackErr)
			}
			return p.compensateAndMark(ctx, event)
		}
		return ProcessResult{}, err
	}
	if lifecycleStatus == "cancelling" || lifecycleStatus == "cancelled" || lifecycleStatus == "succeeded" || lifecycleStatus == "rejected" {
		if lifecycleStatus == "cancelling" {
			// 提前取消记录现已具备不可变 Kafka 事实。在当前事务中将释放任务与
			// processed_events 一并持久化，确保提交 offset 后不会留下无法恢复的 Redis 预占。
			row, err := release.LoadLifecycleForUpdate(ctx, tx, event.OrderID)
			if err != nil {
				return ProcessResult{}, fmt.Errorf("load cancelling lifecycle: %w", err)
			}
			if _, err := release.CreateForLifecycle(ctx, tx, row); err != nil {
				return ProcessResult{}, fmt.Errorf("create early cancellation release task: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO processed_events (event_id, event_type) VALUES (?, ?)`, event.EventID, event.EventType); err != nil {
			return ProcessResult{}, fmt.Errorf("record non-creatable order event: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return ProcessResult{}, fmt.Errorf("commit non-creatable order event: %w", err)
		}
		return ProcessResult{Event: event, Outcome: "suppressed"}, nil
	}
	orderInsertStarted := time.Now()
	_, err = tx.ExecContext(ctx, `INSERT INTO orders (id, reservation_id, user_id, product_id, activity_id, quantity, amount, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'created')`, event.OrderID, event.ReservationID, event.UserID, event.ProductID, event.ActivityID, event.Quantity, event.Amount)
	p.observeMySQLStep("order_insert", orderInsertStarted)
	if err != nil {
		if isDuplicate(err) {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				return ProcessResult{}, fmt.Errorf("rollback duplicate order transaction: %w", rollbackErr)
			}
			return p.compensateAndMark(ctx, event)
		}
		return ProcessResult{}, fmt.Errorf("insert order: %w", err)
	}
	inventoryStarted := time.Now()
	activityUpdate, err := tx.ExecContext(ctx, `UPDATE seckill_activities SET stock = stock - 1 WHERE id = ? AND product_id = ? AND stock > 0`, event.ActivityID, event.ProductID)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("update activity inventory: %w", err)
	}
	if affected, _ := activityUpdate.RowsAffected(); affected != 1 {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return ProcessResult{}, fmt.Errorf("rollback unavailable activity transaction: %w", rollbackErr)
		}
		return p.compensateAndMark(ctx, event)
	}
	inventoryUpdate, err := tx.ExecContext(ctx, `UPDATE inventories SET available = available - 1, sold = sold + 1, version = version + 1 WHERE product_id = ? AND available > 0`, event.ProductID)
	p.observeMySQLStep("inventory_updates", inventoryStarted)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("update persistent inventory: %w", err)
	}
	if affected, _ := inventoryUpdate.RowsAffected(); affected != 1 {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return ProcessResult{}, fmt.Errorf("rollback unavailable inventory transaction: %w", rollbackErr)
		}
		return p.compensateAndMark(ctx, event)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET persistent_inventory_state = 'applied' WHERE order_id = ? AND persistent_inventory_state = 'not_applied'`, event.OrderID); err != nil {
		return ProcessResult{}, fmt.Errorf("record persistent inventory application: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO processed_events (event_id, event_type) VALUES (?, ?)`, event.EventID, event.EventType); err != nil {
		return ProcessResult{}, fmt.Errorf("record processed event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return ProcessResult{}, fmt.Errorf("commit order transaction: %w", err)
	}
	return ProcessResult{Event: event, Persisted: true, Outcome: "persisted"}, nil
}

func (p *EventProcessor) compensateAndMark(ctx context.Context, event eventkafka.OrderCreatedEvent) (ProcessResult, error) {
	compensationContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if _, err := p.stock.Compensate(compensationContext, event.ActivityID, event.ProductID, event.ReservationID); err != nil {
		return ProcessResult{}, fmt.Errorf("compensate redis stock: %w", err)
	}
	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ProcessResult{}, fmt.Errorf("begin processed-event transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO order_lifecycle (order_id, reservation_id, event_id, user_id, product_id, activity_id, quantity, status, reservation_state, persistent_inventory_state) VALUES (?, ?, ?, ?, ?, ?, ?, 'rejected', 'released', 'not_applied')`, event.OrderID, event.ReservationID, event.EventID, event.UserID, event.ProductID, event.ActivityID, event.Quantity); err != nil {
		return ProcessResult{}, fmt.Errorf("record rejected order lifecycle: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO processed_events (event_id, event_type) VALUES (?, ?)`, event.EventID, event.EventType); err != nil {
		return ProcessResult{}, fmt.Errorf("record compensated event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ProcessResult{}, fmt.Errorf("commit compensated event: %w", err)
	}
	return ProcessResult{Event: event, Outcome: "compensated"}, nil
}

// lockLifecycle 使用原子的冲突插入作为仲裁点。Kafka Consumer 到达前，取消意图可能已创建该记录。
func lockLifecycle(ctx context.Context, tx *sqlx.Tx, event eventkafka.OrderCreatedEvent, metrics *Metrics) (string, error) {
	upsertStarted := time.Now()
	_, err := tx.ExecContext(ctx, `INSERT INTO order_lifecycle (order_id, reservation_id, event_id, user_id, product_id, activity_id, quantity, status, reservation_state)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'created', 'reserved')
		ON DUPLICATE KEY UPDATE order_id = VALUES(order_id)`, event.OrderID, event.ReservationID, event.EventID, event.UserID, event.ProductID, event.ActivityID, event.Quantity)
	if metrics != nil {
		metrics.MySQLStepDuration.WithLabelValues("lifecycle_upsert").Observe(time.Since(upsertStarted).Seconds())
	}
	if err != nil {
		return "", fmt.Errorf("arbitrate order lifecycle: %w", err)
	}
	var current struct {
		ReservationID sql.NullString `db:"reservation_id"`
		EventID       sql.NullString `db:"event_id"`
		UserID        int64          `db:"user_id"`
		ProductID     sql.NullInt64  `db:"product_id"`
		ActivityID    sql.NullInt64  `db:"activity_id"`
		Quantity      sql.NullInt64  `db:"quantity"`
		Status        string         `db:"status"`
	}
	lockStarted := time.Now()
	if err := tx.GetContext(ctx, &current, `SELECT reservation_id, event_id, user_id, product_id, activity_id, quantity, status FROM order_lifecycle WHERE order_id = ? FOR UPDATE`, event.OrderID); err != nil {
		if metrics != nil {
			metrics.MySQLStepDuration.WithLabelValues("lifecycle_for_update").Observe(time.Since(lockStarted).Seconds())
		}
		return "", fmt.Errorf("lock order lifecycle: %w", err)
	}
	if metrics != nil {
		metrics.MySQLStepDuration.WithLabelValues("lifecycle_for_update").Observe(time.Since(lockStarted).Seconds())
	}
	if current.UserID != event.UserID || (current.ProductID.Valid && current.ProductID.Int64 != event.ProductID) || (current.ActivityID.Valid && current.ActivityID.Int64 != event.ActivityID) || (current.Quantity.Valid && current.Quantity.Int64 != event.Quantity) || (current.ReservationID.Valid && current.ReservationID.String != event.ReservationID) || (current.EventID.Valid && current.EventID.String != event.EventID) {
		return "", lifecycleIdentityMismatch{orderID: event.OrderID}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET reservation_id = COALESCE(reservation_id, ?), event_id = COALESCE(event_id, ?), product_id = COALESCE(product_id, ?), activity_id = COALESCE(activity_id, ?), quantity = COALESCE(quantity, ?), reservation_state = COALESCE(reservation_state, 'reserved') WHERE order_id = ?`, event.ReservationID, event.EventID, event.ProductID, event.ActivityID, event.Quantity, event.OrderID); err != nil {
		return "", fmt.Errorf("complete order lifecycle identity: %w", err)
	}
	return current.Status, nil
}

func (p *EventProcessor) observeMySQLStep(step string, started time.Time) {
	if p.metrics != nil {
		p.metrics.MySQLStepDuration.WithLabelValues(step).Observe(time.Since(started).Seconds())
	}
}

func extractTraceContext(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceParent(headers)})
}

func traceParent(headers []kgo.RecordHeader) string {
	for _, header := range headers {
		if header.Key == "traceparent" {
			return string(header.Value)
		}
	}
	return ""
}

func lagKey(topic string, partition int32) string {
	return topic + ":" + strconv.Itoa(int(partition))
}

func isDuplicate(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
