package inventory

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pengtor/seckill-agent/internal/fault"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type DeductionStatus string

const (
	DeductionSuccess   DeductionStatus = "success"
	DeductionExhausted DeductionStatus = "stock_exhausted"
	DeductionNotWarmed DeductionStatus = "stock_not_warmed"
	DeductionReleased  DeductionStatus = "reservation_released"
)

type Deduction struct {
	Status    DeductionStatus
	Remaining int64
}

// Reservation 包含写入 Redis 预占 Hash 和 Stream 条目的不可变业务身份信息。
// 它刻意不依赖 Kafka，使 Redis 始终是原子预占的事实来源。
type Reservation struct {
	ReservationID string
	EventID       string
	OrderID       int64
	ActivityID    int64
	ProductID     int64
	UserID        int64
	Quantity      int64
	Amount        string
	TraceID       string
	TraceParent   string
	CreatedAt     time.Time
}

// ReservationRecord 是 Publisher 转发 Stream 事件前要校验的 Redis 持久化事实。
// 记录缺失时绝不能猜测或补造。
type ReservationRecord struct {
	Reservation
	State     string
	ReleaseID string
}

// ReleaseResult 描述释放任务在 Redis 侧的结果。只有实际增加库存的调用会令 Released 为 true；
// AlreadyReleased 表示相同的持久化 release_id 已处理过，进程若曾在 Redis 和 MySQL 之间崩溃，
// 当前调用仍可安全完成 MySQL 事务。
type ReleaseResult struct {
	Released        bool
	AlreadyReleased bool
}

type WarmupResult struct {
	Stock  int64
	Seeded bool
}

type Store interface {
	Warmup(context.Context, int64, int64, int64) (WarmupResult, error)
	Reserve(context.Context, Reservation) (Deduction, error)
	Compensate(context.Context, int64, int64, string) (bool, error)
	Release(context.Context, Reservation, string) (ReleaseResult, error)
	ReservationRecord(context.Context, int64, string) (ReservationRecord, error)
	StockKey(int64, int64) string
}

type RedisStore struct {
	client     redis.UniversalClient
	keys       KeyBuilder
	metrics    *Metrics
	faultDelay time.Duration
}

// reserveScript 在首次写入前完成全部类型和参数校验。后续写入的 Key 与类型均已确定，
// 因而不会因畸形请求失败。Redis 不会在 Lua 运行时错误后回滚已执行的修改；
// 按 prepared → XADD → reserved → DECR 的顺序执行时，意外服务端失败可能留下可检测的
// prepared 或孤立记录，但不会留下缺少预占元数据的隐性成功扣库存。
var reserveScript = redis.NewScript(`
-- ARGV：reservation_id、event_id、order_id、activity_id、product_id、
--       user_id、quantity、amount、trace_id、traceparent、created_at。
local function is_positive_integer(value)
  return value ~= nil and string.match(value, '^%d+$') ~= nil and tonumber(value) > 0
end
for index = 3, 6 do
  if not is_positive_integer(ARGV[index]) then return -4 end
end
if ARGV[1] == '' or ARGV[2] == '' or ARGV[8] == '' then return -4 end
local stockType = redis.call('TYPE', KEYS[1]).ok
if stockType == 'none' then return -2 end
if stockType ~= 'string' then return -4 end
local reservationType = redis.call('TYPE', KEYS[2]).ok
if reservationType ~= 'none' and reservationType ~= 'hash' then return -4 end
local streamType = redis.call('TYPE', KEYS[3]).ok
if streamType ~= 'none' and streamType ~= 'stream' then return -4 end
local stock = redis.call('GET', KEYS[1])
if not stock then return -2 end
stock = tonumber(stock)
if not stock then return -4 end
local state = nil
local existing = nil
if reservationType == 'hash' then
  existing = redis.call('HMGET', KEYS[2], 'reservation_id', 'event_id', 'order_id', 'activity_id', 'product_id', 'user_id', 'quantity', 'state')
  state = existing[8]
end
if state == 'released' then return -3 end
if state == 'reserved' then
  for index = 1, 7 do
    if existing[index] ~= ARGV[index] then return -4 end
  end
  return stock
end
if state and state ~= 'prepared' then return -4 end
if stock <= 0 then return -1 end
redis.call('HSET', KEYS[2], 'state', 'prepared', 'reservation_id', ARGV[1], 'event_id', ARGV[2], 'order_id', ARGV[3], 'activity_id', ARGV[4], 'product_id', ARGV[5], 'user_id', ARGV[6], 'quantity', ARGV[7], 'amount', ARGV[8], 'trace_id', ARGV[9], 'traceparent', ARGV[10], 'created_at', ARGV[11])
redis.call('XADD', KEYS[3], '*', 'reservation_id', ARGV[1], 'event_id', ARGV[2], 'order_id', ARGV[3], 'activity_id', ARGV[4], 'product_id', ARGV[5], 'user_id', ARGV[6], 'quantity', ARGV[7], 'amount', ARGV[8], 'trace_id', ARGV[9], 'traceparent', ARGV[10], 'created_at', ARGV[11])
redis.call('HSET', KEYS[2], 'state', 'reserved')
return redis.call('DECR', KEYS[1])
`)

var compensateScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -2 end
local reservationType = redis.call('TYPE', KEYS[2]).ok
if reservationType ~= 'hash' then return -3 end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'released' then return -1 end
if state ~= 'reserved' then return -3 end
redis.call('HSET', KEYS[2], 'state', 'released')
return redis.call('INCRBY', KEYS[1], 1)
`)

// releaseScript 比旧版补偿脚本更严格。持久化任务会提供所有不可变预占事实和稳定的
// release_id。Redis 事实缺失或 release_id 无关都属于安全失败：两种情形均不能通过
// INCRBY 凭空增加库存。
var releaseScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return -2 end
if redis.call('TYPE', KEYS[2]).ok ~= 'hash' then return -3 end
if redis.call('HGET', KEYS[2], 'reservation_id') ~= ARGV[1] or redis.call('HGET', KEYS[2], 'order_id') ~= ARGV[2] or redis.call('HGET', KEYS[2], 'activity_id') ~= ARGV[3] or redis.call('HGET', KEYS[2], 'product_id') ~= ARGV[4] or redis.call('HGET', KEYS[2], 'quantity') ~= ARGV[5] then return -4 end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'released' then
  if redis.call('HGET', KEYS[2], 'release_id') == ARGV[6] then return -1 end
  return -5
end
if state ~= 'reserved' then return -4 end
redis.call('INCRBY', KEYS[1], tonumber(ARGV[5]))
redis.call('HSET', KEYS[2], 'state', 'released', 'release_id', ARGV[6])
return 1
`)

func NewRedisStore(client redis.UniversalClient, metrics *Metrics) *RedisStore {
	return &RedisStore{client: client, metrics: metrics}
}

func NewRedisStoreWithFaultDelay(client redis.UniversalClient, metrics *Metrics, delay time.Duration) *RedisStore {
	return &RedisStore{client: client, metrics: metrics, faultDelay: delay}
}

func (s *RedisStore) StockKey(activityID, productID int64) string {
	return s.keys.Stock(activityID, productID)
}

func (s *RedisStore) Warmup(ctx context.Context, activityID, productID, stock int64) (WarmupResult, error) {
	ctx, span := otel.Tracer("inventory").Start(ctx, "redis.warmup", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(attribute.String("db.system", "redis"), attribute.Int64("seckill.activity_id", activityID), attribute.Int64("product.id", productID))
	key := s.StockKey(activityID, productID)
	started := time.Now()
	if err := fault.Delay(ctx, s.faultDelay); err != nil {
		span.RecordError(err)
		s.observe("warmup", "error", started)
		return WarmupResult{}, err
	}
	seeded, err := s.client.SetNX(ctx, key, stock, 0).Result()
	if err != nil {
		span.RecordError(err)
		s.observe("warmup", "error", started)
		return WarmupResult{}, fmt.Errorf("set stock key: %w", err)
	}
	if seeded {
		s.observe("warmup", "seeded", started)
		return WarmupResult{Stock: stock, Seeded: true}, nil
	}
	existing, err := s.client.Get(ctx, key).Int64()
	if err != nil {
		span.RecordError(err)
		s.observe("warmup", "error", started)
		return WarmupResult{}, fmt.Errorf("read existing stock key: %w", err)
	}
	s.observe("warmup", "already_exists", started)
	return WarmupResult{Stock: existing, Seeded: false}, nil
}

// Reserve 原子记录库存扣减、不可变预占事实和 Outbox Stream 条目。
// 只有 Redis 提交正常路径上的全部写入后才返回；Kafka 投递刻意异步执行。
func (s *RedisStore) Reserve(ctx context.Context, reservation Reservation) (Deduction, error) {
	if reservation.ReservationID == "" || reservation.EventID == "" || reservation.OrderID <= 0 || reservation.ActivityID <= 0 || reservation.ProductID <= 0 || reservation.UserID <= 0 || reservation.Quantity != 1 || reservation.Amount == "" {
		return Deduction{}, fmt.Errorf("invalid reservation payload")
	}
	ctx, span := otel.Tracer("inventory").Start(ctx, "redis.reserve", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(attribute.String("db.system", "redis"), attribute.Int64("seckill.activity_id", reservation.ActivityID), attribute.Int64("product.id", reservation.ProductID), attribute.String("order.reservation_id", reservation.ReservationID))
	started := time.Now()
	if err := fault.Delay(ctx, s.faultDelay); err != nil {
		span.RecordError(err)
		s.observe("reserve", "error", started)
		s.metrics.OutboxFailures.Inc()
		return Deduction{}, err
	}
	createdAt := reservation.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	result, err := reserveScript.Run(ctx, s.client, []string{s.StockKey(reservation.ActivityID, reservation.ProductID), s.keys.Reservation(reservation.ActivityID, reservation.ReservationID), s.keys.ReservationOutbox(reservation.ActivityID)},
		reservation.ReservationID, reservation.EventID, strconv.FormatInt(reservation.OrderID, 10), strconv.FormatInt(reservation.ActivityID, 10), strconv.FormatInt(reservation.ProductID, 10), strconv.FormatInt(reservation.UserID, 10), strconv.FormatInt(reservation.Quantity, 10), reservation.Amount, reservation.TraceID, reservation.TraceParent, createdAt.Format(time.RFC3339Nano)).Int64()
	if err != nil {
		span.RecordError(err)
		s.observe("reserve", "error", started)
		s.metrics.OutboxFailures.Inc()
		return Deduction{}, fmt.Errorf("run reservation outbox script: %w", err)
	}
	switch result {
	case -4:
		s.observe("reserve", "invalid", started)
		s.metrics.OutboxFailures.Inc()
		return Deduction{}, fmt.Errorf("reservation outbox rejected inconsistent state")
	case -2:
		s.observe("reserve", string(DeductionNotWarmed), started)
		return Deduction{Status: DeductionNotWarmed}, nil
	case -1:
		s.observe("reserve", string(DeductionExhausted), started)
		return Deduction{Status: DeductionExhausted}, nil
	case -3:
		s.observe("reserve", string(DeductionReleased), started)
		return Deduction{Status: DeductionReleased}, nil
	default:
		s.observe("reserve", string(DeductionSuccess), started)
		s.metrics.OutboxReservations.Inc()
		return Deduction{Status: DeductionSuccess, Remaining: result}, nil
	}
}

// ReservationRecord 返回 Redis 明确记录的预占事实。预占状态在 Redis 恢复中丢失时，
// Publisher 会据此阻止对应 Kafka 事件，而不是补造事件。
func (s *RedisStore) ReservationRecord(ctx context.Context, activityID int64, reservationID string) (ReservationRecord, error) {
	values, err := s.client.HGetAll(ctx, s.keys.Reservation(activityID, reservationID)).Result()
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("read reservation record: %w", err)
	}
	if len(values) == 0 {
		return ReservationRecord{}, fmt.Errorf("reservation record missing")
	}
	parse := func(name string) (int64, error) { return strconv.ParseInt(values[name], 10, 64) }
	orderID, err := parse("order_id")
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("parse reservation order_id: %w", err)
	}
	productID, err := parse("product_id")
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("parse reservation product_id: %w", err)
	}
	userID, err := parse("user_id")
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("parse reservation user_id: %w", err)
	}
	quantity, err := parse("quantity")
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("parse reservation quantity: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, values["created_at"])
	if err != nil {
		return ReservationRecord{}, fmt.Errorf("parse reservation created_at: %w", err)
	}
	return ReservationRecord{Reservation: Reservation{ReservationID: values["reservation_id"], EventID: values["event_id"], OrderID: orderID, ActivityID: activityID, ProductID: productID, UserID: userID, Quantity: quantity, Amount: values["amount"], TraceID: values["trace_id"], TraceParent: values["traceparent"], CreatedAt: createdAt}, State: values["state"], ReleaseID: values["release_id"]}, nil
}

// Release 原子回补 Redis 库存并写入稳定的 release ID。
// 预占事实缺失时绝不会退化为直接增加库存。
func (s *RedisStore) Release(ctx context.Context, reservation Reservation, releaseID string) (ReleaseResult, error) {
	if reservation.ReservationID == "" || reservation.OrderID <= 0 || reservation.ActivityID <= 0 || reservation.ProductID <= 0 || reservation.Quantity <= 0 || releaseID == "" {
		return ReleaseResult{}, fmt.Errorf("invalid release payload")
	}
	started := time.Now()
	result, err := releaseScript.Run(ctx, s.client, []string{s.StockKey(reservation.ActivityID, reservation.ProductID), s.keys.Reservation(reservation.ActivityID, reservation.ReservationID)}, reservation.ReservationID, strconv.FormatInt(reservation.OrderID, 10), strconv.FormatInt(reservation.ActivityID, 10), strconv.FormatInt(reservation.ProductID, 10), strconv.FormatInt(reservation.Quantity, 10), releaseID).Int64()
	if err != nil {
		s.observe("release", "error", started)
		return ReleaseResult{}, fmt.Errorf("run release script: %w", err)
	}
	switch result {
	case 1:
		s.observe("release", "applied", started)
		return ReleaseResult{Released: true}, nil
	case -1:
		s.observe("release", "already_applied", started)
		return ReleaseResult{AlreadyReleased: true}, nil
	case -2:
		s.observe("release", "stock_not_warmed", started)
		return ReleaseResult{}, fmt.Errorf("stock key missing for release")
	case -3:
		s.observe("release", "reservation_missing", started)
		return ReleaseResult{}, fmt.Errorf("reservation record missing or legacy")
	case -5:
		s.observe("release", "release_conflict", started)
		return ReleaseResult{}, fmt.Errorf("reservation released by a different release id")
	default:
		s.observe("release", "identity_conflict", started)
		return ReleaseResult{}, fmt.Errorf("reservation facts or state do not permit release")
	}
}

func (s *RedisStore) Compensate(ctx context.Context, activityID, productID int64, reservationID string) (bool, error) {
	ctx, span := otel.Tracer("inventory").Start(ctx, "redis.compensate", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	span.SetAttributes(attribute.String("db.system", "redis"), attribute.Int64("seckill.activity_id", activityID), attribute.Int64("product.id", productID))
	started := time.Now()
	if err := fault.Delay(ctx, s.faultDelay); err != nil {
		span.RecordError(err)
		s.observe("compensate", "error", started)
		s.metrics.CompensationFailures.Inc()
		return false, err
	}
	result, err := compensateScript.Run(ctx, s.client, []string{s.StockKey(activityID, productID), s.keys.Reservation(activityID, reservationID)}).Int64()
	if err != nil {
		span.RecordError(err)
		s.observe("compensate", "error", started)
		s.metrics.CompensationFailures.Inc()
		return false, fmt.Errorf("run compensation script: %w", err)
	}
	if result == -2 {
		s.observe("compensate", "stock_not_warmed", started)
		s.metrics.CompensationFailures.Inc()
		return false, fmt.Errorf("stock key disappeared before compensation")
	}
	if result == -1 {
		s.observe("compensate", "already_applied", started)
		return false, nil
	}
	if result == -3 {
		s.observe("compensate", "reservation_invalid", started)
		s.metrics.CompensationFailures.Inc()
		return false, fmt.Errorf("reservation record missing or not releasable")
	}
	if result == -3 {
		s.observe("compensate", "invalid_reservation", started)
		s.metrics.CompensationFailures.Inc()
		return false, fmt.Errorf("reservation state has unsupported Redis type")
	}
	s.observe("compensate", "applied", started)
	s.metrics.Compensations.Inc()
	return true, nil
}

func (s *RedisStore) observe(operation, result string, started time.Time) {
	s.metrics.OperationDuration.WithLabelValues(operation, result).Observe(time.Since(started).Seconds())
}
