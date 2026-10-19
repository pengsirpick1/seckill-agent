package seckill

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/lock"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/pkg/apperror"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type asyncDependencies struct {
	ids *order.IDGenerator
}

func NewServiceWithAsync(db *sqlx.DB, stock inventory.Store, producer kafka.Producer, ids *order.IDGenerator) *Service {
	// 为保持源代码兼容性，公共构造函数仍保留 producer。
	// M9B-0 已将 Broker 投递迁移至 Redis Stream Outbox Publisher。
	_ = producer
	return NewServiceWithAsyncAndLocker(db, stock, producer, ids, lock.NewNoop())
}

func NewServiceWithAsyncAndLocker(db *sqlx.DB, stock inventory.Store, producer kafka.Producer, ids *order.IDGenerator, locker lock.Locker) *Service {
	_ = producer
	if locker == nil {
		locker = lock.NewNoop()
	}
	return &Service{db: db, stock: stock, async: &asyncDependencies{ids: ids}, locker: locker}
}

func (s *Service) PurchaseAsync(ctx context.Context, input PurchaseInput, traceID string) (int64, *apperror.Error) {
	if s.async == nil || s.stock == nil {
		return 0, apperror.New("ASYNC_ORDER_UNAVAILABLE", "async order pipeline is unavailable", 503, nil)
	}
	activity, domainErr := s.validatePurchase(ctx, input)
	if domainErr != nil {
		return 0, domainErr
	}
	unlock, err := s.locker.Lock(ctx, input.ProductID)
	if err != nil {
		return 0, apperror.New("ETCD_LOCK_ACQUIRE_FAILED", "product lock could not be acquired", 503, err)
	}
	defer func() {
		unlockContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = unlock(unlockContext)
	}()
	reservationID, err := newReservationID()
	if err != nil {
		return 0, apperror.New("ORDER_RESERVATION_FAILED", "could not reserve stock", 503, err)
	}
	eventID, err := newReservationID()
	if err != nil {
		return 0, apperror.New("ORDER_EVENT_ID_FAILED", "could not create order event", 503, err)
	}
	orderID := s.async.ids.Next()
	traceParent := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, traceParent)
	eventTraceID := trace.SpanContextFromContext(ctx).TraceID().String()
	if eventTraceID == "" || eventTraceID == "00000000000000000000000000000000" {
		eventTraceID = traceID
	}
	createdAt := time.Now().UTC()
	deduction, err := s.stock.Reserve(ctx, inventory.Reservation{ReservationID: reservationID, EventID: eventID, OrderID: orderID, ActivityID: input.ActivityID, ProductID: input.ProductID, UserID: input.UserID, Quantity: 1, Amount: activity.SeckillPrice, TraceID: eventTraceID, TraceParent: traceParent.Get("traceparent"), CreatedAt: createdAt})
	if err != nil {
		return 0, apperror.New("REDIS_OPERATION_FAILED", "redis stock deduction failed", 503, err)
	}
	switch deduction.Status {
	case inventory.DeductionNotWarmed:
		return 0, apperror.New("SECKILL_STOCK_NOT_WARMED", "stock is not warmed", 409, nil)
	case inventory.DeductionExhausted:
		return 0, stockExhausted()
	case inventory.DeductionReleased:
		return 0, apperror.New("ORDER_RESERVATION_RELEASED", "reservation was already released", 409, nil)
	}
	return orderID, nil
}

func (s *Service) validatePurchase(ctx context.Context, input PurchaseInput) (Activity, *apperror.Error) {
	if input.ActivityID <= 0 || input.ProductID <= 0 || input.UserID <= 0 {
		return Activity{}, invalidInput("activity_id, product_id and user_id must be positive")
	}
	var activity Activity
	err := s.db.GetContext(ctx, &activity, `SELECT id, product_id, start_time, end_time, seckill_price, stock, status FROM seckill_activities WHERE id = ?`, input.ActivityID)
	if errors.Is(err, sql.ErrNoRows) {
		return Activity{}, apperror.New("ACTIVITY_NOT_FOUND", "activity not found", 404, err)
	}
	if err != nil {
		return Activity{}, internal(err)
	}
	if activity.ProductID != input.ProductID {
		return Activity{}, apperror.New("PRODUCT_ACTIVITY_MISMATCH", "product does not belong to activity", 400, nil)
	}
	now := time.Now().UTC()
	if activity.Status != "active" {
		return Activity{}, apperror.New("ACTIVITY_NOT_ACTIVE", "activity is not active", 409, nil)
	}
	if now.Before(activity.StartTime) {
		return Activity{}, apperror.New("ACTIVITY_NOT_STARTED", "activity has not started", 409, nil)
	}
	if !now.Before(activity.EndTime) {
		return Activity{}, apperror.New("ACTIVITY_ENDED", "activity has ended", 409, nil)
	}
	return activity, nil
}
