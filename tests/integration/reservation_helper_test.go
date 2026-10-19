//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
)

// reserveOrderEvent 演练当前 Redis 预占模型：HTTP 异步下单请求产生 Kafka
// 事件、但事件尚未到达 Consumer 前的状态。
func reserveOrderEvent(t *testing.T, store *inventory.RedisStore, event kafka.OrderCreatedEvent) {
	t.Helper()
	deduction, err := store.Reserve(context.Background(), inventory.Reservation{
		ReservationID: event.ReservationID,
		EventID:       event.EventID,
		OrderID:       event.OrderID,
		ActivityID:    event.ActivityID,
		ProductID:     event.ProductID,
		UserID:        event.UserID,
		Quantity:      event.Quantity,
		Amount:        event.Amount,
		CreatedAt:     event.CreatedAt,
	})
	if err != nil || deduction.Status != inventory.DeductionSuccess {
		t.Fatalf("reserve event %s: deduction=%+v err=%v", event.EventID, deduction, err)
	}
}
