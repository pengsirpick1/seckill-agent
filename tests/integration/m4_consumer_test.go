//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/seckill"
)

func TestOrderConsumerIsIdempotentAndCompensatesDuplicateUser(t *testing.T) {
	db, service, redisClient, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("consumer"), Price: "10.00", Stock: 2})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	activity, domainErr := service.CreateActivity(context.Background(), seckill.CreateActivityInput{ProductID: product.ID, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour), SeckillPrice: "5.00", Stock: 2})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	t.Cleanup(func() { cleanupActivity(t, db, activity.ID, product.ID) })
	if _, domainErr := service.WarmupActivity(context.Background(), activity.ID); domainErr != nil {
		t.Fatal(domainErr)
	}
	processor := order.NewEventProcessor(db, store)
	first := kafka.OrderCreatedEvent{EventID: "event-" + uniqueName("first"), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano() / 1000, ActivityID: activity.ID, ProductID: product.ID, UserID: 55, Quantity: 1, Amount: "5.00", ReservationID: "first-reservation-" + uniqueName("event"), CreatedAt: time.Now().UTC()}
	reserveOrderEvent(t, store, first)
	payload, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), payload); err != nil {
		t.Fatalf("duplicate delivery should be idempotent: %v", err)
	}

	second := first
	second.EventID = "event-" + uniqueName("second")
	second.OrderID++
	second.ReservationID = "second-reservation-" + uniqueName("event")
	second.CreatedAt = time.Now().UTC()
	reserveOrderEvent(t, store, second)
	payload, err = json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	var orders int
	if err := db.Get(&orders, `SELECT COUNT(*) FROM orders WHERE activity_id = ?`, activity.ID); err != nil {
		t.Fatal(err)
	}
	if orders != 1 {
		t.Fatalf("orders=%d, want 1", orders)
	}
	var persistentInventoryState string
	if err := db.Get(&persistentInventoryState, `SELECT persistent_inventory_state FROM order_lifecycle WHERE order_id = ?`, first.OrderID); err != nil || persistentInventoryState != "applied" {
		t.Fatalf("persistent inventory state=%q err=%v, want applied", persistentInventoryState, err)
	}
	if stock, err := redisClient.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 1 {
		t.Fatalf("redis stock=%d err=%v, want 1", stock, err)
	}
	assertInventory(t, db, product.ID, 2, 1, 1)
}

func TestOrderConsumerCompensatesConflictingOrderIDWithoutStrandingReservation(t *testing.T) {
	db, service, redisClient, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("consumer-order-id"), Price: "10.00", Stock: 2})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	activity, domainErr := service.CreateActivity(context.Background(), seckill.CreateActivityInput{ProductID: product.ID, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour), SeckillPrice: "5.00", Stock: 2})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	t.Cleanup(func() { cleanupActivity(t, db, activity.ID, product.ID) })
	if _, domainErr := service.WarmupActivity(context.Background(), activity.ID); domainErr != nil {
		t.Fatal(domainErr)
	}
	processor := order.NewEventProcessor(db, store)
	orderID := time.Now().UnixNano() / 1000
	first := kafka.OrderCreatedEvent{EventID: "event-" + uniqueName("first-id"), EventType: kafka.OrderCreatedEventType, OrderID: orderID, ActivityID: activity.ID, ProductID: product.ID, UserID: 81, Quantity: 1, Amount: "5.00", ReservationID: "reservation-" + uniqueName("first-id"), CreatedAt: time.Now().UTC()}
	second := first
	second.EventID = "event-" + uniqueName("second-id")
	second.ReservationID = "reservation-" + uniqueName("second-id")
	second.UserID = 82
	for _, event := range []kafka.OrderCreatedEvent{first, second} {
		reserveOrderEvent(t, store, event)
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.Process(context.Background(), payload); err != nil {
			t.Fatalf("process %s: %v", event.EventID, err)
		}
	}
	if stock, err := redisClient.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 1 {
		t.Fatalf("redis stock=%d err=%v, want 1", stock, err)
	}
	var orders, processed int
	if err := db.Get(&orders, `SELECT COUNT(*) FROM orders WHERE activity_id = ?`, activity.ID); err != nil || orders != 1 {
		t.Fatalf("orders=%d err=%v, want 1", orders, err)
	}
	if err := db.Get(&processed, `SELECT COUNT(*) FROM processed_events WHERE event_id = ?`, second.EventID); err != nil || processed != 1 {
		t.Fatalf("processed=%d err=%v, want 1", processed, err)
	}
	assertInventory(t, db, product.ID, 2, 1, 1)
}
