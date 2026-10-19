//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/seckill"
	"github.com/pengtor/seckill-agent/pkg/apperror"
)

func TestM9APaymentIsIdempotentAndOnlyCreatedOrdersCanPay(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := lifecycleEvent(activity.ID, product.ID, uniqueName("reservation"))
	reserveOrderEvent(t, store, event)
	processor := order.NewEventProcessor(db, store)
	processLifecycleEvent(t, processor, event)
	if err := service.MarkOrderPaid(context.Background(), event.OrderID); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkOrderPaid(context.Background(), event.OrderID); err != nil {
		t.Fatalf("repeated payment should be idempotent: %v", err)
	}
	var status string
	if err := db.Get(&status, `SELECT status FROM orders WHERE id = ?`, event.OrderID); err != nil || status != "succeeded" {
		t.Fatalf("order status=%q err=%v, want succeeded", status, err)
	}
	if err := db.Get(&status, `SELECT status FROM order_lifecycle WHERE order_id = ?`, event.OrderID); err != nil || status != "succeeded" {
		t.Fatalf("lifecycle status=%q err=%v, want succeeded", status, err)
	}
	if err := order.RecordEarlyCancellationIntentForTest(context.Background(), db, event.OrderID, event.UserID); err == nil {
		t.Fatal("cancellation intent must conflict with succeeded order")
	}
}

func TestM9AEarlyCancellationIntentWinsBeforeConsumerInsert(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := lifecycleEvent(activity.ID, product.ID, uniqueName("reservation"))
	reserveOrderEvent(t, store, event)
	if err := order.RecordEarlyCancellationIntentForTest(context.Background(), db, event.OrderID, event.UserID); err != nil {
		t.Fatal(err)
	}
	processLifecycleEvent(t, order.NewEventProcessor(db, store), event)
	var orders int
	if err := db.Get(&orders, `SELECT COUNT(*) FROM orders WHERE id = ?`, event.OrderID); err != nil || orders != 0 {
		t.Fatalf("orders=%d err=%v; pre-cancelled reservation must not become a normal order", orders, err)
	}
	var lifecycle struct {
		Status                   string `db:"status"`
		ReservationID            string `db:"reservation_id"`
		EventID                  string `db:"event_id"`
		UserID                   int64  `db:"user_id"`
		ProductID                int64  `db:"product_id"`
		ActivityID               int64  `db:"activity_id"`
		Quantity                 int64  `db:"quantity"`
		ReservationState         string `db:"reservation_state"`
		PersistentInventoryState string `db:"persistent_inventory_state"`
	}
	if err := db.Get(&lifecycle, `SELECT status, reservation_id, event_id, user_id, product_id, activity_id, quantity, reservation_state, persistent_inventory_state FROM order_lifecycle WHERE order_id = ?`, event.OrderID); err != nil || lifecycle.Status != "cancelling" {
		t.Fatalf("lifecycle=%+v err=%v, want cancelling", lifecycle, err)
	}
	if lifecycle.ReservationID != event.ReservationID || lifecycle.EventID != event.EventID {
		t.Fatalf("lifecycle IDs=%+v, want reservation=%q event=%q", lifecycle, event.ReservationID, event.EventID)
	}
	if lifecycle.UserID != event.UserID || lifecycle.ProductID != event.ProductID || lifecycle.ActivityID != event.ActivityID || lifecycle.Quantity != event.Quantity || lifecycle.ReservationState != "reserved" {
		t.Fatalf("incomplete recoverable lifecycle record: %+v", lifecycle)
	}
	if lifecycle.PersistentInventoryState != "not_applied" {
		t.Fatalf("persistent inventory state=%q, want not_applied", lifecycle.PersistentInventoryState)
	}
}

func TestM9APaymentAndCancellationArbitrateOnLifecycleRow(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := lifecycleEvent(activity.ID, product.ID, uniqueName("reservation"))
	reserveOrderEvent(t, store, event)
	processLifecycleEvent(t, order.NewEventProcessor(db, store), event)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var paymentErr *apperror.Error
	var cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		paymentErr = service.MarkOrderPaid(context.Background(), event.OrderID)
	}()
	go func() {
		defer wg.Done()
		<-start
		cancelErr = order.RecordEarlyCancellationIntentForTest(context.Background(), db, event.OrderID, event.UserID)
	}()
	close(start)
	wg.Wait()

	var lifecycleStatus, orderStatus string
	if err := db.Get(&lifecycleStatus, `SELECT status FROM order_lifecycle WHERE order_id = ?`, event.OrderID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&orderStatus, `SELECT status FROM orders WHERE id = ?`, event.OrderID); err != nil {
		t.Fatal(err)
	}
	if lifecycleStatus == "succeeded" {
		if orderStatus != "succeeded" || paymentErr != nil || cancelErr == nil {
			t.Fatalf("invalid paid winner: order=%s lifecycle=%s paymentErr=%v cancelErr=%v", orderStatus, lifecycleStatus, paymentErr, cancelErr)
		}
	} else if lifecycleStatus == "cancelling" {
		if orderStatus != "cancelling" || cancelErr != nil || paymentErr == nil {
			t.Fatalf("invalid cancellation winner: order=%s lifecycle=%s paymentErr=%v cancelErr=%v", orderStatus, lifecycleStatus, paymentErr, cancelErr)
		}
	} else {
		t.Fatalf("unexpected lifecycle state %q", lifecycleStatus)
	}
}

func TestM9AConsumerAndEarlyCancellationUpsertRace(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := lifecycleEvent(activity.ID, product.ID, uniqueName("reservation"))
	reserveOrderEvent(t, store, event)
	processor := order.NewEventProcessor(db, store)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var processErr, cancelErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		payload, _ := json.Marshal(event)
		processErr = processor.Process(context.Background(), payload)
	}()
	go func() {
		defer wg.Done()
		<-start
		cancelErr = order.RecordEarlyCancellationIntentForTest(context.Background(), db, event.OrderID, event.UserID)
	}()
	close(start)
	wg.Wait()
	if processErr != nil || cancelErr != nil {
		t.Fatalf("consumer err=%v cancel err=%v", processErr, cancelErr)
	}
	var lifecycleStatus string
	if err := db.Get(&lifecycleStatus, `SELECT status FROM order_lifecycle WHERE order_id = ?`, event.OrderID); err != nil || lifecycleStatus != "cancelling" {
		t.Fatalf("lifecycle status=%q err=%v, want cancelling", lifecycleStatus, err)
	}
	var count int
	if err := db.Get(&count, `SELECT COUNT(*) FROM orders WHERE id = ?`, event.OrderID); err != nil {
		t.Fatal(err)
	}
	if count == 1 {
		var status string
		if err := db.Get(&status, `SELECT status FROM orders WHERE id = ?`, event.OrderID); err != nil || status != "cancelling" {
			t.Fatalf("order status=%q err=%v, want cancelling", status, err)
		}
	}
}

func lifecycleFixture(t *testing.T, dbService *sqlx.DB, service *seckill.Service) (seckill.Product, seckill.Activity) {
	t.Helper()
	product, appErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("m9a"), Price: "10.00", Stock: 4})
	if appErr != nil {
		t.Fatal(appErr)
	}
	activity, appErr := service.CreateActivity(context.Background(), seckill.CreateActivityInput{ProductID: product.ID, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour), SeckillPrice: "5.00", Stock: 4})
	if appErr != nil {
		t.Fatal(appErr)
	}
	t.Cleanup(func() { cleanupActivity(t, dbService, activity.ID, product.ID) })
	if _, appErr := service.WarmupActivity(context.Background(), activity.ID); appErr != nil {
		t.Fatal(appErr)
	}
	return product, activity
}

func lifecycleEvent(activityID, productID int64, reservationID string) kafka.OrderCreatedEvent {
	return kafka.OrderCreatedEvent{EventID: uniqueName("event"), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano(), ActivityID: activityID, ProductID: productID, UserID: time.Now().UnixNano(), Quantity: 1, Amount: "5.00", ReservationID: reservationID, CreatedAt: time.Now().UTC()}
}

func processLifecycleEvent(t *testing.T, processor *order.EventProcessor, event kafka.OrderCreatedEvent) {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), payload); err != nil {
		t.Fatal(fmt.Errorf("process lifecycle event: %w", err))
	}
}
