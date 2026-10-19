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
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/release"
	"github.com/pengtor/seckill-agent/pkg/apperror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestM9FinalCancelRestoresBothLedgersExactlyOnce(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 901)
	processFinalEvent(t, db, store, event)
	if got, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil || got.Status != "cancelling" {
		t.Fatalf("cancel=%+v err=%v", got, err)
	}
	worker := release.NewWorker(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second)
	if err := worker.RunOnce(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	assertFinalCancelled(t, db, client, store, event, 4)
	if got, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil || got.Status != "cancelled" {
		t.Fatalf("repeat cancel=%+v err=%v", got, err)
	}
	if err := worker.RunOnce(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	assertFinalCancelled(t, db, client, store, event, 4)
}

func TestM9FinalPaymentAndCancelHaveOneWinner(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 902)
	processFinalEvent(t, db, store, event)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var payErr, cancelErr *apperror.Error
	wg.Add(2)
	go func() { defer wg.Done(); <-start; payErr = service.MarkOrderPaid(context.Background(), event.OrderID) }()
	go func() {
		defer wg.Done()
		<-start
		_, cancelErr = service.CancelOrder(context.Background(), event.OrderID, event.UserID)
	}()
	close(start)
	wg.Wait()
	var status string
	if err := db.Get(&status, `SELECT status FROM order_lifecycle WHERE order_id=?`, event.OrderID); err != nil {
		t.Fatal(err)
	}
	if status == "succeeded" {
		if payErr != nil || cancelErr == nil {
			t.Fatalf("payment must exclusively win: pay=%v cancel=%v", payErr, cancelErr)
		}
		return
	}
	if status != "cancelling" || payErr == nil || cancelErr != nil {
		t.Fatalf("cancellation must exclusively win: status=%s pay=%v cancel=%v", status, payErr, cancelErr)
	}
}

func TestM9FinalEarlyCancelDoesNotRestoreMySQLNeverDeducted(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 903)
	if got, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil || got.Status != "cancelling" {
		t.Fatalf("early cancel=%+v err=%v", got, err)
	}
	processFinalEvent(t, db, store, event)
	worker := release.NewWorker(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second)
	if err := worker.RunOnce(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	assertFinalCancelled(t, db, client, store, event, 4)
	var orders int
	if err := db.Get(&orders, `SELECT COUNT(*) FROM orders WHERE id=?`, event.OrderID); err != nil || orders != 0 {
		t.Fatalf("early cancelled order=%d err=%v", orders, err)
	}
}

func TestM9FinalRedisReleaseThenMySQLFailureRetriesWithoutDoubleIncrement(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 904)
	processFinalEvent(t, db, store, event)
	if _, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil {
		t.Fatal(err)
	}
	failOnce := true
	worker := release.NewWorkerWithBeforeComplete(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second, func(release.Task) error {
		if failOnce {
			failOnce = false
			return fmt.Errorf("injected mysql completion failure")
		}
		return nil
	})
	if err := worker.RunOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if stock, err := client.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 4 {
		t.Fatalf("redis stock after injected failure=%d err=%v", stock, err)
	}
	var task struct {
		Status    string `db:"status"`
		ReleaseID string `db:"release_id"`
	}
	if err := db.Get(&task, `SELECT status, release_id FROM inventory_release_tasks WHERE reservation_id=?`, event.ReservationID); err != nil || task.Status != "pending" {
		t.Fatalf("task=%+v err=%v", task, err)
	}
	if _, err := db.Exec(`UPDATE inventory_release_tasks SET next_attempt_at=UTC_TIMESTAMP(6) WHERE release_id=?`, task.ReleaseID); err != nil {
		t.Fatal(err)
	}
	if err := worker.RunOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	assertFinalCancelled(t, db, client, store, event, 4)
}

func TestM9FinalExpiredLeaseAndConcurrentWorkersDoNotDoubleRelease(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 905)
	processFinalEvent(t, db, store, event)
	if _, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE inventory_release_tasks SET status='processing', lease_token='crashed', lease_until=DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 SECOND) WHERE reservation_id=?`, event.ReservationID); err != nil {
		t.Fatal(err)
	}
	first := release.NewWorker(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second)
	second := release.NewWorker(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var e1, e2 error
	wg.Add(2)
	go func() { defer wg.Done(); <-start; e1 = first.RunOnce(context.Background(), 1) }()
	go func() { defer wg.Done(); <-start; e2 = second.RunOnce(context.Background(), 1) }()
	close(start)
	wg.Wait()
	if e1 != nil || e2 != nil {
		t.Fatalf("workers err=%v,%v", e1, e2)
	}
	assertFinalCancelled(t, db, client, store, event, 4)
}

func TestM9FinalMissingRedisReservationFailsSafe(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, activity := lifecycleFixture(t, db, service)
	event := finalReservation(t, store, activity.ID, product.ID, 906)
	processFinalEvent(t, db, store, event)
	if _, err := service.CancelOrder(context.Background(), event.OrderID, event.UserID); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(context.Background(), inventory.KeyBuilder{}.Reservation(activity.ID, event.ReservationID)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := release.NewWorker(db, store, release.NewMetrics(prometheus.NewRegistry()), time.Second).RunOnce(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if stock, err := client.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 3 {
		t.Fatalf("unsafe stock recovery=%d err=%v", stock, err)
	}
	var status string
	if err := db.Get(&status, `SELECT status FROM inventory_release_tasks WHERE reservation_id=?`, event.ReservationID); err != nil || status != "pending" {
		t.Fatalf("task status=%q err=%v", status, err)
	}
}

func finalReservation(t *testing.T, store *inventory.RedisStore, activityID, productID, userID int64) kafka.OrderCreatedEvent {
	t.Helper()
	now := time.Now().UTC()
	event := kafka.OrderCreatedEvent{EventID: uniqueName("m9-final-event"), EventType: kafka.OrderCreatedEventType, OrderID: now.UnixNano(), ActivityID: activityID, ProductID: productID, UserID: userID, Quantity: 1, Amount: "5.00", ReservationID: uniqueName("m9-final-reservation"), CreatedAt: now}
	if d, err := store.Reserve(context.Background(), inventory.Reservation{ReservationID: event.ReservationID, EventID: event.EventID, OrderID: event.OrderID, ActivityID: activityID, ProductID: productID, UserID: userID, Quantity: 1, Amount: event.Amount, CreatedAt: now}); err != nil || d.Status != inventory.DeductionSuccess {
		t.Fatalf("reserve=%+v err=%v", d, err)
	}
	return event
}

func processFinalEvent(t *testing.T, db *sqlx.DB, store *inventory.RedisStore, event kafka.OrderCreatedEvent) {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	processor := order.NewEventProcessor(db, store)
	if err := processor.Process(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
}

func assertFinalCancelled(t *testing.T, db *sqlx.DB, client *redis.Client, store *inventory.RedisStore, event kafka.OrderCreatedEvent, redisExpected int64) {
	t.Helper()
	var lifecycle struct {
		Status     string `db:"status"`
		Persistent string `db:"persistent"`
	}
	if err := db.Get(&lifecycle, `SELECT status, persistent_inventory_state AS persistent FROM order_lifecycle WHERE order_id=?`, event.OrderID); err != nil || lifecycle.Status != "cancelled" {
		t.Fatalf("lifecycle=%+v err=%v", lifecycle, err)
	}
	var taskStatus string
	if err := db.Get(&taskStatus, `SELECT status FROM inventory_release_tasks WHERE reservation_id=?`, event.ReservationID); err != nil || taskStatus != "completed" {
		t.Fatalf("task=%q err=%v", taskStatus, err)
	}
	if stock, err := client.Get(context.Background(), store.StockKey(event.ActivityID, event.ProductID)).Int64(); err != nil || stock != redisExpected {
		t.Fatalf("redis stock=%d err=%v want=%d", stock, err, redisExpected)
	}
	assertInventory(t, db, event.ProductID, 4, 4, 0)
}
