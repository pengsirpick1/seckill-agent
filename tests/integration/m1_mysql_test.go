//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/database"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/seckill"
)

func TestAsyncPipelineConcurrentStockInvariant(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("concurrency"), Price: "99.99", Stock: 50})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	activity, domainErr := service.CreateActivity(context.Background(), seckill.CreateActivityInput{ProductID: product.ID, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour), SeckillPrice: "9.99", Stock: 50})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	t.Cleanup(func() { cleanupActivity(t, db, activity.ID, product.ID) })
	if _, domainErr := service.WarmupActivity(context.Background(), activity.ID); domainErr != nil {
		t.Fatal(domainErr)
	}

	var accepted atomic.Int64
	var exhausted atomic.Int64
	var unexpected atomic.Int64
	var eventsMu sync.Mutex
	events := make([]kafka.OrderCreatedEvent, 0, 50)
	baseOrderID := time.Now().UnixNano()
	var wg sync.WaitGroup
	for userID := int64(1); userID <= 100; userID++ {
		userID := userID
		wg.Add(1)
		go func() {
			defer wg.Done()
			event := kafka.OrderCreatedEvent{EventID: fmt.Sprintf("m1-event-%d-%d", activity.ID, userID), EventType: kafka.OrderCreatedEventType, OrderID: baseOrderID + userID, ActivityID: activity.ID, ProductID: product.ID, UserID: userID, Quantity: 1, Amount: "9.99", ReservationID: fmt.Sprintf("m1-reservation-%d-%d", activity.ID, userID), CreatedAt: time.Now().UTC()}
			deduction, err := store.Reserve(context.Background(), inventory.Reservation{ReservationID: event.ReservationID, EventID: event.EventID, OrderID: event.OrderID, ActivityID: event.ActivityID, ProductID: event.ProductID, UserID: event.UserID, Quantity: event.Quantity, Amount: event.Amount, CreatedAt: event.CreatedAt})
			switch {
			case err != nil:
				unexpected.Add(1)
			case deduction.Status == inventory.DeductionSuccess:
				accepted.Add(1)
				eventsMu.Lock()
				events = append(events, event)
				eventsMu.Unlock()
			case deduction.Status == inventory.DeductionExhausted:
				exhausted.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 50 || exhausted.Load() != 50 || unexpected.Load() != 0 {
		t.Fatalf("accepted=%d exhausted=%d unexpected=%d", accepted.Load(), exhausted.Load(), unexpected.Load())
	}

	processor := order.NewEventProcessor(db, store)
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.Process(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	assertInventory(t, db, product.ID, 50, 0, 50)
	var orderCount int
	if err := db.Get(&orderCount, `SELECT COUNT(*) FROM orders WHERE activity_id = ?`, activity.ID); err != nil {
		t.Fatal(err)
	}
	if orderCount != 50 {
		t.Fatalf("orders=%d, want 50", orderCount)
	}
}

func TestAsyncPipelineDuplicatePurchaseCompensatesReservation(t *testing.T) {
	db, service, client, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("duplicate"), Price: "10.00", Stock: 2})
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

	first := kafka.OrderCreatedEvent{EventID: uniqueName("duplicate-event"), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano(), ActivityID: activity.ID, ProductID: product.ID, UserID: 999, Quantity: 1, Amount: "5.00", ReservationID: uniqueName("duplicate-reservation"), CreatedAt: time.Now().UTC()}
	second := first
	second.EventID = uniqueName("duplicate-event")
	second.OrderID++
	second.ReservationID = uniqueName("duplicate-reservation")
	reserveOrderEvent(t, store, first)
	reserveOrderEvent(t, store, second)

	processor := order.NewEventProcessor(db, store)
	for _, event := range []kafka.OrderCreatedEvent{first, second} {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if err := processor.Process(context.Background(), payload); err != nil {
			t.Fatal(err)
		}
	}
	assertInventory(t, db, product.ID, 2, 1, 1)
	if stock, err := client.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 1 {
		t.Fatalf("redis stock=%d err=%v, want 1", stock, err)
	}
}

func integrationService(t *testing.T) (*sqlx.DB, *seckill.Service) {
	t.Helper()
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Fatal("MYSQL_DSN is required for integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := database.OpenMySQL(ctx, config.MySQLConfig{DSN: dsn, MaxOpenConns: 50, MaxIdleConns: 20, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, seckill.NewService(db)
}

func assertInventory(t *testing.T, db *sqlx.DB, productID, total, available, sold int64) {
	t.Helper()
	var actual struct {
		Total     int64 `db:"total"`
		Available int64 `db:"available"`
		Sold      int64 `db:"sold"`
	}
	if err := db.Get(&actual, `SELECT total, available, sold FROM inventories WHERE product_id = ?`, productID); err != nil {
		t.Fatal(err)
	}
	if actual.Total != total || actual.Available != available || actual.Sold != sold {
		t.Fatalf("inventory=%+v, want total=%d available=%d sold=%d", actual, total, available, sold)
	}
}

func cleanupActivity(t *testing.T, db *sqlx.DB, activityID, productID int64) {
	t.Helper()
	for _, query := range []struct {
		statement string
		id        int64
	}{
		{`DELETE FROM inventory_release_tasks WHERE activity_id = ?`, activityID},
		{`DELETE FROM order_lifecycle WHERE activity_id = ?`, activityID},
		{`DELETE FROM orders WHERE activity_id = ?`, activityID},
		{`DELETE FROM seckill_activities WHERE id = ?`, activityID},
		{`DELETE FROM inventories WHERE product_id = ?`, productID},
		{`DELETE FROM products WHERE id = ?`, productID},
	} {
		if _, err := db.Exec(query.statement, query.id); err != nil {
			t.Errorf("cleanup failed: %v", err)
		}
	}
}

func uniqueName(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }
