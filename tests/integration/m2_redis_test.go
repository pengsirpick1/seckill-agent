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
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/order"
	"github.com/pengtor/seckill-agent/internal/seckill"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestRedisReservationLuaIsAtomicUnderContention(t *testing.T) {
	db, service, redisClient, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("redis-atomic"), Price: "99.99", Stock: 50})
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
	var wg sync.WaitGroup
	for userID := int64(1); userID <= 100; userID++ {
		userID := userID
		wg.Add(1)
		go func() {
			defer wg.Done()
			event := kafka.OrderCreatedEvent{EventID: fmt.Sprintf("m2-event-%d-%d", activity.ID, userID), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano() + userID, ActivityID: activity.ID, ProductID: product.ID, UserID: userID, Quantity: 1, Amount: "9.99", ReservationID: fmt.Sprintf("m2-reservation-%d-%d", activity.ID, userID), CreatedAt: time.Now().UTC()}
			deduction, err := store.Reserve(context.Background(), inventory.Reservation{ReservationID: event.ReservationID, EventID: event.EventID, OrderID: event.OrderID, ActivityID: event.ActivityID, ProductID: event.ProductID, UserID: event.UserID, Quantity: event.Quantity, Amount: event.Amount, CreatedAt: event.CreatedAt})
			switch {
			case err != nil:
				unexpected.Add(1)
			case deduction.Status == inventory.DeductionSuccess:
				accepted.Add(1)
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
	if stock, err := redisClient.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 0 {
		t.Fatalf("redis stock=%d err=%v, want 0", stock, err)
	}

	stream := inventory.KeyBuilder{}.ReservationOutbox(activity.ID)
	if entries, err := redisClient.XLen(context.Background(), stream).Result(); err != nil || entries != 50 {
		t.Fatalf("stream entries=%d err=%v, want 50", entries, err)
	}
}

func TestDuplicateOrderEventCompensatesRedisExactlyOnce(t *testing.T) {
	db, service, redisClient, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("redis-duplicate"), Price: "10.00", Stock: 2})
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

	first := kafka.OrderCreatedEvent{EventID: uniqueName("event"), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano(), ActivityID: activity.ID, ProductID: product.ID, UserID: 888, Quantity: 1, Amount: "5.00", ReservationID: uniqueName("reservation"), CreatedAt: time.Now().UTC()}
	second := first
	second.EventID = uniqueName("event")
	second.OrderID++
	second.ReservationID = uniqueName("reservation")
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
	if stock, err := redisClient.Get(context.Background(), store.StockKey(activity.ID, product.ID)).Int64(); err != nil || stock != 1 {
		t.Fatalf("redis stock=%d err=%v, want 1", stock, err)
	}
	assertInventory(t, db, product.ID, 2, 1, 1)
}

func TestReservationRejectsMissingWarmup(t *testing.T) {
	db, service, _, store := redisIntegrationService(t)
	product, domainErr := service.CreateProduct(context.Background(), seckill.CreateProductInput{Name: uniqueName("redis-missing"), Price: "10.00", Stock: 1})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	activity, domainErr := service.CreateActivity(context.Background(), seckill.CreateActivityInput{ProductID: product.ID, StartTime: time.Now().Add(-time.Minute), EndTime: time.Now().Add(time.Hour), SeckillPrice: "5.00", Stock: 1})
	if domainErr != nil {
		t.Fatal(domainErr)
	}
	t.Cleanup(func() { cleanupActivity(t, db, activity.ID, product.ID) })
	event := kafka.OrderCreatedEvent{EventID: uniqueName("event"), EventType: kafka.OrderCreatedEventType, OrderID: time.Now().UnixNano(), ActivityID: activity.ID, ProductID: product.ID, UserID: 777, Quantity: 1, Amount: "5.00", ReservationID: uniqueName("reservation"), CreatedAt: time.Now().UTC()}
	deduction, err := store.Reserve(context.Background(), inventory.Reservation{ReservationID: event.ReservationID, EventID: event.EventID, OrderID: event.OrderID, ActivityID: event.ActivityID, ProductID: event.ProductID, UserID: event.UserID, Quantity: event.Quantity, Amount: event.Amount, CreatedAt: event.CreatedAt})
	if err != nil || deduction.Status != inventory.DeductionNotWarmed {
		t.Fatalf("deduction=%+v err=%v, want stock-not-warmed", deduction, err)
	}
	assertInventory(t, db, product.ID, 1, 1, 0)
}

func redisIntegrationService(t *testing.T) (*sqlx.DB, *seckill.Service, *redis.Client, *inventory.RedisStore) {
	t.Helper()
	db, _ := integrationService(t)
	address := os.Getenv("REDIS_ADDRESS")
	if address == "" {
		address = "redis:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	store := inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry()))
	return db, seckill.NewServiceWithStock(db, store), client, store
}
