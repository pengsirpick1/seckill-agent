//go:build integration

package outbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type recordingProducer struct {
	mu       sync.Mutex
	events   []kafka.OrderCreatedEvent
	failures int
}

func (p *recordingProducer) ProduceOrderCreated(_ context.Context, event kafka.OrderCreatedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return fmt.Errorf("injected kafka outage")
	}
	p.events = append(p.events, event)
	return nil
}
func (p *recordingProducer) ProduceOrderSucceeded(context.Context, kafka.OrderSucceededEvent) error {
	return nil
}
func (p *recordingProducer) Close()     {}
func (p *recordingProducer) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.events) }

func TestPublisherRetainsPendingUntilKafkaAckAndReclaimsIt(t *testing.T) {
	ctx := context.Background()
	client := outboxRedis(t)
	store := inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry()))
	activityID, productID := time.Now().UnixNano(), time.Now().UnixNano()+1
	cleanupOutboxKeys(t, client, activityID, productID)
	if err := client.Set(ctx, store.StockKey(activityID, productID), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	reservation := testReservation(activityID, productID, "pending")
	if d, err := store.Reserve(ctx, reservation); err != nil || d.Status != inventory.DeductionSuccess {
		t.Fatalf("reserve=%+v err=%v", d, err)
	}
	producer := &recordingProducer{failures: 1}
	stream := inventory.KeyBuilder{}.ReservationOutbox(activityID)
	publisher := testPublisher(client, store, producer, "pending-a")
	if err := publisher.RunStreamOnce(ctx, stream); err == nil {
		t.Fatal("expected Kafka failure")
	}
	pending, err := client.XPending(ctx, stream, "m9b-test-group").Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending=%+v err=%v, want 1", pending, err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := testPublisher(client, store, producer, "pending-b").RunStreamOnce(ctx, stream); err != nil {
		t.Fatal(err)
	}
	pending, err = client.XPending(ctx, stream, "m9b-test-group").Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending=%+v err=%v, want 0", pending, err)
	}
	if producer.count() != 1 {
		t.Fatalf("published=%d, want 1", producer.count())
	}
	if stock, err := client.Get(ctx, store.StockKey(activityID, productID)).Int64(); err != nil || stock != 1 {
		t.Fatalf("stock=%d err=%v, want 1", stock, err)
	}
}

func TestPublisherBlocksStreamEntryWhenReservationStateIsMissing(t *testing.T) {
	ctx := context.Background()
	client := outboxRedis(t)
	activityID, productID := time.Now().UnixNano(), time.Now().UnixNano()+1
	cleanupOutboxKeys(t, client, activityID, productID)
	stream := inventory.KeyBuilder{}.ReservationOutbox(activityID)
	created := time.Now().UTC().Format(time.RFC3339Nano)
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"reservation_id": "missing", "event_id": "event-missing", "order_id": "1", "activity_id": fmt.Sprint(activityID), "product_id": fmt.Sprint(productID), "user_id": "1", "quantity": "1", "amount": "1.00", "created_at": created}}).Err(); err != nil {
		t.Fatal(err)
	}
	producer := &recordingProducer{}
	store := inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry()))
	if err := testPublisher(client, store, producer, "missing-state").RunStreamOnce(ctx, stream); err == nil {
		t.Fatal("missing reservation state must block publishing")
	}
	if producer.count() != 0 {
		t.Fatalf("published=%d, want 0", producer.count())
	}
}

func TestReservationStreamSurvivesPublisherRestartAndRetainsBusinessIDs(t *testing.T) {
	ctx := context.Background()
	client := outboxRedis(t)
	store := inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry()))
	activityID, productID := time.Now().UnixNano(), time.Now().UnixNano()+1
	cleanupOutboxKeys(t, client, activityID, productID)
	if err := client.Set(ctx, store.StockKey(activityID, productID), 1, 0).Err(); err != nil {
		t.Fatal(err)
	}
	reservation := testReservation(activityID, productID, "restart")
	if _, err := store.Reserve(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	client = outboxRedis(t)
	producer := &recordingProducer{}
	if err := testPublisher(client, inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry())), producer, "restart").RunStreamOnce(ctx, inventory.KeyBuilder{}.ReservationOutbox(activityID)); err != nil {
		t.Fatal(err)
	}
	if producer.count() != 1 {
		t.Fatalf("published=%d, want 1", producer.count())
	}
	producer.mu.Lock()
	event := producer.events[0]
	producer.mu.Unlock()
	if event.ReservationID != reservation.ReservationID || event.EventID != reservation.EventID || event.OrderID != reservation.OrderID {
		t.Fatalf("event IDs=%+v reservation=%+v", event, reservation)
	}
}

func TestCompensationMarksHashReservationReleasedExactlyOnce(t *testing.T) {
	ctx := context.Background()
	client := outboxRedis(t)
	store := inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry()))
	activityID, productID := time.Now().UnixNano(), time.Now().UnixNano()+1
	cleanupOutboxKeys(t, client, activityID, productID)
	if err := client.Set(ctx, store.StockKey(activityID, productID), 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	reservation := testReservation(activityID, productID, "compensate")
	if _, err := store.Reserve(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if applied, err := store.Compensate(ctx, activityID, productID, reservation.ReservationID); err != nil || !applied {
		t.Fatalf("first compensation applied=%t err=%v", applied, err)
	}
	if applied, err := store.Compensate(ctx, activityID, productID, reservation.ReservationID); err != nil || applied {
		t.Fatalf("repeat compensation applied=%t err=%v", applied, err)
	}
	record, err := store.ReservationRecord(ctx, activityID, reservation.ReservationID)
	if err != nil || record.State != "released" {
		t.Fatalf("record=%+v err=%v, want released", record, err)
	}
	if stock, err := client.Get(ctx, store.StockKey(activityID, productID)).Int64(); err != nil || stock != 2 {
		t.Fatalf("stock=%d err=%v, want 2", stock, err)
	}
}

func TestObserveStreamToleratesGroupRemovalDuringDiscovery(t *testing.T) {
	ctx := context.Background()
	client := outboxRedis(t)
	activityID, productID := time.Now().UnixNano(), time.Now().UnixNano()+1
	cleanupOutboxKeys(t, client, activityID, productID)
	stream := inventory.KeyBuilder{}.ReservationOutbox(activityID)
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"test": "1"}}).Err(); err != nil {
		t.Fatal(err)
	}
	publisher := testPublisher(client, inventory.NewRedisStore(client, inventory.NewMetrics(prometheus.NewRegistry())), &recordingProducer{}, "observation")
	if err := publisher.observeStream(ctx, stream); err != nil {
		t.Fatalf("NOGROUP must not crash or fail observation: %v", err)
	}
}

func outboxRedis(t *testing.T) *redis.Client {
	t.Helper()
	address := os.Getenv("REDIS_ADDRESS")
	if address == "" {
		address = "redis:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func testReservation(activityID, productID int64, suffix string) inventory.Reservation {
	now := time.Now().UTC()
	return inventory.Reservation{ReservationID: "reservation-" + suffix + fmt.Sprint(now.UnixNano()), EventID: "event-" + suffix + fmt.Sprint(now.UnixNano()), OrderID: now.UnixNano(), ActivityID: activityID, ProductID: productID, UserID: 7, Quantity: 1, Amount: "1.00", CreatedAt: now}
}

func testPublisher(client *redis.Client, store *inventory.RedisStore, producer kafka.Producer, consumer string) *Publisher {
	cfg := config.OutboxConfig{ConsumerGroup: "m9b-test-group", ConsumerName: consumer, ClaimIdle: time.Nanosecond, PollInterval: time.Millisecond, BatchSize: 10}
	return NewPublisher(client, store, producer, cfg, NewMetrics(prometheus.NewRegistry()), slogDiscard())
}

func cleanupOutboxKeys(t *testing.T, client *redis.Client, activityID, productID int64) {
	t.Helper()
	keys := inventory.KeyBuilder{}
	t.Cleanup(func() {
		ctx := context.Background()
		reservationPattern := fmt.Sprintf("seckill:reservation:{%d}:*", activityID)
		var cursor uint64
		for {
			found, next, err := client.Scan(ctx, cursor, reservationPattern, 100).Result()
			if err != nil {
				t.Errorf("scan test reservation keys: %v", err)
				return
			}
			if len(found) > 0 {
				_ = client.Del(ctx, found...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if err := client.Del(ctx, keys.Stock(activityID, productID), keys.ReservationOutbox(activityID)).Err(); err != nil {
			t.Errorf("cleanup test outbox: %v", err)
		}
	})
}

func slogDiscard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
