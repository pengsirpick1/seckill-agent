//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/kafka"
	"github.com/pengtor/seckill-agent/internal/ranking"
)

func TestRankingApplyIsIdempotent(t *testing.T) {
	_, _, redisClient, _ := redisIntegrationService(t)
	activityID := time.Now().UnixNano()
	key := inventory.KeyBuilder{}.Ranking(activityID)
	t.Cleanup(func() { _ = redisClient.Del(context.Background(), key).Err() })
	event := kafka.OrderSucceededEvent{
		EventID:    "ranking-" + uniqueName("event"),
		EventType:  kafka.OrderSucceededEventType,
		OrderID:    time.Now().UnixNano() / 1000,
		ActivityID: activityID,
		ProductID:  1,
		UserID:     1,
		CreatedAt:  time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := ranking.Apply(context.Background(), redisClient, inventory.KeyBuilder{}, payload); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := redisClient.ZCard(context.Background(), key).Result(); err != nil || count != 1 {
		t.Fatalf("ranking cardinality=%d err=%v, want 1", count, err)
	}
}
