//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/limiter"
)

func TestRedisLuaTokenBucketRefillsAndRejects(t *testing.T) {
	_, _, client, _ := redisIntegrationService(t)
	key := fmt.Sprintf("seckill:limit:test:{%d}", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Del(context.Background(), key).Err() })
	bucket := limiter.NewRedisLimiter(client)
	for range 3 {
		result, err := bucket.Allow(context.Background(), key, 1, 3)
		if err != nil || !result.Allowed {
			t.Fatalf("initial allowance: result=%+v err=%v", result, err)
		}
	}
	rejected, err := bucket.Allow(context.Background(), key, 1, 3)
	if err != nil || rejected.Allowed || rejected.RetryAfter <= 0 {
		t.Fatalf("rejection: result=%+v err=%v", rejected, err)
	}
	time.Sleep(1100 * time.Millisecond)
	refilled, err := bucket.Allow(context.Background(), key, 1, 3)
	if err != nil || !refilled.Allowed {
		t.Fatalf("refill: result=%+v err=%v", refilled, err)
	}
}
