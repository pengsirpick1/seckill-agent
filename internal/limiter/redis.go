package limiter

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

type Result struct {
	Allowed        bool
	RetryAfter     time.Duration
	RemainingMilli int64
}

type RedisLimiter struct {
	client redis.UniversalClient
}

var tokenBucketScript = redis.NewScript(`
local current = redis.call('TIME')
local now = current[1] * 1000 + math.floor(current[2] / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(state[1])
local updatedAt = tonumber(state[2])
local capacity = tonumber(ARGV[2]) * 1000
local rate = tonumber(ARGV[1])
if not tokens or not updatedAt then
  tokens = capacity
  updatedAt = now
end
local elapsed = math.max(0, now - updatedAt)
tokens = math.min(capacity, tokens + elapsed * rate)
local allowed = 0
if tokens >= 1000 then
  tokens = tokens - 1000
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
redis.call('EXPIRE', KEYS[1], ARGV[3])
return {allowed, tokens}
`)

func NewRedisLimiter(client redis.UniversalClient) *RedisLimiter {
	return &RedisLimiter{client: client}
}

func (l *RedisLimiter) Allow(ctx context.Context, key string, rate, burst int) (Result, error) {
	if rate <= 0 || burst <= 0 {
		return Result{}, fmt.Errorf("rate and burst must be positive")
	}
	ttl := max(2, int(math.Ceil(float64(burst)/float64(rate))*2))
	values, err := tokenBucketScript.Run(ctx, l.client, []string{key}, rate, burst, ttl).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("run token bucket script: %w", err)
	}
	if len(values) != 2 {
		return Result{}, fmt.Errorf("unexpected token bucket result length %d", len(values))
	}
	allowed, err := asInt64(values[0])
	if err != nil {
		return Result{}, err
	}
	remaining, err := asInt64(values[1])
	if err != nil {
		return Result{}, err
	}
	result := Result{Allowed: allowed == 1, RemainingMilli: remaining}
	if !result.Allowed {
		missingMilli := max(int64(1), 1000-remaining)
		result.RetryAfter = time.Duration(math.Ceil(float64(missingMilli)/float64(rate))) * time.Millisecond
	}
	return result, nil
}

func asInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		var parsed int64
		if _, err := fmt.Sscan(typed, &parsed); err != nil {
			return 0, fmt.Errorf("parse redis result: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected redis result type %T", value)
	}
}
