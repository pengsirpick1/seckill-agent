package limiter

import (
	"sync"
	"time"
)

type LocalBucket struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	tokens    float64
	updatedAt time.Time
}

func NewLocalBucket(rate, burst int) *LocalBucket {
	now := time.Now()
	return &LocalBucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), updatedAt: now}
}

func (b *LocalBucket) Allow() (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.updatedAt).Seconds()
	b.tokens = min(b.burst, b.tokens+elapsed*b.rate)
	b.updatedAt = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration(float64(time.Second) / b.rate)
}
