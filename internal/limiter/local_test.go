package limiter

import "testing"

func TestLocalBucketRejectsAfterBurst(t *testing.T) {
	bucket := NewLocalBucket(1, 2)
	for range 2 {
		if allowed, _ := bucket.Allow(); !allowed {
			t.Fatal("burst token unexpectedly rejected")
		}
	}
	if allowed, retryAfter := bucket.Allow(); allowed || retryAfter <= 0 {
		t.Fatalf("allowed=%t retryAfter=%s", allowed, retryAfter)
	}
}
