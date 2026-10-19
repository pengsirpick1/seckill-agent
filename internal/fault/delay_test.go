package fault

import (
	"context"
	"testing"
	"time"
)

func TestDelayHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Delay(ctx, time.Second); err == nil {
		t.Fatal("expected canceled context error")
	}
}
