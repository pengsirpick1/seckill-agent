//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/internal/lock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestEtcdLockerSerializesOneProduct(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := lock.NewMetrics(registry)
	locker, err := lock.NewEtcd(config.EtcdConfig{
		Endpoints:          []string{"etcd:2379"},
		DialTimeout:        time.Second,
		LockSessionTTL:     5 * time.Second,
		LockAcquireTimeout: time.Second,
	}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = locker.Close() })

	firstUnlock, err := locker.Lock(context.Background(), 2001)
	if err != nil {
		t.Fatal(err)
	}
	secondAcquired := make(chan lock.UnlockFunc, 1)
	secondError := make(chan error, 1)
	go func() {
		unlock, lockErr := locker.Lock(context.Background(), 2001)
		if lockErr != nil {
			secondError <- lockErr
			return
		}
		secondAcquired <- unlock
	}()

	select {
	case <-secondAcquired:
		t.Fatal("second request acquired the same product lock before the first released it")
	case err := <-secondError:
		t.Fatalf("second lock failed before release: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := firstUnlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case unlock := <-secondAcquired:
		if err := unlock(context.Background()); err != nil {
			t.Fatal(err)
		}
	case err := <-secondError:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("second request did not acquire the product lock after release")
	}
	if got := testutil.ToFloat64(metrics.Acquires); got != 2 {
		t.Fatalf("etcd_lock_acquire_total=%v, want 2", got)
	}
}

func TestEtcdLockerRecordsAcquireFailure(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := lock.NewMetrics(registry)
	locker, err := lock.NewEtcd(config.EtcdConfig{
		Endpoints:          []string{"127.0.0.1:1"},
		DialTimeout:        50 * time.Millisecond,
		LockSessionTTL:     time.Second,
		LockAcquireTimeout: 100 * time.Millisecond,
	}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = locker.Close() })
	if _, err := locker.Lock(context.Background(), 2001); err == nil {
		t.Fatal("expected Etcd lock acquisition to fail")
	}
	if got := testutil.ToFloat64(metrics.Failures); got != 1 {
		t.Fatalf("etcd_lock_failure_total=%v, want 1", got)
	}
}
