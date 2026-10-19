// lock 包提供 M5 中可选的、按商品粒度的 Etcd 互斥锁。
package lock

import (
	"context"
	"time"

	"github.com/pengtor/seckill-agent/internal/fault"
)

// UnlockFunc 释放一个已获取的互斥锁。
type UnlockFunc func(context.Context) error

// Locker 将同一商品下单流程中受保护的部分串行化。
type Locker interface {
	Lock(ctx context.Context, productID int64) (UnlockFunc, error)
	Close() error
}

type noopLocker struct{}

type DelayLocker struct {
	Locker
	Delay time.Duration
}

func (l DelayLocker) Lock(ctx context.Context, productID int64) (UnlockFunc, error) {
	unlock, err := l.Locker.Lock(ctx, productID)
	if err != nil {
		return nil, err
	}
	if err := fault.Delay(ctx, l.Delay); err != nil {
		unlockContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = unlock(unlockContext)
		return nil, err
	}
	return unlock, nil
}

// NewNoop 返回 redis_lua_only 实现，不会增加锁。
func NewNoop() Locker { return noopLocker{} }

func (noopLocker) Lock(context.Context, int64) (UnlockFunc, error) {
	return func(context.Context) error { return nil }, nil
}

func (noopLocker) Close() error { return nil }
