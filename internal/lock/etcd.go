package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// EtcdLocker 每次获取锁时创建一个短生命周期会话。并发请求之间不能共享会话，
// 因为 concurrency.Mutex 通过租约 ID 标识锁竞争者。
type EtcdLocker struct {
	client         *clientv3.Client
	metrics        *Metrics
	sessionTTL     int
	acquireTimeout time.Duration
}

func NewEtcd(cfg config.EtcdConfig, metrics *Metrics) (*EtcdLocker, error) {
	client, err := clientv3.New(clientv3.Config{Endpoints: cfg.Endpoints, DialTimeout: cfg.DialTimeout})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}
	return &EtcdLocker{
		client:         client,
		metrics:        metrics,
		sessionTTL:     max(1, int(cfg.LockSessionTTL.Seconds())),
		acquireTimeout: cfg.LockAcquireTimeout,
	}, nil
}

func (l *EtcdLocker) Lock(ctx context.Context, productID int64) (UnlockFunc, error) {
	started := time.Now()
	acquireContext, cancel := context.WithTimeout(ctx, l.acquireTimeout)
	defer cancel()

	lease, err := l.client.Grant(acquireContext, int64(l.sessionTTL))
	if err != nil {
		l.observeFailure(started)
		return nil, fmt.Errorf("grant Etcd lock lease: %w", err)
	}
	// 会话会在客户端连接断开后继续存在，直到 Unlock 将其关闭。
	// 上方刻意使用 acquireContext 执行 Grant：否则 NewSession 会用自己的会话 Context
	// 处理第一次租约请求，Etcd 不可用时可能无限等待。
	sessionContext, cancelSession := context.WithCancel(context.WithoutCancel(ctx))
	session, err := concurrency.NewSession(l.client, concurrency.WithLease(lease.ID), concurrency.WithContext(sessionContext))
	if err != nil {
		cancelSession()
		revokeContext, cancelRevoke := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		_, _ = l.client.Revoke(revokeContext, lease.ID)
		cancelRevoke()
		l.observeFailure(started)
		return nil, fmt.Errorf("create etcd lock session: %w", err)
	}
	mutex := concurrency.NewMutex(session, fmt.Sprintf("seckill:lock:product:%d", productID))
	if err := mutex.Lock(acquireContext); err != nil {
		cancelSession()
		_ = session.Close()
		l.observeFailure(started)
		return nil, fmt.Errorf("acquire Etcd product mutex: %w", err)
	}
	l.metrics.WaitDuration.WithLabelValues("acquired").Observe(time.Since(started).Seconds())
	l.metrics.Acquires.Inc()

	return func(unlockContext context.Context) error {
		defer cancelSession()
		defer session.Close()
		if err := mutex.Unlock(unlockContext); err != nil {
			return fmt.Errorf("release Etcd product mutex: %w", err)
		}
		return nil
	}, nil
}

func (l *EtcdLocker) observeFailure(started time.Time) {
	l.metrics.WaitDuration.WithLabelValues("failed").Observe(time.Since(started).Seconds())
	l.metrics.Failures.Inc()
}

func (l *EtcdLocker) Close() error { return l.client.Close() }
