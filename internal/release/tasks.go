// release 包负责可持久化、可重试的库存释放任务。
package release

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/prometheus/client_golang/prometheus"
)

type Lifecycle struct {
	OrderID                  int64          `db:"order_id"`
	ReservationID            sql.NullString `db:"reservation_id"`
	EventID                  sql.NullString `db:"event_id"`
	UserID                   int64          `db:"user_id"`
	ProductID                sql.NullInt64  `db:"product_id"`
	ActivityID               sql.NullInt64  `db:"activity_id"`
	Quantity                 sql.NullInt64  `db:"quantity"`
	Status                   string         `db:"status"`
	PersistentInventoryState string         `db:"persistent_inventory_state"`
}

type Task struct {
	ReleaseID     string    `db:"release_id"`
	ReservationID string    `db:"reservation_id"`
	OrderID       int64     `db:"order_id"`
	ActivityID    int64     `db:"activity_id"`
	ProductID     int64     `db:"product_id"`
	Quantity      int64     `db:"quantity"`
	RestoreMySQL  bool      `db:"restore_mysql"`
	Status        string    `db:"status"`
	AttemptCount  int       `db:"attempt_count"`
	NextAttemptAt time.Time `db:"next_attempt_at"`
	LeaseToken    string    `db:"lease_token"`
	LeaseUntil    time.Time `db:"lease_until"`
	LastError     string    `db:"last_error"`
}

func releaseID(reservationID string) string { return "release:" + reservationID }

// CreateForLifecycle 在将生命周期记录迁移为 cancelling 的同一事务内写入释放任务。
// 如果提前取消意图尚未被 Kafka Consumer 完成，则返回 false。
func CreateForLifecycle(ctx context.Context, tx *sqlx.Tx, row Lifecycle) (bool, error) {
	if !row.ReservationID.Valid || !row.ProductID.Valid || !row.ActivityID.Valid || !row.Quantity.Valid || row.ReservationID.String == "" || row.ProductID.Int64 <= 0 || row.ActivityID.Int64 <= 0 || row.Quantity.Int64 <= 0 {
		return false, nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO inventory_release_tasks
		(release_id, reservation_id, order_id, activity_id, product_id, quantity, restore_mysql, status, next_attempt_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE release_id = release_id`,
		releaseID(row.ReservationID.String), row.ReservationID.String, row.OrderID, row.ActivityID.Int64, row.ProductID.Int64, row.Quantity.Int64, row.PersistentInventoryState == "applied")
	if err != nil {
		return false, fmt.Errorf("insert inventory release task: %w", err)
	}
	return true, nil
}

func LoadLifecycleForUpdate(ctx context.Context, tx *sqlx.Tx, orderID int64) (Lifecycle, error) {
	var row Lifecycle
	err := tx.GetContext(ctx, &row, `SELECT order_id, reservation_id, event_id, user_id, product_id, activity_id, quantity, status, persistent_inventory_state FROM order_lifecycle WHERE order_id = ? FOR UPDATE`, orderID)
	if err != nil {
		return Lifecycle{}, err
	}
	return row, nil
}

type Metrics struct {
	Pending  prometheus.Gauge
	Duration prometheus.Histogram
	Retries  prometheus.Counter
	Failed   prometheus.Counter
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Pending:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "inventory_release_tasks_pending", Help: "Durable inventory release tasks not completed."}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "inventory_release_duration_seconds", Help: "Inventory release worker processing time."}),
		Retries:  prometheus.NewCounter(prometheus.CounterOpts{Name: "inventory_release_retry_total", Help: "Inventory release retries scheduled."}),
		Failed:   prometheus.NewCounter(prometheus.CounterOpts{Name: "inventory_release_failed_total", Help: "Inventory release task failures."}),
	}
	registry.MustRegister(m.Pending, m.Duration, m.Retries, m.Failed)
	return m
}

type Worker struct {
	db             *sqlx.DB
	stock          inventory.Store
	metrics        *Metrics
	lease          time.Duration
	beforeComplete func(Task) error
}

func NewWorker(db *sqlx.DB, stock inventory.Store, metrics *Metrics, lease time.Duration) *Worker {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	return &Worker{db: db, stock: stock, metrics: metrics, lease: lease}
}

// NewWorkerWithBeforeComplete 提供确定性的故障注入点，供集成测试使用。
// 生产调用方使用 NewWorker，因此不存在该注入失败路径。
func NewWorkerWithBeforeComplete(db *sqlx.DB, stock inventory.Store, metrics *Metrics, lease time.Duration, beforeComplete func(Task) error) *Worker {
	w := NewWorker(db, stock, metrics, lease)
	w.beforeComplete = beforeComplete
	return w
}

func (w *Worker) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := w.RunOnce(ctx, 20); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 1
	}
	tasks, err := w.claim(ctx, limit)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		w.process(ctx, task)
	}
	if w.metrics != nil {
		var pending int
		if err := w.db.GetContext(ctx, &pending, `SELECT COUNT(*) FROM inventory_release_tasks WHERE status <> 'completed'`); err == nil {
			w.metrics.Pending.Set(float64(pending))
		}
	}
	return nil
}

func (w *Worker) claim(ctx context.Context, limit int) ([]Task, error) {
	tx, err := w.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var tasks []Task
	if err := tx.SelectContext(ctx, &tasks, `SELECT release_id, reservation_id, order_id, activity_id, product_id, quantity, restore_mysql, status, attempt_count, next_attempt_at, COALESCE(lease_token, '') AS lease_token
		FROM inventory_release_tasks
		WHERE (status = 'pending' AND next_attempt_at <= UTC_TIMESTAMP(6)) OR (status = 'processing' AND lease_until < UTC_TIMESTAMP(6))
		ORDER BY next_attempt_at, created_at LIMIT ? FOR UPDATE SKIP LOCKED`, limit); err != nil {
		return nil, fmt.Errorf("claim release tasks: %w", err)
	}
	for i := range tasks {
		token, err := token()
		if err != nil {
			return nil, err
		}
		until := time.Now().UTC().Add(w.lease)
		if _, err := tx.ExecContext(ctx, `UPDATE inventory_release_tasks SET status='processing', lease_token=?, lease_until=?, attempt_count=attempt_count+1 WHERE release_id=?`, token, until, tasks[i].ReleaseID); err != nil {
			return nil, err
		}
		tasks[i].LeaseToken = token
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (w *Worker) process(ctx context.Context, task Task) {
	started := time.Now()
	defer func() {
		if w.metrics != nil {
			w.metrics.Duration.Observe(time.Since(started).Seconds())
		}
	}()
	reservation := inventory.Reservation{ReservationID: task.ReservationID, OrderID: task.OrderID, ActivityID: task.ActivityID, ProductID: task.ProductID, Quantity: task.Quantity}
	if _, err := w.stock.Release(ctx, reservation, task.ReleaseID); err != nil {
		w.retry(ctx, task, err)
		return
	}
	if w.beforeComplete != nil {
		if err := w.beforeComplete(task); err != nil {
			w.retry(ctx, task, err)
			return
		}
	}
	if err := w.complete(ctx, task); err != nil {
		w.retry(ctx, task, err)
	}
}

func (w *Worker) complete(ctx context.Context, task Task) error {
	tx, err := w.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current Task
	if err := tx.GetContext(ctx, &current, `SELECT release_id, reservation_id, order_id, activity_id, product_id, quantity, restore_mysql, status, attempt_count, next_attempt_at, COALESCE(lease_token, '') AS lease_token FROM inventory_release_tasks WHERE release_id = ? FOR UPDATE`, task.ReleaseID); err != nil {
		return err
	}
	if current.Status == "completed" {
		return tx.Commit()
	}
	if current.Status != "processing" || current.LeaseToken != task.LeaseToken {
		return fmt.Errorf("release lease was lost")
	}
	if current.RestoreMySQL {
		if result, err := tx.ExecContext(ctx, `UPDATE seckill_activities SET stock = stock + ? WHERE id = ? AND product_id = ?`, current.Quantity, current.ActivityID, current.ProductID); err != nil {
			return err
		} else if n, _ := result.RowsAffected(); n != 1 {
			return fmt.Errorf("activity missing while restoring stock")
		}
		if result, err := tx.ExecContext(ctx, `UPDATE inventories SET available = available + ?, sold = sold - ?, version = version + 1 WHERE product_id = ? AND sold >= ?`, current.Quantity, current.Quantity, current.ProductID, current.Quantity); err != nil {
			return err
		} else if n, _ := result.RowsAffected(); n != 1 {
			return fmt.Errorf("persistent inventory cannot be restored")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET persistent_inventory_state='restored', reservation_state='released', status='cancelled' WHERE order_id=? AND status='cancelling'`, current.OrderID); err != nil {
			return err
		}
	} else if _, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET reservation_state='released', status='cancelled' WHERE order_id=? AND status='cancelling'`, current.OrderID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status='cancelled' WHERE id=? AND status='cancelling'`, current.OrderID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory_release_tasks SET status='completed', lease_token=NULL, lease_until=NULL, last_error=NULL WHERE release_id=? AND status='processing' AND lease_token=?`, current.ReleaseID, task.LeaseToken); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *Worker) retry(ctx context.Context, task Task, cause error) {
	if w.metrics != nil {
		w.metrics.Failed.Inc()
		w.metrics.Retries.Inc()
	}
	backoff := time.Second * time.Duration(1<<min(task.AttemptCount, 6))
	if backoff > time.Minute {
		backoff = time.Minute
	}
	_, _ = w.db.ExecContext(ctx, `UPDATE inventory_release_tasks SET status='pending', lease_token=NULL, lease_until=NULL, next_attempt_at=?, last_error=? WHERE release_id=? AND status='processing' AND lease_token=?`, time.Now().UTC().Add(backoff), truncate(cause.Error(), 1024), task.ReleaseID, task.LeaseToken)
}

func token() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func truncate(v string, n int) string {
	if len(v) <= n {
		return v
	}
	return v[:n]
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
