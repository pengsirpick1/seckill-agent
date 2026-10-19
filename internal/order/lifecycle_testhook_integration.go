//go:build integration

package order

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// RecordEarlyCancellationIntentForTest 只会在集成测试中编译。
// 它创建或迁移生命周期仲裁记录，不暴露生产取消 API，也不创建库存释放任务。
func RecordEarlyCancellationIntentForTest(ctx context.Context, db *sqlx.DB, orderID, userID int64) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO order_lifecycle (order_id, user_id, status) VALUES (?, ?, 'cancelling')
		ON DUPLICATE KEY UPDATE status = IF(status = 'created', 'cancelling', status)`, orderID, userID); err != nil {
		return fmt.Errorf("upsert test cancellation intent: %w", err)
	}
	var row struct {
		UserID int64  `db:"user_id"`
		Status string `db:"status"`
	}
	if err := tx.GetContext(ctx, &row, `SELECT user_id, status FROM order_lifecycle WHERE order_id = ? FOR UPDATE`, orderID); err != nil {
		return err
	}
	if row.UserID != userID {
		return fmt.Errorf("order owner mismatch")
	}
	if row.Status != "cancelling" {
		return fmt.Errorf("order state conflict: %s", row.Status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status = 'cancelling' WHERE id = ? AND status = 'created'`, orderID); err != nil {
		return err
	}
	return tx.Commit()
}
