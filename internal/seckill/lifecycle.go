package seckill

import (
	"context"
	"database/sql"
	"errors"

	"github.com/pengtor/seckill-agent/internal/release"
	"github.com/pengtor/seckill-agent/pkg/apperror"
)

// MarkOrderPaid 是内部模拟支付操作。在服务具备可信用户认证前，
// 它刻意不提供公开 HTTP 路由。
func (s *Service) MarkOrderPaid(ctx context.Context, orderID int64) *apperror.Error {
	if orderID <= 0 {
		return invalidInput("order_id must be positive")
	}
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return internal(err)
	}
	defer tx.Rollback()

	var order Order
	err = tx.GetContext(ctx, &order, `SELECT id, user_id, product_id, activity_id, quantity, amount, status, created_at, updated_at FROM orders WHERE id = ?`, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return apperror.New("ORDER_NOT_FOUND", "order not found", 404, err)
	}
	if err != nil {
		return internal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO order_lifecycle (order_id, user_id, product_id, activity_id, status)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE order_id = VALUES(order_id)`, order.ID, order.UserID, order.ProductID, order.ActivityID, order.Status); err != nil {
		return internal(err)
	}
	var lifecycleStatus string
	if err := tx.GetContext(ctx, &lifecycleStatus, `SELECT status FROM order_lifecycle WHERE order_id = ? FOR UPDATE`, orderID); err != nil {
		return internal(err)
	}
	if lifecycleStatus == "succeeded" && order.Status == "succeeded" {
		if err := tx.Commit(); err != nil {
			return internal(err)
		}
		return nil
	}
	if lifecycleStatus != "created" || order.Status != "created" {
		return apperror.New("ORDER_STATE_CONFLICT", "order is not payable in its current state", 409, nil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status = 'succeeded' WHERE id = ? AND status = 'created'`, orderID); err != nil {
		return internal(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET status = 'succeeded' WHERE order_id = ? AND status = 'created'`, orderID)
	if err != nil {
		return internal(err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return apperror.New("ORDER_STATE_CONFLICT", "order state changed concurrently", 409, nil)
	}
	if err := tx.Commit(); err != nil {
		return internal(err)
	}
	return nil
}

// 在独立持久化的释放任务恢复两侧库存账本前，CancellationResult 会刻意保持 pending。
type CancellationResult struct {
	Status string `json:"status"`
}

// CancelOrder 通过生命周期记录与模拟支付进行仲裁。调用方身份由 HTTP 认证边界提供；
// 本方法仍会将其与持久化的生命周期归属者校验。
func (s *Service) CancelOrder(ctx context.Context, orderID, userID int64) (CancellationResult, *apperror.Error) {
	if orderID <= 0 || userID <= 0 {
		return CancellationResult{}, invalidInput("order_id and user_id must be positive")
	}
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return CancellationResult{}, internal(err)
	}
	defer tx.Rollback()
	// 这里与 Consumer 使用相同的 INSERT ... ON DUPLICATE KEY 仲裁点。
	// 它使取消意图能在 Kafka 持久化订单事实前存活，避免“先查后插”的竞态。
	if _, err := tx.ExecContext(ctx, `INSERT INTO order_lifecycle (order_id, user_id, status) VALUES (?, ?, 'cancelling') ON DUPLICATE KEY UPDATE order_id = VALUES(order_id)`, orderID, userID); err != nil {
		return CancellationResult{}, internal(err)
	}
	row, err := release.LoadLifecycleForUpdate(ctx, tx, orderID)
	if err != nil {
		return CancellationResult{}, internal(err)
	}
	if row.UserID != userID {
		return CancellationResult{}, apperror.New("ORDER_FORBIDDEN", "order does not belong to caller", 403, nil)
	}
	switch row.Status {
	case "succeeded", "rejected":
		return CancellationResult{}, apperror.New("ORDER_STATE_CONFLICT", "order cannot be cancelled in its current state", 409, nil)
	case "cancelled":
		if err := tx.Commit(); err != nil {
			return CancellationResult{}, internal(err)
		}
		return CancellationResult{Status: "cancelled"}, nil
	case "created":
		if _, err := tx.ExecContext(ctx, `UPDATE order_lifecycle SET status='cancelling' WHERE order_id=? AND status='created'`, orderID); err != nil {
			return CancellationResult{}, internal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE orders SET status='cancelling' WHERE id=? AND status='created'`, orderID); err != nil {
			return CancellationResult{}, internal(err)
		}
		row.Status = "cancelling"
	case "cancelling":
		// 幂等：并发调用方或更早的 Consumer 前取消意图已拥有同一生命周期记录和释放任务。
	default:
		return CancellationResult{}, apperror.New("ORDER_STATE_CONFLICT", "unknown order lifecycle state", 409, nil)
	}
	if _, err := release.CreateForLifecycle(ctx, tx, row); err != nil {
		return CancellationResult{}, internal(err)
	}
	if err := tx.Commit(); err != nil {
		return CancellationResult{}, internal(err)
	}
	return CancellationResult{Status: "cancelling"}, nil
}
