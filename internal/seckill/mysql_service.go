// seckill 包实现面向事务的秒杀服务。
package seckill

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pengtor/seckill-agent/internal/inventory"
	"github.com/pengtor/seckill-agent/internal/lock"
	"github.com/pengtor/seckill-agent/pkg/apperror"
)

var moneyPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]{1,2})?$`)

type Service struct {
	db     *sqlx.DB
	stock  inventory.Store
	async  *asyncDependencies
	locker lock.Locker
}

func NewService(db *sqlx.DB) *Service { return &Service{db: db} }

func NewServiceWithStock(db *sqlx.DB, stock inventory.Store) *Service {
	return &Service{db: db, stock: stock}
}

type CreateProductInput struct {
	Name  string `json:"name"`
	Price string `json:"price"`
	Stock int64  `json:"stock"`
}

type CreateActivityInput struct {
	ProductID    int64     `json:"product_id"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	SeckillPrice string    `json:"seckill_price"`
	Stock        int64     `json:"stock"`
}

type PurchaseInput struct {
	ActivityID int64 `json:"activity_id"`
	ProductID  int64 `json:"product_id"`
	UserID     int64 `json:"user_id"`
}

type Product struct {
	ID        int64     `db:"id" json:"id"`
	Name      string    `db:"name" json:"name"`
	Price     string    `db:"price" json:"price"`
	Status    string    `db:"status" json:"status"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

type Activity struct {
	ID           int64     `db:"id" json:"id"`
	ProductID    int64     `db:"product_id" json:"product_id"`
	StartTime    time.Time `db:"start_time" json:"start_time"`
	EndTime      time.Time `db:"end_time" json:"end_time"`
	SeckillPrice string    `db:"seckill_price" json:"seckill_price"`
	Stock        int64     `db:"stock" json:"stock"`
	Status       string    `db:"status" json:"status"`
}

type Order struct {
	ID         int64     `db:"id" json:"id"`
	UserID     int64     `db:"user_id" json:"user_id"`
	ProductID  int64     `db:"product_id" json:"product_id"`
	ActivityID int64     `db:"activity_id" json:"activity_id"`
	Quantity   int64     `db:"quantity" json:"quantity"`
	Amount     string    `db:"amount" json:"amount"`
	Status     string    `db:"status" json:"status"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

func (s *Service) CreateProduct(ctx context.Context, input CreateProductInput) (Product, *apperror.Error) {
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" || len(input.Name) > 120 || !validMoney(input.Price) || input.Stock <= 0 {
		return Product{}, invalidInput("name, price and positive stock are required")
	}
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Product{}, internal(err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO products (name, price, status) VALUES (?, ?, 'active')`, input.Name, input.Price)
	if err != nil {
		return Product{}, internal(err)
	}
	productID, err := result.LastInsertId()
	if err != nil {
		return Product{}, internal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inventories (product_id, total, available, sold, version) VALUES (?, ?, ?, 0, 0)`, productID, input.Stock, input.Stock); err != nil {
		return Product{}, internal(err)
	}
	if err := tx.Commit(); err != nil {
		return Product{}, internal(err)
	}
	return Product{ID: productID, Name: input.Name, Price: input.Price, Status: "active"}, nil
}

func (s *Service) CreateActivity(ctx context.Context, input CreateActivityInput) (Activity, *apperror.Error) {
	if input.ProductID <= 0 || input.Stock <= 0 || !input.EndTime.After(input.StartTime) || !validMoney(input.SeckillPrice) {
		return Activity{}, invalidInput("valid product_id, time window, seckill_price and positive stock are required")
	}
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Activity{}, internal(err)
	}
	defer tx.Rollback()
	var exists int
	if err := tx.GetContext(ctx, &exists, `SELECT 1 FROM products WHERE id = ? FOR UPDATE`, input.ProductID); errors.Is(err, sql.ErrNoRows) {
		return Activity{}, apperror.New("PRODUCT_NOT_FOUND", "product not found", 404, err)
	} else if err != nil {
		return Activity{}, internal(err)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO seckill_activities (product_id, start_time, end_time, seckill_price, stock, status) VALUES (?, ?, ?, ?, ?, 'active')`, input.ProductID, input.StartTime.UTC(), input.EndTime.UTC(), input.SeckillPrice, input.Stock)
	if err != nil {
		return Activity{}, internal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Activity{}, internal(err)
	}
	if err := tx.Commit(); err != nil {
		return Activity{}, internal(err)
	}
	return Activity{ID: id, ProductID: input.ProductID, StartTime: input.StartTime.UTC(), EndTime: input.EndTime.UTC(), SeckillPrice: input.SeckillPrice, Stock: input.Stock, Status: "active"}, nil
}

func (s *Service) WarmupActivity(ctx context.Context, activityID int64) (inventory.WarmupResult, *apperror.Error) {
	if s.stock == nil {
		return inventory.WarmupResult{}, apperror.New("REDIS_STOCK_DISABLED", "redis stock is not enabled", 503, nil)
	}
	if activityID <= 0 {
		return inventory.WarmupResult{}, invalidInput("activity id must be positive")
	}
	var source struct {
		ProductID int64  `db:"product_id"`
		Stock     int64  `db:"stock"`
		Available int64  `db:"available"`
		Status    string `db:"status"`
	}
	err := s.db.GetContext(ctx, &source, `SELECT a.product_id, a.stock, i.available, a.status FROM seckill_activities a JOIN inventories i ON i.product_id = a.product_id WHERE a.id = ?`, activityID)
	if errors.Is(err, sql.ErrNoRows) {
		return inventory.WarmupResult{}, apperror.New("ACTIVITY_NOT_FOUND", "activity not found", 404, err)
	}
	if err != nil {
		return inventory.WarmupResult{}, internal(err)
	}
	if source.Status != "active" {
		return inventory.WarmupResult{}, apperror.New("ACTIVITY_NOT_ACTIVE", "activity is not active", 409, nil)
	}
	stock := min(source.Stock, source.Available)
	result, err := s.stock.Warmup(ctx, activityID, source.ProductID, stock)
	if err != nil {
		return inventory.WarmupResult{}, apperror.New("REDIS_OPERATION_FAILED", "redis stock warmup failed", 503, err)
	}
	return result, nil
}

func (s *Service) GetOrder(ctx context.Context, orderID int64) (Order, *apperror.Error) {
	var order Order
	err := s.db.GetContext(ctx, &order, `SELECT id, user_id, product_id, activity_id, quantity, amount, status, created_at, updated_at FROM orders WHERE id = ?`, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		// 提前取消会在 Kafka 事件创建 orders 记录前刻意持久化。
		// 应通过已有订单查询暴露该生命周期进度，不能假装已接受的意图丢失。
		// 商品事实要等 Consumer 到达后才可获取。
		var lifecycle struct {
			OrderID   int64     `db:"order_id"`
			UserID    int64     `db:"user_id"`
			Status    string    `db:"status"`
			CreatedAt time.Time `db:"created_at"`
			UpdatedAt time.Time `db:"updated_at"`
		}
		lifecycleErr := s.db.GetContext(ctx, &lifecycle, `SELECT order_id, user_id, status, created_at, updated_at FROM order_lifecycle WHERE order_id = ?`, orderID)
		if lifecycleErr == nil {
			return Order{ID: lifecycle.OrderID, UserID: lifecycle.UserID, Status: lifecycle.Status, CreatedAt: lifecycle.CreatedAt, UpdatedAt: lifecycle.UpdatedAt}, nil
		}
		if errors.Is(lifecycleErr, sql.ErrNoRows) {
			return Order{}, apperror.New("ORDER_NOT_FOUND", "order not found", 404, err)
		}
		return Order{}, internal(lifecycleErr)
	}
	if err != nil {
		return Order{}, internal(err)
	}
	return order, nil
}

func (s *Service) ListUserOrders(ctx context.Context, userID int64) ([]Order, *apperror.Error) {
	if userID <= 0 {
		return nil, invalidInput("user_id must be positive")
	}
	orders := make([]Order, 0)
	if err := s.db.SelectContext(ctx, &orders, `SELECT id, user_id, product_id, activity_id, quantity, amount, status, created_at, updated_at FROM orders WHERE user_id = ? ORDER BY id DESC`, userID); err != nil {
		return nil, internal(err)
	}
	return orders, nil
}

func validMoney(value string) bool { return moneyPattern.MatchString(strings.TrimSpace(value)) }

func invalidInput(message string) *apperror.Error {
	return apperror.New("VALIDATION_ERROR", message, 400, nil)
}

func stockExhausted() *apperror.Error {
	return apperror.New("SECKILL_STOCK_EXHAUSTED", "stock exhausted", 409, nil)
}

func newReservationID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate reservation id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func internal(err error) *apperror.Error {
	return apperror.Internal(fmt.Errorf("seckill storage: %w", err))
}
