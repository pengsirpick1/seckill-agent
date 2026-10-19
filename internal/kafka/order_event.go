package kafka

import "time"

const OrderCreatedEventType = "order.created"
const OrderSucceededEventType = "order.succeeded"

type OrderCreatedEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OrderID       int64     `json:"order_id"`
	ActivityID    int64     `json:"activity_id"`
	ProductID     int64     `json:"product_id"`
	UserID        int64     `json:"user_id"`
	Quantity      int64     `json:"quantity"`
	Amount        string    `json:"amount"`
	ReservationID string    `json:"reservation_id"`
	TraceID       string    `json:"trace_id,omitempty"`
	TraceParent   string    `json:"traceparent,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// OrderSucceededEvent 只会在订单事务提交后发出。
type OrderSucceededEvent struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	OrderID     int64     `json:"order_id"`
	ActivityID  int64     `json:"activity_id"`
	ProductID   int64     `json:"product_id"`
	UserID      int64     `json:"user_id"`
	TraceID     string    `json:"trace_id,omitempty"`
	TraceParent string    `json:"traceparent,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}
