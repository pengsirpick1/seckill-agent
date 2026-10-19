package inventory

import "fmt"

type KeyBuilder struct{}

func (KeyBuilder) Stock(activityID, productID int64) string {
	return fmt.Sprintf("seckill:stock:{%d}:%d", activityID, productID)
}

func (KeyBuilder) Reservation(activityID int64, reservationID string) string {
	return fmt.Sprintf("seckill:reservation:{%d}:%s", activityID, reservationID)
}

// ReservationOutbox 与活动的库存和预占 Hash 放在同一哈希槽。
// 保持相同 Hash Tag 后，预占 Lua 脚本既能用于 Redis Cluster，也能用于单机 Redis。
func (KeyBuilder) ReservationOutbox(activityID int64) string {
	return fmt.Sprintf("seckill:reservation-outbox:{%d}", activityID)
}

func (KeyBuilder) Ranking(activityID int64) string {
	return fmt.Sprintf("seckill:ranking:{%d}", activityID)
}
