package limiter

import "fmt"

type KeyBuilder struct{}

func (KeyBuilder) Global() string { return "seckill:limit:global" }

func (KeyBuilder) Product(productID int64) string {
	return fmt.Sprintf("seckill:limit:product:{%d}", productID)
}

func (KeyBuilder) User(activityID, userID int64) string {
	return fmt.Sprintf("seckill:user:{%d}:%d", activityID, userID)
}
