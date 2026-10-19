package main

import (
	"github.com/pengtor/seckill-agent/internal/bootstrap"
	"github.com/pengtor/seckill-agent/internal/order"
)

func main() {
	bootstrap.Main("order-consumer", order.RunConsumer)
}
