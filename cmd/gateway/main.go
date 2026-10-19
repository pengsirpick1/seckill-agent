package main

import (
	"github.com/pengtor/seckill-agent/internal/bootstrap"
	"github.com/pengtor/seckill-agent/internal/gateway"
)

func main() {
	bootstrap.Main("gateway", gateway.Run)
}
