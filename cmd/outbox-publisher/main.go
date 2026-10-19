package main

import (
	"github.com/pengtor/seckill-agent/internal/bootstrap"
	"github.com/pengtor/seckill-agent/internal/outbox"
)

func main() {
	bootstrap.Main("outbox-publisher", outbox.Run)
}
