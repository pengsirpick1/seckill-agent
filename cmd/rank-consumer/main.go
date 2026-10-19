package main

import (
	"github.com/pengtor/seckill-agent/internal/bootstrap"
	"github.com/pengtor/seckill-agent/internal/ranking"
)

func main() {
	bootstrap.Main("rank-consumer", ranking.RunConsumer)
}
