package main

import (
	"github.com/pengtor/seckill-agent/internal/bootstrap"
	"github.com/pengtor/seckill-agent/internal/release"
)

func main() {
	bootstrap.Main("release-worker", release.Run)
}
