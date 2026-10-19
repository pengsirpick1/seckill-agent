package order

import (
	"fmt"
	"sync"
	"time"
)

// IDGenerator 生成可按时间排序的 int64 订单 ID。各 Gateway 副本必须配置不同的节点 ID；
// 默认值只适用于单个 Gateway 实例。
type IDGenerator struct {
	mu         sync.Mutex
	nodeID     int64
	lastMillis int64
	sequence   int64
}

func NewIDGenerator(nodeID int64) (*IDGenerator, error) {
	if nodeID < 0 || nodeID > 1023 {
		return nil, fmt.Errorf("node id must be between 0 and 1023")
	}
	return &IDGenerator{nodeID: nodeID}, nil
}

func (g *IDGenerator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixMilli()
	if now == g.lastMillis {
		g.sequence = (g.sequence + 1) & 0xFFF
		if g.sequence == 0 {
			for now <= g.lastMillis {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}
	g.lastMillis = now
	return (now << 22) | (g.nodeID << 12) | g.sequence
}
