// fault 包提供可选的、感知 Context 取消的延迟能力，用于开发环境故障演练。
package fault

import (
	"context"
	"time"
)

// Delay 等待指定时长；操作 Context 结束时会提前返回。
func Delay(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
