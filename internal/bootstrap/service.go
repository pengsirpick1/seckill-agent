// bootstrap 包提供各可执行程序共用的进程生命周期管理。
// 它只负责配置、日志、系统信号和退出状态；依赖仍由使用它的服务自行构建。
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/pengtor/seckill-agent/pkg/logger"
)

// Runner 是服务特有的进程主体。
type Runner func(context.Context, config.Config, *slog.Logger) error

// Main 加载进程共用依赖，将终止信号传递给 Runner，并将 Runner 的非预期失败转换为非零退出码。
func Main(service string, run Runner) {
	if err := Run(service, run); err != nil {
		slog.Error("service stopped unexpectedly", "service", service, "error", err)
		os.Exit(1)
	}
}

// Run 是可测试的 Main 变体。正常进程关闭导致的 Context 取消不视为服务失败。
func Run(service string, run Runner) error {
	if run == nil {
		return fmt.Errorf("%s runner is required", service)
	}
	cfg, err := config.Load(service)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	log := logger.New(cfg.LogLevel, cfg.ServiceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
