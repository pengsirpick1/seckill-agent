// httpserver 包提供感知系统信号的 HTTP 服务生命周期管理。
package httpserver

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Run(parent context.Context, server *http.Server, shutdownTimeout time.Duration) error {
	stopContext, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errorsCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsCh <- err
		}
		close(errorsCh)
	}()

	select {
	case err := <-errorsCh:
		return err
	case <-stopContext.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownContext)
	}
}
