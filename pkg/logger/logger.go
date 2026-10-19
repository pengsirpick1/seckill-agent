package logger

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

var processLogs = NewRing(2000)

func New(level, service string) *slog.Logger {
	var configured slog.Level
	switch strings.ToLower(level) {
	case "debug":
		configured = slog.LevelDebug
	case "warn", "warning":
		configured = slog.LevelWarn
	case "error":
		configured = slog.LevelError
	default:
		configured = slog.LevelInfo
	}
	writers := []io.Writer{os.Stdout, processLogs}
	if endpoint := strings.TrimSpace(os.Getenv("LOG_SHIPPER_URL")); endpoint != "" {
		writers = append(writers, newLokiWriter(endpoint))
	}
	writer := io.MultiWriter(writers...)
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: configured})).With("service", service)
}
