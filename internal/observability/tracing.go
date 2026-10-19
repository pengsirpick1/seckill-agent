// observability 包负责运行时链路追踪和共用指标端点。
package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracing 配置 W3C Trace Context 传播和 OTLP HTTP 导出器。
func InitTracing(ctx context.Context, cfg config.Config) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(cfg.Observability.OTLPEndpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	provider := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes("", semconv.ServiceName(cfg.ServiceName))),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

// RunMetricsServer 为后台 Consumer 暴露指标和健康检查。
func RunMetricsServer(ctx context.Context, address, service string, handler http.Handler, log *slog.Logger) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok","service":"` + service + `"}`))
	})
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server stopped unexpectedly", "error", err)
		}
	}()
	return func() {
		shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}
}
