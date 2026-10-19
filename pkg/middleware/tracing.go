package middleware

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const TraceIDHeader = "X-Trace-ID"

// Tracing 提取 W3C traceparent，创建服务端 Span 并继续传递链路上下文。
func Tracing(service string) gin.HandlerFunc {
	tracer := otel.Tracer(service)
	propagator := otel.GetTextMapPropagator()
	return func(c *gin.Context) {
		parent := propagator.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		ctx, span := tracer.Start(parent, fmt.Sprintf("%s %s", c.Request.Method, c.Request.URL.Path), trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		traceID := span.SpanContext().TraceID().String()
		c.Set("trace_id", traceID)
		c.Header(TraceIDHeader, traceID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		span.SetAttributes(
			attribute.String("http.request.method", c.Request.Method),
			attribute.String("http.route", c.FullPath()),
			attribute.Int("http.response.status_code", c.Writer.Status()),
		)
	}
}
