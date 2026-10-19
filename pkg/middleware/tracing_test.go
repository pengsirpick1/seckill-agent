package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestTracingPreservesIncomingTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	router := gin.New()
	router.Use(Tracing("test"))
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if got, want := response.Header().Get(TraceIDHeader), "4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Fatalf("%s = %q, want %q", TraceIDHeader, got, want)
	}
}
