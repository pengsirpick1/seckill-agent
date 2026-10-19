package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestIDPreservesCallerValue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID())
	router.GET("/", func(c *gin.Context) {
		if got := RequestIDFromContext(c.Request.Context()); got != "caller-id" {
			t.Fatalf("context request id = %q", got)
		}
		c.Status(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(RequestIDHeader, "caller-id")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if got := response.Header().Get(RequestIDHeader); got != "caller-id" {
		t.Fatalf("header request id = %q", got)
	}
}
