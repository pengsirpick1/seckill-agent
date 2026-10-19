package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

type HTTPMetrics struct {
	Requests *prometheus.CounterVec
	Duration *prometheus.HistogramVec
}

func NewHTTPMetrics(registry prometheus.Registerer) *HTTPMetrics {
	metrics := &HTTPMetrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests."}, []string{"method", "route", "status"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request latency in seconds.", Buckets: prometheus.DefBuckets}, []string{"method", "route", "status"}),
	}
	registry.MustRegister(metrics.Requests, metrics.Duration)
	return metrics
}

func (m *HTTPMetrics) Observe() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		labels := []string{c.Request.Method, route, strconv.Itoa(c.Writer.Status())}
		m.Requests.WithLabelValues(labels...).Inc()
		m.Duration.WithLabelValues(labels...).Observe(time.Since(started).Seconds())
	}
}
