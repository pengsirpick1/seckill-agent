package limiter

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Rejected *prometheus.CounterVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{Rejected: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rate_limit_rejected_total", Help: "Requests rejected by the local or distributed rate limiter."}, []string{"layer", "scope"})}
	registry.MustRegister(metrics.Rejected)
	return metrics
}
