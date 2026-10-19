package gateway

import "github.com/prometheus/client_golang/prometheus"

type SeckillMetrics struct {
	Requests       *prometheus.CounterVec
	Success        prometheus.Counter
	StockExhausted prometheus.Counter
	StockNotWarmed prometheus.Counter
}

func NewSeckillMetrics(registry prometheus.Registerer) *SeckillMetrics {
	metrics := &SeckillMetrics{
		Requests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "seckill_requests_total", Help: "Total seckill requests by result."}, []string{"result"}),
		Success:        prometheus.NewCounter(prometheus.CounterOpts{Name: "seckill_success_total", Help: "Successful seckill orders."}),
		StockExhausted: prometheus.NewCounter(prometheus.CounterOpts{Name: "seckill_stock_exhausted_total", Help: "Seckill requests rejected because stock is exhausted."}),
		StockNotWarmed: prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_stock_not_warmed_total", Help: "Seckill requests rejected because Redis stock was not warmed."}),
	}
	registry.MustRegister(metrics.Requests, metrics.Success, metrics.StockExhausted, metrics.StockNotWarmed)
	return metrics
}

func (m *SeckillMetrics) ObserveSuccess() {
	m.Requests.WithLabelValues("success").Inc()
	m.Success.Inc()
}

func (m *SeckillMetrics) ObserveFailure(code string) {
	m.Requests.WithLabelValues("error").Inc()
	switch code {
	case "SECKILL_STOCK_EXHAUSTED":
		m.StockExhausted.Inc()
	case "SECKILL_STOCK_NOT_WARMED":
		m.StockNotWarmed.Inc()
	}
}
