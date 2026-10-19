package lock

import "github.com/prometheus/client_golang/prometheus"

// Metrics 描述可选 Etcd 互斥锁的获取行为。
type Metrics struct {
	WaitDuration *prometheus.HistogramVec
	Acquires     prometheus.Counter
	Failures     prometheus.Counter
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		WaitDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "etcd_lock_wait_duration_seconds",
			Help:    "Time spent waiting to acquire an Etcd product mutex.",
			Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.225, 0.25, 0.275, 0.3, 0.325, 0.35, 0.375, 0.4, 0.45, 0.5, 0.75, 0.9, 1, 1.05, 1.1, 1.25, 1.5, 2, 5},
		}, []string{"result"}),
		Acquires: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etcd_lock_acquire_total",
			Help: "Total successful Etcd mutex acquisitions.",
		}),
		Failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "etcd_lock_failure_total",
			Help: "Total failed Etcd mutex acquisitions.",
		}),
	}
	registry.MustRegister(metrics.WaitDuration, metrics.Acquires, metrics.Failures)
	return metrics
}
