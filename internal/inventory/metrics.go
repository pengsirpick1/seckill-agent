package inventory

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	OperationDuration    *prometheus.HistogramVec
	Compensations        prometheus.Counter
	CompensationFailures prometheus.Counter
	OutboxReservations   prometheus.Counter
	OutboxFailures       prometheus.Counter
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		OperationDuration:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "redis_operation_duration_seconds", Help: "Redis operation latency in seconds."}, []string{"operation", "result"}),
		Compensations:        prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_compensation_total", Help: "Successful idempotent Redis stock compensations."}),
		CompensationFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_compensation_failures_total", Help: "Redis stock compensation failures."}),
		OutboxReservations:   prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_reservation_outbox_total", Help: "Reservations atomically recorded in the Redis Stream outbox."}),
		OutboxFailures:       prometheus.NewCounter(prometheus.CounterOpts{Name: "redis_reservation_outbox_failures_total", Help: "Redis reservation outbox writes that did not complete."}),
	}
	registry.MustRegister(metrics.OperationDuration, metrics.Compensations, metrics.CompensationFailures, metrics.OutboxReservations, metrics.OutboxFailures)
	return metrics
}
