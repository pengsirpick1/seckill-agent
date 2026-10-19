package order

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Processed         *prometheus.CounterVec
	Failed            prometheus.Counter
	Lag               *prometheus.GaugeVec
	MySQLDuration     prometheus.Histogram
	MySQLErrors       prometheus.Counter
	MySQLStepDuration *prometheus.HistogramVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Processed:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "order_consumer_processed_total", Help: "Order-created events processed after durable transaction handling."}, []string{"result"}),
		Failed:            prometheus.NewCounter(prometheus.CounterOpts{Name: "order_consumer_failed_total", Help: "Order-created events that failed before offset commit."}),
		Lag:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumer_lag", Help: "Fetched Kafka lag estimate by topic and partition."}, []string{"consumer", "topic", "partition"}),
		MySQLDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "mysql_transaction_duration_seconds", Help: "Order MySQL transaction duration in seconds."}),
		MySQLErrors:       prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_transaction_errors_total", Help: "Order MySQL transaction failures."}),
		MySQLStepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "order_consumer_mysql_step_duration_seconds", Help: "Duration of fixed MySQL order-consumer transaction steps.", Buckets: []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}}, []string{"step"}),
	}
	registry.MustRegister(metrics.Processed, metrics.Failed, metrics.Lag, metrics.MySQLDuration, metrics.MySQLErrors, metrics.MySQLStepDuration)
	return metrics
}
