package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pengtor/seckill-agent/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Producer interface {
	ProduceOrderCreated(context.Context, OrderCreatedEvent) error
	ProduceOrderSucceeded(context.Context, OrderSucceededEvent) error
	Close()
}

type Metrics struct {
	Requests prometheus.Counter
	Failures prometheus.Counter
	Duration *prometheus.HistogramVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		Requests: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafka_producer_requests_total", Help: "Kafka producer requests acknowledged by the broker."}),
		Failures: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafka_producer_failures_total", Help: "Kafka producer failures after retries."}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "kafka_producer_duration_seconds", Help: "Kafka producer broker acknowledgement latency."}, []string{"event_type", "result"}),
	}
	registry.MustRegister(metrics.Requests, metrics.Failures, metrics.Duration)
	return metrics
}

type FranzProducer struct {
	client         *kgo.Client
	orderTopic     string
	succeededTopic string
	metrics        *Metrics
}

func NewProducer(cfg config.KafkaConfig, metrics *Metrics) (*FranzProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(3),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &FranzProducer{client: client, orderTopic: cfg.OrderTopic, succeededTopic: cfg.OrderSucceededTopic, metrics: metrics}, nil
}

func (p *FranzProducer) ProduceOrderCreated(ctx context.Context, event OrderCreatedEvent) error {
	return p.produce(ctx, p.orderTopic, event.EventType, event.ActivityID, event.TraceParent, event)
}

func (p *FranzProducer) ProduceOrderSucceeded(ctx context.Context, event OrderSucceededEvent) error {
	return p.produce(ctx, p.succeededTopic, event.EventType, event.ActivityID, event.TraceParent, event)
}

func (p *FranzProducer) produce(ctx context.Context, topic, eventType string, activityID int64, traceParent string, event any) error {
	ctx, span := otel.Tracer("kafka-producer").Start(ctx, "kafka.produce", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(attribute.String("messaging.destination.name", topic), attribute.String("messaging.operation.name", "publish"), attribute.String("messaging.message.type", eventType))
	started := time.Now()
	payload, err := json.Marshal(event)
	if err != nil {
		p.metrics.Duration.WithLabelValues(eventType, "failure").Observe(time.Since(started).Seconds())
		span.RecordError(err)
		return fmt.Errorf("marshal order event: %w", err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if propagated := carrier.Get("traceparent"); propagated != "" {
		traceParent = propagated
	}
	headers := []kgo.RecordHeader{}
	if traceParent != "" {
		headers = append(headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(traceParent)})
	}
	result := p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(strconv.FormatInt(activityID, 10)), Value: payload, Headers: headers, Timestamp: time.Now().UTC()})
	if err := result.FirstErr(); err != nil {
		p.metrics.Failures.Inc()
		p.metrics.Duration.WithLabelValues(eventType, "failure").Observe(time.Since(started).Seconds())
		span.RecordError(err)
		return fmt.Errorf("produce order event: %w", err)
	}
	p.metrics.Requests.Inc()
	p.metrics.Duration.WithLabelValues(eventType, "success").Observe(time.Since(started).Seconds())
	return nil
}

func (p *FranzProducer) Close() { p.client.Close() }
