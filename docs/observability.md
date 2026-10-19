# M6 可观测性

Prometheus 抓取 Gateway 的 :8080/metrics、order-consumer:8082/metrics 与 rank-consumer:8083/metrics。重点指标包括：

- http_requests_total、http_request_duration_seconds、seckill_success_total
- redis_operation_duration_seconds、redis_compensation_total
- kafka_producer_requests_total、kafka_producer_duration_seconds
- order_consumer_processed_total、order_consumer_failed_total、kafka_consumer_lag
- rank_consumer_processed_total、rank_consumer_failed_total
- etcd_lock_wait_duration_seconds、etcd_lock_failure_total

Grafana 自动加载 **Seckill M6 Overview**。其中“接入速率、Kafka lag 与持久化速率”面板用于直接比较 Gateway accepted rate、kafka_consumer_lag 与 order_consumer_processed_total 的 rate()。

Gateway 提取或创建 W3C traceparent，Kafka producer 以其 producer span 注入消息 header；两个 Consumer 提取该 header 继续创建 consumer span。请使用 HTTP 响应的 X-Trace-ID 在 Jaeger 查询完整请求。JSON logs 的 request_id 与 trace_id 可与 trace 相互关联。
