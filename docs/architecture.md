# 架构

当前运行五个 Go 数据面进程：gateway、outbox-publisher、order-consumer、rank-consumer 与 release-worker。

Client -> gateway -> local Token Bucket -> Redis Lua limiter -> optional Etcd product mutex -> Redis Lua stock deduct -> Kafka seckill.order-created.v1 -> order-consumer (max_in_flight=1) -> MySQL orders + inventories + seckill_activities + processed_events -> Kafka seckill.order-succeeded.v1 -> rank-consumer -> Redis ZSet seckill:ranking:{activity_id}

取消不进入 Gateway 秒杀热路径：它把 lifecycle 置为 `cancelling` 并在同一 MySQL transaction 写 `inventory_release_tasks`；独立 `release-worker` 以 Redis Lua 幂等回补后，按 `persistent_inventory_state` 完成 MySQL 回补并写 `cancelled`。详见 [M9-Final](m9-final-cancellation.md)。

排行榜只反映最终持久化成功订单，不会阻塞 HTTP 202 accepted。两个 Kafka Consumer 都是手动 offset commit；order-consumer 只有在 MySQL 处理及必要的成功事件派生完成后才提交原始 offset。

gateway、order-consumer 和 rank-consumer 都输出 Prometheus metrics 与 JSON logs。OpenTelemetry 通过 Kafka header 中的 W3C traceparent 将 HTTP、Kafka、MySQL 与 Redis span 串联到 Jaeger。Grafana 的 **Seckill M6 Overview** 用于在同一时间轴上观察接入速率、Consumer throughput 与 Kafka lag。

