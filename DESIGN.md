# 设计：Go 高并发秒杀交易系统

## 核心请求链路

```text
Client
  -> Gateway：参数校验、本地令牌桶、Redis 分布式限流
  -> Redis Lua：库存预扣、Reservation 记录、Stream XADD
  -> Outbox Publisher：Consumer Group 读取 Stream，Kafka 确认后 XACK
  -> Kafka order.created
  -> Order Consumer：MySQL 事务、event_id 去重、订单与持久库存落库
  -> Kafka order.succeeded
  -> Rank Consumer：Redis ZSet 排行榜
```

Gateway 默认使用 `redis_lua_only`。Etcd Mutex 只保留为 A/B 对比模式，不属于热点商品的默认路径。

## 取消与回补

订单从 `created` 通过 MySQL 条件更新进入 `cancelling`，同时创建持久化 `inventory_release_tasks`。Release Worker 使用任务租约领取工作；Redis Lua 以 `release_id` 幂等回补 Redis 库存。只有 `persistent_inventory_state=applied` 的订单才回补 MySQL 库存，随后订单状态变为 `cancelled`。

该系统的消息语义是至少一次投递：Stream Pending 恢复、Kafka 重投、`processed_events.event_id` 和数据库唯一约束共同防止重复订单与重复库存扣减，不宣称跨 Redis、Kafka、MySQL 的 Exactly Once。

## 可观测性与故障演练

Gateway 和后台进程导出 Prometheus 指标、JSON 日志与 OpenTelemetry Trace；Grafana 用于指标查看，Jaeger 用于链路追踪。开发环境可用 `FAULT_REDIS_DELAY_MS`、`FAULT_DB_DELAY_MS`、`FAULT_CONSUMER_DELAY_MS`、`FAULT_ETCD_DELAY_MS` 注入可控延迟。告警 Agent、RCA 和 Incident Correlation 不属于当前工程。
