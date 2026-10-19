# Seckill Agent

这是一个用于学习和验证高并发秒杀交易系统的 Go 项目。项目从 MySQL 正确性基线逐步演进到 Redis Lua 热库存、Kafka 异步落库、订单取消回补、Etcd A/B 实验与可观测性。

## 当前状态

当前默认业务主链路是 **限流 + Redis Lua 热库存及 reservation Stream outbox + outbox-publisher + Kafka + order-consumer + MySQL**；订单持久化成功后派生 order.succeeded，由独立 rank-consumer 更新 Redis ZSet 排行榜。Etcd Lock 保持为可切换的 A/B 实验模式，而不是默认热点路径。Prometheus、Grafana、OpenTelemetry 与 Jaeger 已接入。真实状态见 [STATUS.md](STATUS.md)。

## 当前真实架构

```text
Client -> Gateway (Gin) -> local Token Bucket
                         -> Redis Lua limiter (Global / Product / User)
                         -> Redis Lua reserve + Stream XADD
                         -> outbox-publisher -> Kafka order.created
                         -> order-consumer -> MySQL transaction
                                              -> inventories / orders / processed_events
                                               -> Kafka order.succeeded
                                                  -> rank-consumer -> Redis ZSet

Gateway / order-consumer / rank-consumer
  -> Prometheus metrics + structured JSON logs + OpenTelemetry traces -> Jaeger
```

## Quick Start

需要 Docker Desktop：

```bash
make up
make migrate
make test
make test-race
make test-integration
```

当前 API：`POST /api/v1/admin/product`、`POST /api/v1/admin/activity`、`POST /api/v1/seckill`、`GET /api/v1/orders/:id`、`GET /api/v1/users/:user_id/orders`、`GET /healthz`、`GET /readyz`、`GET /metrics`。

M9-Final 新增可恢复取消回补，说明见 [M9-Final 取消设计](docs/m9-final-cancellation.md)。取消 API 默认关闭；仅在本地开发环境明确设置 `ORDER_CANCEL_API_ENABLED=true` 后注册，并使用 `X-Development-User-ID` 作为测试身份，不能用于生产鉴权。

## M1 一致性

`POST /api/v1/admin/activity/:id/warmup` 用 `SET NX` 将 MySQL 可售库存写入 Redis。每次 `POST /api/v1/seckill` 先经 Redis Lua 原子扣减，再执行 MySQL transaction。后续 transaction 失败时，幂等 Lua compensation 只会回补一次。`UNIQUE(activity_id, user_id)` 是最终订单幂等防线。

## M4 Kafka 异步订单

秒杀请求在 Kafka broker 确认 `order.created` 后返回 `202 accepted` 和 `order_id`。`order-consumer` 使用手动 offset commit：只有 MySQL transaction 写入订单、持久库存和 `processed_events` 成功后才提交 offset。重复 `event_id` 不会重复写单；同用户重复订单会通过 Redis Lua compensation 回补一次热库存。

## M3 限流

每个请求先经过本地 Token Bucket，再由 Redis Lua 按 Global、Product、User 顺序检查共享配额。任一层拒绝时返回 `429`、`RATE_LIMITED` 和 `Retry-After`，不会进入 Redis 库存或 MySQL。默认限额由 `LOCAL_LIMIT_*` 与 `REDIS_LIMIT_*` 环境变量配置。

## M1 基线压测

M1 与 M2 均在 Docker 网络内以 100 VU、5 秒执行真实压测。M2 Redis Lua 路径的 Throughput 为 235.55 requests/s，P50/P95/P99 为 324.94 ms / 987.12 ms / 1.34 s，HTTP failure rate 为 0.00%。由于 M2 仍同步执行 MySQL transaction，当前测试不能证明 Redis 路径带来吞吐提升；详细对比见 [docs/performance.md](docs/performance.md)。

完整数据见 [docs/performance.md](docs/performance.md)。

## M6 排行榜与可观测性

排行榜只表示最终 MySQL 持久化成功的订单：order-consumer transaction 成功后发布 order.succeeded，rank-consumer 使用 ZADD seckill:ranking:{activity_id} 写入 order_id。重复消息不会重复增加 member。该链路是 at-least-once + 幂等处理，不是 Exactly Once，也尚未采用 Transactional Outbox。

/metrics 由 Gateway、order-consumer:8082 和 rank-consumer:8083 暴露；Prometheus 自动抓取，Grafana 预置 **Seckill M6 Overview**。HTTP request 的 W3C traceparent 会透传到 Kafka header，并在 Jaeger 串联 HTTP、Redis、Kafka、MySQL 与排行榜 Redis span。结构化日志包含 request_id、trace_id。

## M7 故障注入

仅 `APP_ENV=development` 时可用 `FAULT_REDIS_DELAY_MS`、`FAULT_DB_DELAY_MS`、`FAULT_CONSUMER_DELAY_MS`、`FAULT_ETCD_DELAY_MS` 注入依赖延迟；其他环境会忽略这些故障参数。受控演练脚本为 `scripts/k6/m7_fault.js`。
