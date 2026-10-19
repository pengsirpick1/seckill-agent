# 性能记录

## M1 — MySQL Baseline

执行日期：2026-10-07。

```bash
docker run --rm --network seckill-agent_backend -v "$PWD/scripts/k6:/scripts:ro" -e VUS=100 -e DURATION=5s grafana/k6:0.56.0 run /scripts/baseline.js
```

工作负载先创建库存 100,000 的独立商品和活动，再以 100 VU 和互不重复的 `user_id` 调用 `POST /api/v1/seckill` 5 秒。

| 指标 | 实测值 |
| --- | --- |
| HTTP requests | 1,306 |
| 成功秒杀 iterations | 1,304 |
| Throughput | 239.73 requests/s |
| P50 | 321.53 ms |
| P95 | 908.17 ms |
| P99 | 1.23 s |
| HTTP failure rate | 0.00% |

压测结束后直接查询 MySQL：`orders_count=1304`、`total=100000`、`available=98696`、`sold=1304`、活动剩余 `stock=98696`，因此 `oversell_count=0`。

该结果仅适用于本机 Docker 环境和 M1 同步 MySQL 实现，不能代表 Redis、Kafka 或 Etcd 的性能。

## M2 — Redis Lua 热库存

执行日期：2026-10-07。

```bash
docker run --rm --network seckill-agent_backend -v "$PWD/scripts/k6:/scripts:ro" -e VUS=100 -e DURATION=5s grafana/k6:0.56.0 run /scripts/redis_lua.js
```

脚本创建库存 100,000 的商品和活动，调用 `POST /api/v1/admin/activity/:id/warmup`，再以 100 VU、唯一 `user_id` 执行 5 秒秒杀。

| 指标 | 实测值 |
| --- | --- |
| HTTP requests | 1,281 |
| 成功秒杀 iterations | 1,278 |
| Throughput | 235.55 requests/s |
| P50 | 324.94 ms |
| P95 | 987.12 ms |
| P99 | 1.34 s |
| HTTP failure rate | 0.00% |

压测后：`orders_count=1278`、MySQL `available=98722`、`sold=1278`、活动 `stock=98722`、Redis `seckill:stock:{11}:11=98722`，因此 `oversell_count=0`。

与 M1 同工作负载的 239.73 requests/s 相比，本次 M2 为 235.55 requests/s；P99 也从 1.23 s 变为 1.34 s。M2 仍同步锁定并更新 MySQL 活动与库存行，Redis 只提前拒绝耗尽请求，不能消除持久层热点锁。该数据不支持“Redis Lua 已提升整体吞吐”的结论。

## M3 — 限流验证

执行命令：

```bash
docker run --rm --network seckill-agent_backend -v "$PWD/scripts/k6:/scripts:ro" -e VUS=100 -e DURATION=5s grafana/k6:0.56.0 run /scripts/limit_test.js
```

默认 `REDIS_LIMIT_PRODUCT_RATE=200`、`REDIS_LIMIT_PRODUCT_BURST=200`。脚本将 `200`、`201` 与限流 `429` 都作为预期响应，并要求 `429` 含有 `Retry-After`。

| 指标 | 实测值 |
| --- | --- |
| HTTP requests | 63,026 |
| Throughput | 11,674.83 requests/s |
| `rate_limited_responses` | 61,824 |
| P50 | 299.43 µs |
| P95 | 818 µs |
| P99 | 308.3 ms |
| HTTP failure rate | 0.00% |
| checks | 100.00%（63,026 / 63,026） |

最终活动有 1,199 笔持久订单，MySQL `sold=1199`、`available=98801`、活动 `stock=98801`。超额请求在 Redis Lua 限流阶段被拒绝，没有消耗库存。该测试的高 Throughput 主要来自快速返回 `429`，不能与 M1/M2 的成功秒杀吞吐直接比较。

## M4 — Kafka 异步订单

执行日期：2026-10-07。

```bash
docker run --rm --network seckill-agent_backend -v "$PWD/scripts/k6:/scripts:ro" -e VUS=100 -e DURATION=5s grafana/k6:0.56.0 run /scripts/kafka_async.js
```

该场景与 M2 一致：脚本创建库存为 `100000` 的独立商品和活动、执行 warmup，并以 100 VU、互不重复的 `user_id` 持续调用 `POST /api/v1/seckill` 5 秒。M3 的默认限流阈值会使该场景主要得到 `429`；因此仅在本次压测期间将 `LOCAL_LIMIT_*` 与 `REDIS_LIMIT_*` 临时设为 `100000`，压测完成后恢复 Compose 默认值。该设置不改变请求链路、Kafka topic 或 consumer 实现。

| 指标 | 实测值 |
| --- | --- |
| HTTP requests | 38,923 |
| HTTP throughput | 7,740.15 requests/s |
| P50 | 12.20 ms |
| P95 | 19.93 ms |
| P99 | 24.96 ms |
| `accepted_202` | 38,920 |
| HTTP failure rate | 0.00%（0 / 38,923） |
| Kafka observed peak lag | 38,361 |
| Kafka lag drain time | 206.24 s |

`Kafka observed peak lag` 通过 consumer group `seckill-order-consumer` 的 `LAG` 列轮询得到。k6 输出完成时间为 `2026-10-07T20:41:32.770+08:00`，首次观测到 `lag=0` 的时间为 `2026-10-07T20:44:59.006+08:00`，故 drain time 为 206.24 秒。

在 `lag=0` 后，压测活动（`activity_id=30`、`product_id=30`）的核对结果为：`orders_count=38920`、`total=100000`、MySQL `sold=38920`、`available=61080`、活动 `stock=61080`、Redis `seckill:stock:{30}:30=61080`。`orders_count=sold`、`total=available+sold`、活动 `stock=available`、Redis stock=`available`，因此 `oversell_count=0`。

### M2 / M4 对比

| 指标 | M2 — Redis Lua 热库存 | M4 — Kafka 异步订单 |
| --- | ---: | ---: |
| 工作负载 | 100 VU，5 秒，100,000 库存 | 100 VU，5 秒，100,000 库存 |
| HTTP throughput | 235.55 requests/s | 7,740.15 requests/s |
| P50 / P95 / P99 | 324.94 ms / 987.12 ms / 1.34 s | 12.20 ms / 19.93 ms / 24.96 ms |
| 接受请求 | 1,278 个同步 `201` | 38,920 个 `202` |
| HTTP failure rate | 0.00% | 0.00% |
| Kafka peak lag | 不适用 | 38,361 |
| lag drain time | 不适用 | 206.24 s |
| lag 清零后的订单数 | 1,278 | 38,920 |
| `oversell_count` | 0 | 0 |

两者的入口响应语义不同：M2 的 `201` 表示 MySQL transaction 已完成；M4 的 `202` 仅表示订单事件已获 Kafka broker ack，订单由 consumer 异步落库。因此，M4 的 HTTP throughput 和延迟不能解释为同步订单完成吞吐的提升；M4 的最终正确性只能以 `lag=0` 后的订单与库存核对为准。本次核对通过，但 206.24 秒的 drain time 也表明 consumer 持久化链路是需要单独观察的吞吐阶段。

## M5 — Etcd Lock A/B

执行日期：2026-10-07。两个场景都使用 100 VU、5 秒、100,000 库存/活动、唯一 `user_id`、Kafka 异步订单链路。为避免 M3 限流掩盖锁等待，压测期间临时将 `LOCAL_LIMIT_*` 和 `REDIS_LIMIT_*` 设为 `100000`；完成后恢复 Compose 默认限流值。

`redis_lua_only` 不获取 Etcd mutex；`etcd` 在 Redis Lua 扣库存及 Kafka broker ack 的外围获取 `seckill:lock:product:<product_id>`。`etcd_lock_wait_duration_seconds` 的分位数由该 histogram 的 bucket 插值得出，属于近似值。

### 单热点商品

| 指标 | `redis_lua_only` | `etcd` |
| --- | ---: | ---: |
| HTTP throughput | 8,891.27 requests/s | 97.54 requests/s |
| P50 | 10.44 ms | 1.01 s |
| P95 | 16.01 ms | 1.02 s |
| P99 | 22.10 ms | 1.05 s |
| `accepted_202` | 44,763 | 581 |
| HTTP failure rate | 0.00% | 0.00% |
| Kafka observed peak lag | 39,193 | 2 |
| Etcd lock wait P50 / P95 / P99 | 不适用 | 1.01 s / 1.05 s / 1.05 s |
| `etcd_lock_acquire_total` / `etcd_lock_failure_total` | 不适用 | 581 / 0 |
| `oversell_count` | 0 | 0 |

Lua-only 样本在 `lag=0` 后为 `orders=sold=44763`、`available=activity stock=Redis stock=55237`。Etcd 样本的 consumer 始终能跟上入口，采样期内 observed peak lag 仅为 2。

### 20 商品分散流量

请求按轮转方式分配给 20 个独立商品和活动，每个商品都有自己的 Etcd mutex。

| 指标 | `redis_lua_only` | `etcd` |
| --- | ---: | ---: |
| HTTP throughput | 9,035.65 requests/s | 438.19 requests/s |
| P50 | 10.11 ms | 218.93 ms |
| P95 | 15.67 ms | 286.36 ms |
| P99 | 20.16 ms | 309.78 ms |
| `accepted_202` | 46,862 | 2,296 |
| HTTP failure rate | 0.00% | 0.00% |
| Kafka observed peak lag | 40,797 | 1,433 |
| Etcd lock wait P50 / P95 / P99 | 不适用 | 190.8 ms / 259.5 ms / 274.7 ms |
| `etcd_lock_acquire_total` / `etcd_lock_failure_total` | 不适用 | 2,296 / 0 |
| `oversell_count` | 0 | 0 |

Lua-only 场景在 `lag=0` 后，20 个活动累计 `orders=sold=46862`、`available=activity stock=1953138`，没有不一致活动。Etcd 场景在 `lag=0` 后，最新 20 个活动累计 `orders=sold=2296`、`available=activity stock=1997704`，20 个 Redis stock 都等于对应 MySQL `available`，`oversell_count=0`。

### 结论

对当前秒杀库存场景，Redis Lua 已覆盖库存检查和扣减的原子性要求，因此在热路径额外增加商品级 Etcd 分布式锁属于重复串行化；在高竞争场景下锁等待显著放大尾延迟。当前实现每次请求创建独立 Etcd lease/session，因此上述 Etcd 数据同时包含 session/lease 建立成本和锁竞争成本；它描述的是当前商品锁实现的端到端表现，不能代表 Etcd 的理论性能。

在本机单实例 Etcd 和单个 Gateway 的真实 A/B 中，Etcd 锁没有增加 Redis Lua 已有的库存正确性：四组场景都没有超卖。它显著降低了入口接入能力：单热点从 8,891.27 降至 97.54 requests/s，20 商品从 9,035.65 降至 438.19 requests/s。多商品把锁等待 P95 从约 1.05 s 降至约 259.5 ms，说明锁按商品隔离确实允许并行；但 session/lease 与 mutex 协调成本仍远高于 Redis Lua 原子扣库存。

因此，当前默认模式继续使用 `SECKILL_LOCK_MODE=redis_lua_only`。`etcd` 保留为可验证的实验模式，而不是热点秒杀路径的默认保护手段。M5 没有修改 Kafka consumer 的 `max_in_flight=1`，也没有进行额外性能优化。
