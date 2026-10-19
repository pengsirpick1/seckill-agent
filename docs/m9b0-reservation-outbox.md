# M9B-0 — Redis reservation outbox

## 已交付

Gateway 保持不写 MySQL 的秒杀热路径：它先生成 `reservation_id`、`event_id` 和 `order_id`，然后执行一个 Redis Lua 脚本。该脚本在同一原子执行中检查热库存、写入不可变 reservation Hash、写入按活动分片的 Redis Stream，并扣减库存。

```text
Gateway -> Redis Lua reserve + XADD -> reservation Stream
                                      -> outbox-publisher group -> Kafka order.created
                                                                       -> order-consumer -> MySQL
```

每个 activity 使用一个与库存相同 hash tag 的 Stream：`seckill:reservation-outbox:{activity_id}`。这使 Lua 脚本可用于 Redis Cluster；Publisher 通过 `SCAN` 发现 Stream，不需要跨 slot 的全局索引。

`outbox-publisher` 使用 Consumer Group。Kafka 返回错误时不执行 `XACK`；进程在 Kafka ACK 后、XACK 前崩溃时，原 Stream entry 会由 `XAUTOCLAIM` 重投。重投保留原始 `event_id`，MySQL `processed_events`、订单唯一约束和 `order_lifecycle` 仲裁继续承担幂等防线。Publisher 在发送前核对 reservation Hash 的 state 与所有业务 ID；缺失或不一致时保留 Pending，不会构造 Kafka 事件或执行库存回补。

默认 Consumer 名称含容器 hostname，因此同一 Group 中的多个 Publisher 实例可并行工作。显式设置 `RESERVATION_OUTBOX_CONSUMER_NAME` 时，部署者必须保证各实例名称不同。

## 原子性与故障边界

Lua 会在第一次写入前验证所有参数与三个 Redis key 类型。正常写入顺序是 `prepared` Hash、`XADD`、`reserved` Hash、`DECR`。Redis 不会为 Lua 运行时错误回滚已执行命令；此顺序使异常结果可见：不会出现已扣热库存但没有 reservation 元数据的正常路径。遇到 Redis 服务器级 OOM 或 AOF 故障时，调用方必须将结果视为不确定，依赖 Hash/Stream 审计，不能自动补偿。

reservation Hash 在 M9B-0 不设 24 小时 TTL，避免 Kafka 长时间不可用时 Stream 仍在而幂等状态已经过期。已确认 Stream 也不自动裁剪。M9B-1/M9B-3 必须在 MySQL 已记录最终状态和对账确认后，才引入有界清理策略。

Redis Compose 配置启用 AOF 且挂载 data volume，因此正常容器重启可以恢复 AOF 中的 reservation 与 Stream。Redis 整体数据丢失、损坏 AOF、或故障切换到未复制这些写入的副本，仍会同时失去 Redis 预扣事实和 Stream；M9B-0 没有足以自动恢复该情况的 MySQL 预占登记，不能声称 Exactly Once。该边界须由 Redis 持久化/复制监控、人工对账和告警处理。

## MySQL 库存事实

迁移 `005_m9b_outbox_inventory_state.sql` 为 `order_lifecycle` 增加 `persistent_inventory_state`：

- `not_applied`：提前取消或拒绝路径，MySQL 库存没有扣减。
- `applied`：Consumer 在同一 MySQL transaction 内完成订单、活动库存和持久库存扣减后写入。
- `restored`：预留给 M9B-1 的可靠库存释放任务。

旧 Kafka `order.created` 事件没有新增必填字段，Consumer 保持兼容。旧的已有订单迁移为 `applied`；没有 `orders` 行的 lifecycle 记录继续为 `not_applied`。

## 运行与指标

新增 `outbox-publisher` 服务，Prometheus 指标包括：

- `reservation_outbox_published_total`、`reservation_outbox_xack_total`
- `reservation_outbox_publish_failures_total`、`reservation_outbox_blocked_total`
- `reservation_outbox_pending_entries`、`reservation_outbox_stream_entries`
- Gateway 的 `redis_operation_duration_seconds{operation="reserve"}`

`reservation_outbox_stream_entries` 包含保留历史，不能被解释为未确认数量；未确认交付使用 Pending 指标，尚未读取数量使用 `XINFO GROUPS` 的 `lag`。Kafka Consumer lag 仍由 order-consumer 指标和 Kafka Group 查询提供。

## 验收结果

自动化覆盖：Stream 预扣与 reservation ID 绑定、Kafka 失败不 XACK、另一个 Publisher 经 `XAUTOCLAIM` 接管、reservation 状态缺失时阻断发布、Publisher 客户端重连后的 Stream 读取、Hash reservation 的一次性补偿、Consumer 的 `applied/not_applied` 库存事实、Publisher 面对 `NOGROUP` 的恢复，以及冲突 `order_id` 的确定性补偿。

2026-10-09 在 Docker 网络、`redis_lua_only`、100 VU、5 秒以及临时提高限流阈值的条件下，五轮 HTTP 数据为：

| 轮次 | QPS | P50 | P95 | P99 | HTTP 失败 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 7,416.32 | 12.66 ms | 18.61 ms | 23.44 ms | 0% |
| 2 | 7,003.25 | 13.02 ms | 20.11 ms | 25.52 ms | 0% |
| 3 | 6,684.39 | 14.18 ms | 20.60 ms | 28.87 ms | 0% |
| 4 | 6,694.01 | 14.05 ms | 20.84 ms | 29.28 ms | 0% |
| 5 | 6,400.89 | 14.42 ms | 22.37 ms | 37.42 ms | 0% |

QPS 均值为 6,839.77，中位数 6,694.01；P99 均值为 28.91 ms，中位数 28.87 ms。相对于 M9A.1 的两轮均值 7,366.22 QPS 和 29.29 ms P99，QPS 低约 7.2%，P99 基本持平。M9A.1 基线仅两轮，不能据此作统计显著性结论。原始 k6 JSON 位于 `artifacts/m9b/`。

五轮产生 171,956 个 HTTP `202`。最终持久化 171,953 个订单；三条 Kafka 事件与已有订单使用同一 `order_id` 但 reservation/event/user 不同，被 Consumer 判为确定性冲突，重放后以原 event_id 写入 `processed_events` 并幂等回补 Redis，不创建订单、不扣减 MySQL。五个活动最终均满足 `orders = sold`、`available = activity stock = Redis stock`；所有 Stream 的 pending 与 lag 均为 0，Kafka order-consumer lag 为 0。

Redis AOF 容器重启后，抽检 activity 252 的热库存仍为 62,668、其 Stream 长度为 37,332，Consumer Group lag 为 0。该测试证明正常 AOF 重启恢复，不覆盖 Redis 全量数据丢失或失去最近 AOF 写入的故障边界。

运行中曾遇到 Docker 暂停导致 Consumer Kafka group membership 失效。Consumer 现对处理、成功事件发布或 offset 提交错误退出，由 Compose `restart: unless-stopped` 从最后提交 offset 重启，避免后续 offset 间接跳过失败事件。

本次压测还观测到三次相同 `order_id` 的冲突。当前 ID 生成器要求每个 Gateway 实例配置唯一的 `ORDER_ID_NODE_ID`；部署多个 Gateway 前必须满足这个条件。M9B-0 已把冲突转为可恢复的拒绝和库存补偿，避免库存遗失，但不能将发生冲突的 `202` 转化为正常订单。多 Gateway 的节点 ID 分配仍是上线前运维前置条件。
