# 一致性设计

## M1 当前实现

M1 的 MySQL 是库存和订单的唯一事实来源：

```text
lock seckill_activities row
-> validate activity / product / time window
-> decrement seckill_activities.stock when stock > 0
-> decrement inventories.available and increment sold when available > 0
-> INSERT orders
-> COMMIT
```

`orders` 上的 `UNIQUE(activity_id, user_id)` 是最终幂等保障。重复插入即使发生在库存更新之后，transaction 也会 rollback，因此活动库存和 `inventories` 不会被错误扣减。

## 后续演进

M2 将把 Redis 用作热路径库存并使用 Lua 扣减与补偿。M4 将使用 Kafka at-least-once consumer，并只在 MySQL transaction 成功后确认 offset。

## M2 历史阶段

`POST /api/v1/admin/activity/:id/warmup` 从 MySQL 读取活动库存和持久可用库存的较小值，通过 Redis `SET NX` 写入 `seckill:stock:{activity_id}:{product_id}`。重复 warmup 只返回已存在值，不会重置正在销售的库存。

Lua 扣减在一个 Redis operation 中读取、判断和 `DECR`。MySQL transaction 未提交时，服务以内部 `reservation_id` 调用 Lua compensation；其 idempotency key 确保同一个补偿至多增加一次库存。Redis Key 缺失将返回 `SECKILL_STOCK_NOT_WARMED`，不会回退到 MySQL 直接售卖。

如果进程在 Redis 成功扣减后异常退出，M2 可能产生少卖但不会造成超卖。该缺口会在 M4 通过可靠事件持久化和 consumer 设计继续收敛。

## M4 历史阶段

Gateway 仅在 Kafka broker ack `order.created` 后返回 `202 accepted`。Producer 失败时会对同一 `reservation_id` 执行 Redis Lua compensation。Consumer 使用 at-least-once：处理 event 后在一个 MySQL transaction 中写入订单、扣减 `seckill_activities` 与 `inventories`、插入 `processed_events`，transaction 提交后才手动确认 Kafka offset。

重复 `event_id` 会被 `processed_events` 拦截；不同 event 的同用户重复订单触发数据库唯一约束，Consumer 补偿其 Redis 预扣库存后记录该 event 已处理。该项目未实现 Kafka transaction 或跨 Kafka/MySQL Exactly Once。
