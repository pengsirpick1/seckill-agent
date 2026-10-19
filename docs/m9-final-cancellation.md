# M9-Final — 取消与幂等库存回补

## 实现范围

取消路径不改变秒杀热链路：

```text
Gateway -> Redis reserve + Stream -> Publisher -> Kafka -> Consumer -> MySQL
                                                              |
POST cancel -> order_lifecycle(cancelling) + release task ----+
                                                              v
                                                     release-worker -> Redis Lua
                                                                    -> MySQL complete
```

`POST /api/v1/orders/:order_id/cancel` 仅在 `APP_ENV=development` 且
`ORDER_CANCEL_API_ENABLED=true` 时注册。请求用 `X-Development-User-ID` 表示
测试身份，不把请求体的 `user_id` 当作身份，也不把该机制描述为生产鉴权。默认
未注册该路由。

异步下单响应保留原有数值 `order_id`，并增加无精度损失的
`order_id_string`。JavaScript 客户端应使用字符串字段进行查询、支付或取消，
因为 Snowflake ID 超过 JavaScript 的安全整数范围。

取消请求以 `order_lifecycle.order_id` 的 `INSERT ... ON DUPLICATE KEY` 加
`SELECT ... FOR UPDATE` 仲裁：`created -> cancelling` 和
`created -> succeeded` 只有一个能提交。重复取消返回 `cancelling` 或
`cancelled`；`succeeded` 和 `rejected` 返回状态冲突。取消提交后返回 `202`，
订单查询显示 `cancelling`；Worker 完成后才变为 `cancelled`。

## 任务与库存事实

迁移 `006_m9_final_release_tasks.sql` 建立 `inventory_release_tasks`。它按
`reservation_id` 唯一，并持有稳定 `release_id`、状态、尝试次数、下一次时间、
租约和最后错误。任务和 `created -> cancelling` 在同一个 MySQL transaction
中写入。提前取消先写只有 `order_id/user_id` 的 lifecycle intent；Consumer
补全 Redis/Kafka 的 immutable facts 后，在写 `processed_events` 的同一事务中
创建任务，所以 Kafka offset 提交后仍有持久化恢复责任。

- `persistent_inventory_state=applied`：Consumer 已扣 MySQL；Worker 回补
  Redis、`seckill_activities.stock`、`inventories.available/sold`，再写
  `restored` 与 `cancelled`。
- `not_applied`：提前取消胜出；Worker 只回补 Redis，再写
  `reservation_state=released` 与 `cancelled`，不伪造 MySQL 回补。

Redis Lua 只有在 reservation Hash 的 `reservation_id/order_id/activity_id/
product_id/quantity` 都匹配且 `state=reserved` 时才 `INCRBY`。它在同一脚本
写入 `state=released` 与 `release_id`。同一 `release_id` 重试返回幂等成功；
不同 release ID、缺 Hash、缺 stock key 或事实冲突均安全失败，任务保留并带
退避重试，绝不直接加库存。

Worker 使用 `FOR UPDATE SKIP LOCKED` 领取 pending 或租约到期的任务，领取时
写 `lease_token/lease_until`。完成 transaction 再次核对 token，过期 Worker
不能提交旧结果。进程在 Redis 成功与 MySQL 完成之间崩溃时，重试会以相同
`release_id` 看到 Redis 已释放，只完成 MySQL，不会二次 `INCRBY`。

## 已知边界

Redis 与 MySQL 没有跨库事务，因此这不是 Exactly Once。Redis AOF/副本整体
丢失而 reservation Hash 不存在时，Worker 会留下 pending 任务和错误，等待
人工对账；它不会猜测库存并回补。当前库存链路只处理带 immutable Hash 的
Reservation，避免将缺失或不完整的 Redis 状态误认为可安全回补。

`inventory_release_tasks_pending`、`inventory_release_duration_seconds`、
`inventory_release_retry_total` 和 `inventory_release_failed_total` 由
`release-worker` 的 `:8085/metrics` 暴露。

## 自动化证据

`TestM9Final*` 覆盖：双账本取消回补与重复取消、支付取消竞争、提前取消的
`not_applied` 路径、Redis 成功后 MySQL 完成失败并重试、过期租约下两个
Worker 的互斥、reservation 丢失时安全失败。完整 Kafka/Stream 回归仍由已有
M4、M9A、M9B-0 集成测试覆盖。

## 本地性能记录

2026-10-10，Docker 网络、`redis_lua_only`、临时提高限流阈值、100 VU、5 秒
的正常异步下单得到 7,229.22 req/s，P50 13.29 ms、P95 17.47 ms、P99 21.80 ms，
HTTP failure 为 0%。这与 M9B-0 的五轮均值 6,839.77 req/s、P99 均值 28.91 ms
相比，不显示热路径回归；该结果是一轮回归样本，不代表统计显著性。

取消场景建立 5 份库存，等待 5 个订单取消并由 Worker 回补，再让 100 VU 各
抢购一次：`released_stock_accepted=5`，HTTP failure 为 0%，说明每份释放库存
只被重新预占一次。原始 k6 JSON 位于 `artifacts/m9-final/`。
