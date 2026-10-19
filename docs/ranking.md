# M6 排行榜

Redis Key 为 seckill:ranking:{activity_id}，member 是 order_id，score 是订单成功事件的 created_at 毫秒时间戳。因为 ZSet member 唯一，重复 order.succeeded 不会产生重复排行记录。

事件只在 order-consumer 完成 MySQL transaction 后派生，因此排行榜的业务语义是“最终持久化成功订单”，不是 Gateway 已返回的 202 accepted 数量。排行榜消费者独立运行，故 Redis ZSet 写入不在 HTTP 请求主链路内。

当前可靠性语义为 at-least-once：MySQL 已提交但 order.succeeded 还未发送的窗口，可能靠重放 order.created 重新派生事件；processed_events 重放时会核对真实订单是否存在。未引入 Transactional Outbox，因此不能称为 Exactly Once。
