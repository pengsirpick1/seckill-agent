-- 取消意图在订单记录创建前胜出时，生命周期记录是持久化恢复点。
-- Consumer 在记录 Kafka 事件已处理的同一事务中补全这些字段。
ALTER TABLE order_lifecycle
    ADD COLUMN quantity INT NULL AFTER activity_id,
    ADD COLUMN reservation_state VARCHAR(16) NULL AFTER status,
    ADD CONSTRAINT chk_order_lifecycle_reservation_state CHECK (reservation_state IN ('reserved', 'released') OR reservation_state IS NULL);
