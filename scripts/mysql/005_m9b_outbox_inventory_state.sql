-- M9B-0 记录订单 Consumer 是否实际更新了持久化库存。
-- 未来释放任务必须依据这一事实，不能为提前取消的预占盲目增加 MySQL 库存。
ALTER TABLE order_lifecycle
    ADD COLUMN persistent_inventory_state VARCHAR(16) NOT NULL DEFAULT 'not_applied' AFTER reservation_state,
    ADD CONSTRAINT chk_order_lifecycle_persistent_inventory_state CHECK (persistent_inventory_state IN ('not_applied', 'applied', 'restored'));

-- 既有正常 M9A 订单已扣减 MySQL 库存。没有订单的生命周期记录代表提前取消或拒绝形态，
-- 其状态保持 not_applied。
UPDATE order_lifecycle l
JOIN orders o ON o.id = l.order_id
SET l.persistent_inventory_state = 'applied'
WHERE o.status IN ('created', 'succeeded', 'cancelling')
  AND l.persistent_inventory_state = 'not_applied';
