-- M9A 保持异步下单链路不变。生命周期记录首先由订单 Consumer 写入，测试专用的提前取消路径除外。
ALTER TABLE orders
    ADD COLUMN reservation_id VARCHAR(128) NULL AFTER id,
    ADD UNIQUE KEY uk_orders_reservation (reservation_id),
    DROP CHECK chk_orders_status,
    ADD CONSTRAINT chk_orders_status CHECK (status IN ('created', 'succeeded', 'cancelling', 'cancelled'));

-- 以 order_id 为键的记录对订单创建和 Kafka Consumer 插入订单前到达的取消意图进行仲裁。
CREATE TABLE IF NOT EXISTS order_lifecycle (
    order_id BIGINT UNSIGNED NOT NULL,
    reservation_id VARCHAR(128) NULL,
    event_id VARCHAR(128) NULL,
    user_id BIGINT NOT NULL,
    product_id BIGINT UNSIGNED NULL,
    activity_id BIGINT UNSIGNED NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id),
    UNIQUE KEY uk_order_lifecycle_reservation (reservation_id),
    UNIQUE KEY uk_order_lifecycle_event (event_id),
    KEY idx_order_lifecycle_status_updated (status, updated_at),
    CONSTRAINT chk_order_lifecycle_status CHECK (status IN ('created', 'succeeded', 'cancelling', 'cancelled', 'rejected'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
