-- M9-Final 的释放任务独立于订单状态持久化。唯一 reservation 键用于防止拒绝恢复、
-- 提前取消和正常取消三条路径重复释放。
CREATE TABLE IF NOT EXISTS inventory_release_tasks (
    release_id VARCHAR(160) NOT NULL,
    reservation_id VARCHAR(128) NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    activity_id BIGINT UNSIGNED NOT NULL,
    product_id BIGINT UNSIGNED NOT NULL,
    quantity INT NOT NULL,
    restore_mysql BOOLEAN NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'pending',
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    lease_token VARCHAR(128) NULL,
    lease_until DATETIME(6) NULL,
    last_error VARCHAR(1024) NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (release_id),
    UNIQUE KEY uk_inventory_release_reservation (reservation_id),
    KEY idx_inventory_release_ready (status, next_attempt_at),
    KEY idx_inventory_release_lease (status, lease_until),
    CONSTRAINT chk_inventory_release_status CHECK (status IN ('pending', 'processing', 'completed')),
    CONSTRAINT chk_inventory_release_quantity CHECK (quantity > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
