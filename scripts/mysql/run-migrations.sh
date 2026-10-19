#!/bin/sh
# 每项结构变更只执行一次。只有对应 SQL 文件成功后才记录版本，
# 因此迁移失败后可以安全重试。
set -eu

mysql_cmd() {
  mysql -h mysql -u seckill seckill "$@"
}

mysql_cmd -e '
  CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(128) NOT NULL,
    applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (version)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;'

apply_migration() {
  version="$1"
  file="$2"

  if mysql_cmd -N -e "SELECT 1 FROM schema_migrations WHERE version = '${version}'" | grep -qx '1'; then
    echo "migration ${version} already applied"
    return
  fi

  echo "applying migration ${version}"
  mysql_cmd < "$file"
  mysql_cmd -e "INSERT INTO schema_migrations (version) VALUES ('${version}')"
}

apply_migration 002_m1_schema /migrations/002_m1_schema.sql
apply_migration 003_m9a_order_lifecycle /migrations/003_m9a_order_lifecycle.sql
apply_migration 004_m9a_lifecycle_reservation /migrations/004_m9a_lifecycle_reservation.sql
apply_migration 005_m9b_outbox_inventory_state /migrations/005_m9b_outbox_inventory_state.sql
apply_migration 006_m9_final_release_tasks /migrations/006_m9_final_release_tasks.sql
