# k6 workloads

`baseline.js` 是 M1 的真实 MySQL 基线压测；`redis_lua.js` 是 M2 的 Redis Lua 热库存压测；`limit_test.js` 验证 M3 的 `429` 和 `Retry-After`。三者均通过管理 API 创建独立商品和活动，并以唯一 `user_id` 发起请求。`redis_lua.js` 与 `limit_test.js` 会在请求前调用 `POST /api/v1/admin/activity/:id/warmup`。

`kafka_async.js` 使用与 M2 相同的商品、活动、唯一用户和 100 VU 工作负载，但断言 `202 accepted`。运行时需要同时记录 Kafka consumer group lag，并在 lag drain 完成后执行 MySQL/Redis 一致性检查。

`etcd_hot_product.js` 与 `etcd_multi_product.js` 是 M5 的 A/B 工作负载：前者的所有请求竞争同一商品锁，后者把请求按轮转方式均匀分配到 20 个商品锁。两个脚本记录 `accepted_202` 与 `etcd_lock_acquire_failures`；`503/ETCD_LOCK_ACQUIRE_FAILED` 保持为 HTTP failure，以避免掩盖锁竞争下的失败率。每一种工作负载必须分别在 `SECKILL_LOCK_MODE=redis_lua_only` 与 `SECKILL_LOCK_MODE=etcd` 下执行；压测期间将限流阈值临时调高，结束后恢复默认值，并等待 Kafka lag 清零后再核对订单和库存。

示例：

```bash
docker run --rm --network seckill-agent_backend -v "$PWD/scripts/k6:/scripts:ro" grafana/k6 run /scripts/baseline.js
```

可使用 `VUS`、`DURATION` 与 `BASE_URL` 覆盖脚本默认负载；Redis Lua、Kafka、限流与 Etcd 多商品场景分别见对应脚本。

`m9_cancel_repurchase.js` 建立 5 份库存，完成 5 个异步下单、开发测试取消和 Worker 回补，再由 100 个 VU 各发起一次抢购。它要求 Gateway 以 `APP_ENV=development` 和 `ORDER_CANCEL_API_ENABLED=true` 启动；脚本只把 `202` 或库存耗尽 `409` 视为重新抢购的预期结果，并记录 `released_stock_accepted`。
