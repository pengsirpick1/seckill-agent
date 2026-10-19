# 交付状态

> 当前代码已移除 Alert Agent、Alertmanager、Incident Correlation 和 RCA 模块；下方 M8 章节仅保留为历史记录，不代表当前可运行功能。

## M0 — 基础设施与项目骨架

**Status: Complete**

已完成 Go Module、配置、JSON 日志、Request ID、统一错误模型、健康检查、优雅停机以及 MySQL、Redis、Kafka、Etcd、Prometheus、Grafana、Alertmanager、Jaeger、gateway、alert-agent 的 Docker Compose 部署。已通过容器构建、`go test ./...`、`go test -race ./...` 和完整 Compose 健康检查。

## M1 — MySQL Baseline 秒杀

**Status: Complete**

### 已实现

- `products`、`inventories`、`seckill_activities`、`orders`、`processed_events` 的版本化 schema。
- 商品、活动、同步秒杀、单订单和用户订单 API。
- `database/sql` + `sqlx` 连接池与 MySQL transaction。
- 活动库存与持久库存扣减、订单唯一索引和 rollback。
- 真实 MySQL 并发集成测试与 k6 基线脚本。

### 已执行测试

- `go test ./...`：通过。
- `go test -race ./...`：通过。
- `go test -tags=integration -race ./tests/integration`：通过。
- 100 并发用户争抢 50 库存：成功 50、库存耗尽 50、异常 0。
- 同一用户双并发：仅成功 1 笔，另一请求返回 `ORDER_ALREADY_EXISTS`，库存 rollback 正确。
- k6 100 VU、5 秒：详见 `docs/performance.md`。

### 已知限制

当前是单热点活动行锁下的同步 MySQL 基线，不是最终吞吐架构。订单 ID 目前为 MySQL `AUTO_INCREMENT`；Redis Lua、限流、Kafka、Etcd Lock、排行榜、业务 tracing、故障注入和 Agent RCA 均未实现。

### 下一步

等待用户确认后进入 M3：进程内 Token Bucket 与 Redis Lua distributed rate limiter。

## M2 — Redis Lua 热库存

**Status: Complete**

### 已实现

- Redis Key Builder、`warmup` API、`SET NX` 防覆盖预热。
- Redis Lua 原子扣库存，明确区分成功、耗尽与未预热。
- MySQL transaction 失败后的幂等 Lua compensation。
- Redis operation、compensation、秒杀成功、库存耗尽和未预热 Prometheus metrics。
- Redis + MySQL 真实并发集成测试及 k6 脚本。

### 已执行测试

- `go test ./...`：通过。
- `go test -tags=integration -race ./tests/integration`：通过。
- Redis Lua：100 并发请求、库存 50，成功 50、耗尽 50、异常 0。
- 重复下单：数据库返回 `ORDER_ALREADY_EXISTS` 后 Redis 只补偿一次。
- 未预热活动：返回 `SECKILL_STOCK_NOT_WARMED`，没有创建订单或扣减 MySQL 库存。
- k6 100 VU、5 秒：详见 `docs/performance.md`。

### 已知限制

- M2 在 Redis 成功后仍同步执行 MySQL transaction，因此热点 MySQL 活动行仍是主要瓶颈。
- Redis 扣减后进程异常退出且未触发 compensation 时可能少卖；当前采取失败关闭，不允许运行中覆盖 warm stock。可靠异步持久化在 M4 处理。

### 下一步

M2 已冻结；后续阶段 M3 至 M5 已完成，下一阶段是排行榜、Dashboard 与 OpenTelemetry。

## M6 — 排行榜与可观测性

**Status: Complete**

### 已实现

- order.created -> order-consumer MySQL transaction -> order.succeeded -> rank-consumer -> Redis ZSet 的派生事件链路。
- Redis ZSet member 使用 order_id，排行榜只包含最终持久化成功订单；重复消息幂等。
- Gateway、order-consumer、rank-consumer 的 Prometheus metrics；包括 order_consumer_processed_total 和 kafka_consumer_lag。
- Grafana 自动预置 Seckill M6 Overview Dashboard。
- W3C traceparent 经 Kafka header 跨进程传播，Jaeger 可关联 HTTP、Kafka、MySQL 与 Redis span。
- JSON logs 同时记录 request_id 与 trace_id。

### 已执行测试

- 容器镜像构建、go test ./...、Compose 配置校验。
- 真实 HTTP -> Kafka -> MySQL -> Redis ZSet 端到端验证。
- Prometheus targets、Grafana Dashboard、Jaeger trace 查询。

### 已知限制

- 当前可靠性为 at-least-once + 幂等；MySQL transaction 与 order.succeeded 之间没有 Transactional Outbox。
- kafka_consumer_lag 是 consumer 成功提交 offset 后、相对于当前 fetch HighWatermark 的估算值。
- order-consumer 保持 max_in_flight=1，M6 未进行吞吐优化。

### 下一步

M6 已冻结；后续 M7 进行故障注入与告警验证。

## M7 — 故障注入与告警验证

**Status: Complete**

### 本阶段实际完成内容

- 增加仅 development 生效的 Redis、MySQL、Consumer、Etcd 延迟注入；其他环境会清零故障配置。
- 建立带有 service/component/severity/environment 标签的 Prometheus 规则，并通过 Alertmanager webhook 将 firing/resolved 事件送入 Alert Agent。
- Alert Agent 将输入转换为统一 `AlertEvent`，保留状态、标签、注释与时间并输出结构化日志；增加 `alert_events_received_total`。
- 真实演练覆盖 Consumer Bottleneck、Redis Degradation、MySQL Slow Transaction、Etcd Hot Lock 与高 429 比例。

### 新增 / 修改的关键文件

- 新增 `internal/fault/delay.go`、`internal/alert/event.go` 与对应单元测试。
- 修改 `internal/config/config.go`、`internal/gateway/gateway.go`、`internal/inventory/redis_store.go`、`internal/order/consumer.go`、`internal/order/metrics.go`、`internal/lock/locker.go`。
- 修改 `deploy/prometheus/alerts.yml`、`deploy/alertmanager/alertmanager.yml`、`docker-compose.yml`。
- 新增 `scripts/k6/m7_fault.js`；更新 README、DESIGN、STATUS 与 M7 演练记录。

### 当前真实架构

秒杀业务主链路不变。development 环境可在 Gateway Redis/Etcd 操作、order-consumer 消费及 MySQL transaction 处注入延迟。Prometheus 根据真实指标计算告警，经 Alertmanager 发送至 Alert Agent；Agent 规范化并记录 `AlertEvent`。当前 Agent 不做 RCA 或自动修复，M8 再实现证据驱动分析。

### 已执行测试

- `go test -race ./...`：在 Docker Go test stage 中执行。
- `go test -tags=integration -race ./tests/integration`：在 Compose 网络中连接真实 MySQL 与 Redis 执行。
- `docker compose config -q`：通过。
- 真实 Docker/k6 故障场景与 Prometheus → Alertmanager → Alert Agent firing/resolved 验证。

### 测试结果

- race 单元测试和 integration race 测试通过；Compose 配置校验通过。
- Consumer Bottleneck：`FAULT_CONSUMER_DELAY_MS=100`、`RATE=80`、`DURATION=20s`，Kafka lag 峰值约 812；`KafkaConsumerLagHigh` firing/resolved 均到达 Agent。
- Redis Degradation：延迟 250ms、`RATE=40`、`DURATION=22s`，HTTP P95/P99 约 259ms、Redis `deduct` latency 约 250ms；`RedisLatencyHigh` firing/resolved 均到达 Agent。
- MySQL Slow Transaction：延迟 250ms、`RATE=20`、`DURATION=22s`，440 个请求被接受；`MySQLTransactionLatencyHigh` 触发，恢复后解除。
- Etcd Hot Lock：Etcd 模式、延迟 350ms、`RATE=8`、`DURATION=25s`，锁等待 P95 约 1.975s、HTTP P99 约 2.485s；Etcd 锁等待与 HTTP P99 告警 firing/resolved 均验证。
- 高拒绝率：临时 Product rate/burst=20、`RATE=80`、`DURATION=25s`，2,001 次迭代中 520 个 `202`、1,481 个 `429`，告警表达式比例约 74.95%；`HighRateLimitRejectRate` firing/resolved 均验证。
- 所有演练后已恢复 `SECKILL_LOCK_MODE=redis_lua_only`、`FAULT_*_DELAY_MS=0` 与默认限流阈值；Prometheus active alerts 为空。

### 与原设计存在的差异

- `FAULT_ETCD_DELAY_MS` 在获取 Etcd mutex 后持锁注入，因此实测包含锁等待与当前每请求独立 lease/session 的端到端开销；这不是 Etcd 理论性能结论。
- 首次误用 M4 无限循环压测脚本时产生约 45 万请求和大量 429，该次未作为 M7 场景数据；正式数据使用固定到达率脚本。

### 当前已知问题

- Alert Agent 目前仅接收和记录事件，不提供 M8 RCA、证据查询或自动修复。
- 告警使用 1 分钟 `rate()` 窗口及 `for` 持续时间；故障清除后需等待窗口排空才会 resolved。
- AlertEvent 当前由 webhook 即时处理并记录，不是可独立重放的持久化消息队列。

### Acceptance Criteria 是否满足

满足 M7 范围：故障注入在 development 之外关闭；配置的告警规则能在真实指标变化时触发；Alertmanager 将 firing/resolved webhook 交付给 Agent；事件携带约定标签并被规范化记录；既有单元、race 与真实依赖集成测试通过；默认架构与 M4/M5 一致性行为未改变。

### 是否可以冻结本阶段

可以冻结 M7。Prometheus 当前无 active alerts，服务已回到默认运行配置。

### 下一阶段计划

M7 冻结后进入 M8：基于 AlertEvent 调用 Prometheus、健康检查、Kafka/Redis/Etcd 状态与近期日志工具，先收集证据再形成 RCA 与建议；不自动执行修复。

## M8A — 指标告警 RCA Baseline

**Status: Complete**

### 本阶段实际完成内容

- 增加 Alert Orchestrator：真实 firing `AlertEvent` 入有界内存队列，resolved 事件只记录；按 incident 去重并异步诊断。
- 接入 OpenAI-compatible Tool Calling 协议，模型可动态选工具，最多 8 次调用；工具/模型失败不会伪装成成功证据。
- 实现 7 个只读工具：Prometheus 指标、服务健康、Kafka、Redis、Etcd、结构化日志、Jaeger Trace。
- 固定 `RCAReport` schema，要求可验证根因类别、置信度、证据、替代假设、人工建议和证据缺口；Evidence source 必须来自本次成功调用的工具。
- 新增受 bearer token 保护的诊断查询 API、工具/诊断 Prometheus metrics，以及五类 M7 故障的在线评测脚本。
- 未增加持久化队列或自动修复能力。

- 用户目标已澄清为高并发时由检测告警动态唤醒 Agent，跨组件关联近同时/乱序日志并生成分级 RCA，交由员工修复；Agent 不自动修复。当前代码只实现指标告警触发后的单事件 RCA，尚未实现该目标。

### 新增 / 修改的关键文件

- 新增 `internal/alert/tools.go`、`provider.go`、`orchestrator.go` 及对应测试。
- 新增 `pkg/logger/ring.go` 与测试；日志仍写 stdout，同时进入每进程容量 2,000 的 ring buffer。
- 修改 `internal/config/config.go`、`internal/gateway/gateway.go`、`internal/order/consumer.go`、`internal/ranking/consumer.go`、`internal/observability/tracing.go`、`internal/alert/agent.go` 与 `docker-compose.yml`。
- 新增 `scripts/m8/benchmark.ps1`；更新 README、DESIGN、STATUS、`docs/alert-agent.md` 与 `docs/evolution.md`。

### 当前真实架构

Alertmanager firing → `AlertEvent` → bounded in-memory queue → Alert Orchestrator → OpenAI-compatible Tool Calling → 按需读取 Prometheus、健康端点、Kafka、Redis、Etcd、服务日志、Jaeger Trace → 固定 `RCAReport`。RCA 存在有界内存记录中，由受保护的 `/diagnoses` API读取。Agent 不具备任何系统修改工具。

M8A baseline 本身使用的 `get_recent_logs` 数据源是每进程 ring buffer；M8B-1 新增了集中日志投递和独立 Incident pipeline，但尚未将 M8A Logs Tool 改为 Loki。日志规则告警、Kafka incident queue、IncidentContext Agent 输入、确定性严重级别和通知仍未实现。

### 已执行测试

- Docker Go test image 编译所有服务。
- `go test -race ./...`：通过。
- `go test -tags=integration -race ./tests/integration ./internal/alert`：通过，覆盖既有业务依赖及 M8 诊断工具。
- Mock OpenAI-compatible server 测试了模型发起 `get_prometheus_metrics`、工具结果回传、后续结构化 RCA；固定指标目录、PromQL 限制、日志 ring 与 token 鉴权均有单测。
- Compose 网络真实联通测试成功读取 Prometheus、Gateway health、Kafka、Redis、Etcd、Gateway ring logs 与 Jaeger Trace；`/diagnoses` 未带 token 返回 HTTP 401。
- M8 runtime image 已构建并更新本地 `gateway`、`alert-agent`、`order-consumer`、`rank-consumer`；服务正常运行，Prometheus active alerts 为 0。
- DeepSeek OpenAI-compatible 在线 baseline 已运行五个指标故障场景；详情见 `artifacts/m8/benchmark-20261008-150155.csv`。
- benchmark 脚本已修正为等待 Prometheus firing 和 Alert Agent 收到事件后再恢复故障，避免告警链路竞态；失败诊断记录增加顶层 `alert_name`，避免 RCA 失败被错误地等待至通用超时。

### 测试结果

- 五类真实在线指标告警 RCA 均完成：根因分类 4/5（80%）、Tool Selection Accuracy 100%、False Root Cause Rate 0%（仅中/高置信度错判计入）、Average Tool Calls 8、Average RCA Latency 11,867ms。
- MySQL Persistence Bottleneck 被错判为 `redis_degradation`，置信度 low；报告把 `MYSQL_TX_P99(1m)` 与 `REDIS_P99(5m)` 解释为同窗证据，且缺少 Redis 状态与 trace 的直接证据。该错误不能因 low confidence 而从 root-cause accuracy 中忽略。
- 本轮在线评测只验证“指标告警 → Agent RCA”；没有执行高并发乱序日志测试。此结果不能宣称为目标 Alert Agent 的完整验收。
- PowerShell benchmark syntax 检查通过。当前 Codex 执行环境没有 Go executable，因此本轮 Agent tool-budget 变更未能在此环境执行 Go race tests；需在拥有 Go/Docker 构建环境中补跑。

### 与用户目标存在的差异

- 当前实现是 Prometheus 指标告警触发后的只读 RCA 原型，不会持续监视所有日志，也不做事件时间重排或跨服务告警聚合。
- 没有集中日志 backend、LogQL 告警、incident correlation window、持久化 incident queue 或员工通知渠道；这些是修订目标，不是已完成内容。
- RCA 工具可查询不同时间窗口；本次误判暴露出跨窗口指标被模型错误地视为同步因果的缺陷，当前 schema 尚未强制证据窗口对齐。

### 当前已知问题

- 在线 benchmark 需要本机 Docker Compose 依赖持续运行；它会按场景重建 Gateway 或 Consumer，并在结束时恢复默认参数。
- 工具调用达到 8 次上限；此次五类报告平均都达到上限，工具预算和证据选择还需优化。
- 每进程 ring buffer 会丢弃旧日志且不汇聚跨服务数据；短时间高并发时无法保证完整上下文。
- 当前 `False Root Cause Rate` 排除 low-confidence 错分类，应调整为同时报告 all-confidence classification error 与 high-confidence false RCA。
- Agent 诊断队列/记录仍为内存态；高并发及重启恢复需要持久化队列与去重设计。
- `OPENAI_API_KEY`、`OPENAI_BASE_URL`、`OPENAI_MODEL` 按本机 DeepSeek/OpenAI-compatible 配置；凭据不写入仓库。

### Acceptance Criteria 是否满足

当前原型的指标告警 RCA 路径和五类在线 baseline 已完成，但根因准确率为 80%，且并非用户要求的乱序并发日志检测/关联能力。修订目标的 Acceptance Criteria 尚未满足。

### 是否可以冻结本阶段

暂不可冻结。需要实现集中日志采集/规则、乱序 event-time incident correlation、严重级别策略、人工通知和相应高并发测试；同时修复跨窗口证据推理和评分定义。

### 下一阶段计划

先按用户确认的目标完成 Alert Agent 修订设计与组件边界评审，再实现 log ingestion/rules、event-time correlation、incident queue 和分级人工通知；建立多个并发故障、乱序日志、重复告警与多 incident 隔离测试。现有五类 RCA 数据保留为 baseline，不作为最终验收。

## M5 — Etcd Lock A/B

**Status: Complete**

### 已实现

- `SECKILL_LOCK_MODE=redis_lua_only` 与 `SECKILL_LOCK_MODE=etcd` 两种真实运行模式，默认保持 `redis_lua_only`。
- 商品级 `seckill:lock:product:<product_id>` Etcd mutex；每次 acquisition 使用独立 lease/session，避免同一 lease 的并发请求失去互斥语义。
- `ETCD_LOCK_SESSION_TTL`、`ETCD_LOCK_ACQUIRE_TIMEOUT`，以及 `etcd_lock_wait_duration_seconds`、`etcd_lock_acquire_total`、`etcd_lock_failure_total`。
- 单热点与 20 商品分散流量的 k6 A/B 脚本。

### 已执行测试

- `go test ./...`：通过。
- Etcd 集成 + race 测试：同商品请求严格互斥；不可达 Etcd 在 acquire timeout 内失败并增加 `etcd_lock_failure_total`。
- 100 VU、5 秒单热点与 20 商品的四组真实 k6 A/B；每组均在 Kafka `lag=0` 后核对订单与库存。详见 `docs/performance.md`。

### 已知限制

- Etcd 仍为单节点开发环境，结果不能外推到多节点集群。
- `max_in_flight=1` 的 Kafka consumer 保持为 M4 基准，没有在 M5 优化。

### 下一步

等待用户确认后进入 M6：Redis ZSet 排行榜、Grafana Dashboard 与业务 OpenTelemetry。

## M4 — Kafka 异步削峰

**Status: Complete**

### 已实现

- franz-go Producer，使用 broker ack、重试与失败 metrics。
- `order.created` event，包含 `event_id`、`order_id`、业务字段、`reservation_id` 与关联 ID。
- 独立 `order-consumer`，手动 offset commit，`max_in_flight=1`。
- MySQL transaction 内写订单、持久库存与 `processed_events`。
- Kafka event 重放幂等、同用户重复订单 Redis compensation。
- `kafka-init` Compose 服务显式创建 `seckill.order-created.v1`。

### 已执行测试

- `go test ./...`：通过。
- `go test -tags=integration -race ./tests/integration`：通过。
- 消费者集成测试：同一 `event_id` 重放不重复写单；同用户不同 event 只保留一笔订单并补偿 Redis。
- HTTP 端到端：Gateway 返回 `202 accepted`，消费者随后写入订单。
- Kafka consumer group：`CURRENT-OFFSET=1`、`LOG-END-OFFSET=1`、`LAG=0`。

### 已知限制

- `max_in_flight=1` 是有意的有界顺序消费实现；更高并发的分区安全 worker pool 留待后续优化。
- Gateway 仍会读取 MySQL 活动状态以完成时间、商品校验；MySQL 写入已不在请求同步路径。

### 下一步

M5 已冻结；下一阶段是排行榜、Dashboard 与 OpenTelemetry。

## M8B — 乱序信号关联与可靠告警

**Status: In Progress（M8B-1 已实现，待运行环境验证）**

### M8B-1 Implementation Report

#### 本阶段实际完成内容

- 增加 Grafana Alloy + Loki；Gateway、Alert Agent、order-consumer、rank-consumer 通过有界异步队列推送结构化 JSON 日志，保持 stdout 与本地 ring buffer。日志补充 `timestamp`、`event_time`、`ingest_time`；没有挂载 Docker socket。
- 新增 `Signal` 模型、Alertmanager 与 JSON log normalizer；实现可配置 event-time reorder buffer，默认 `INCIDENT_REORDER_WINDOW=3s`，使用 watermark 排序并统计 buffer size、乱序和迟到信号。
- 新增确定性 Correlator：优先按 `trace_id`、`request_id`、`order_id`、`activity_id`、`product_id` 聚合；缺少这些 ID 时按 alert identity 或 service/component 窗口分组。相同 Signal ID 去重但增加 occurrence count。
- 新增 Redis Incident Store，支持 create/update、resolve、get、list open 和 correlation-key 索引；Incident JSON、open index 与关联索引通过 Lua 原子更新，resolve 使用 Redis WATCH transaction。
- Alertmanager webhook 同时保留 M8A 原有处理，并将规范化 Signal 送入单独 M8B pipeline；Agent 尚未消费 Incident。
- 新增 Loki Query API client，可限制最多 100 条并按 service 与 `trace_id` 查询；Grafana provision 增加 Loki datasource。

#### 新增 / 修改的关键文件

- 新增 `deploy/loki/config.yml`、`deploy/alloy/config.alloy`、`deploy/grafana/provisioning/datasources/loki.yml`。
- 新增 `internal/signal/`、`internal/correlation/`、`internal/incident/`、`internal/logs/`；新增 Redis/Loki integration tests。
- 新增 `pkg/logger/loki.go`；修改 `pkg/logger/logger.go`、`internal/config/config.go`、`internal/alert/agent.go`、`docker-compose.yml` 与 `.env.example`。

#### 当前真实架构

Go JSON logs → bounded async logger queue → Alloy HTTP push endpoint → Loki；Prometheus firing/resolved → Alertmanager webhook → M8A 原 AlertEvent queue，同时转换 Signal → 3 秒 event-time reorder buffer → deterministic Correlator → Redis Incident Store。Redis 中使用 `incident:{id}`、`incident:index:open`、`incident:correlation:{hash}`。M8A RCA 与本阶段 Incident Store 当前并行，尚无 Incident→Agent 路径。

#### 已执行测试 / 测试结果

- 已新增 unit tests：C,A,B 到达后按 event_time 输出 A,B,C；超 watermark 信号计为 late arrival；同 trace 多 Signal 聚合；相同 Signal ID 去重；两个并发 trace 形成不同 Incident；Signal normalization；Loki Query 的 service/trace_id LogQL 过滤。
- 已新增 `go test -tags=integration -race ./tests/integration` 覆盖 Redis Incident Store 生命周期与 Alloy→Loki→service/trace_id 查询。
- 当前执行环境没有 `go` 命令；`docker compose run --rm go-test gofmt ...` 因当前进程无权访问 Docker Desktop named pipe 而失败。因此本轮没有实际跑 `gofmt`、`go test`、`go test -race` 或 integration tests，也没有验证容器内 Alloy/Loki 实际启动与查询。
- `docker compose config -q`：通过。该命令只验证 Compose 配置解析，不代表容器已启动或依赖链路可用。

#### 乱序测试结果

测试代码按 C、A、B 输入，并断言 Flush 顺序为 A、B、C；本环境未执行测试，因此**预期断言已写入但结果未验证**。

#### Incident correlation 测试结果

已写入同 `trace_id` 合并多个 Signal、两个不同 trace 并发隔离的单测；未执行，结果待 Go/Docker 可用环境验证。

#### Duplicate 测试结果

已写入 Signal ID 相同不重复 append、`occurrence_count` 增加的单测；Alertmanager alert identity 包含 alertname/service/component/labels。未执行，结果待验证。

#### Concurrent Incident 测试结果

Correlator 以 mutex 串行化单进程状态更新，测试并发提交 Redis 与 Kafka 两个不同 trace 并断言 Incident ID 不同。未执行 race detector，不能宣称并发验收通过；多 Alert Agent replica 的跨进程竞争尚未验证。

#### 与原设计存在的差异

- 本阶段尚未建立 Loki recording/alert rules；Logs Tool 也尚未从 ring buffer 切到 Loki。当前 Loki Query client 是独立能力。
- 事件进入 Alertmanager 后 Incident signal 会等待 event-time watermark；M8A 仍按原先方式单独收到 firing 并启动 RCA，M8A 不会等待 Incident 聚合。
- Correlation 使用关联 ID、alert identity、service/component 窗口；尚无完整服务拓扑图和无 ID 时的跨组件因果推断。
- 日志投递采用有界、非阻塞异步 queue；持续 collector outage 或队列满时会丢弃日志并保留 stdout，不会阻塞秒杀请求。

#### 当前已知问题

- 本地尚未运行 `gofmt`、Go unit/race/integration tests 或真实 Loki/Redis 验证；Compose 静态校验已通过，当前仍为实现待运行验证状态。
- Signal reorder buffer 与 Correlator 当前保存在 Alert Agent 进程内存；进程重启会丢失尚未越过 watermark 的 Signal 和窗口缓存。Redis Incident Store 已持久化已处理 Incident，但不是 incident queue。
- Redis API 当前有 Redis 原子更新，但全局串行化只覆盖单个 Alert Agent 进程；多副本下同 key 并发 read-modify-write 仍需分布式原子合并/锁方案验证。
- Redis 持久化失败时日志会记录错误，但已从 reorder buffer drain 的 Signal 不会自动重试；M8B-2 需用可靠事件队列解决。
- 高基数 `trace_id` 保持在 JSON body 中，不作为 Loki index label；查询使用 LogQL `| json | trace_id="..."`。

#### Acceptance Criteria 是否满足

代码层已覆盖 Loki/Alloy 配置、日志异步采集实现、service/trace_id Loki query client、统一 Signal、event-time reorder、确定性聚合/去重、Redis Incident Store 和对应测试代码。因测试与容器运行均未执行，且缺少同服务真实多容器日志查询、完整跨组件 correlation 和实测 race 结果，**M8B-1 Acceptance Criteria 尚不能判定满足**。

#### 是否可以冻结本阶段

暂不可冻结。需先在用户 Docker/Go 环境运行 Compose 配置校验、镜像构建、unit/race/integration tests，并实际确认 Alloy 收到多个 Go 服务日志及 Loki 可按 service/trace_id 查询；测试失败需修复后重新验收。

#### 下一阶段计划

先完成 M8B-1 的运行环境验证和问题修复；用户确认冻结后再进入 M8B-2。当前不实现 Kafka incident queue、IncidentContext Agent、通知或完整 benchmark。

## M3 — 分布式限流

**Status: Complete**

### 已实现

- 进程内 Token Bucket。
- Redis Lua distributed Token Bucket，支持 Global、Product、User 三级共享限额。
- `RATE_LIMITED`、HTTP `429`、`Retry-After` 与 `rate_limit_rejected_total`。
- Redis token refill 集成测试与 k6 限流验证。

### 已执行测试

- `go test ./...`：通过。
- `go test -tags=integration -race ./tests/integration`：通过。
- Redis bucket：burst 耗尽后拒绝，等待 1 秒后按配置速率回填。
- 同一用户连续六次 HTTP 请求：第六次返回 `429`、`RATE_LIMITED`、`Retry-After: 1`。
- k6 100 VU、5 秒：详见 `docs/performance.md`。

### 已知限制

- 配额仅支持环境变量配置，不支持运行时调整。
- Global 或 Product 拒绝时不会执行后续 User 检查。

### 下一步

等待用户确认后进入 M4：Kafka producer、order consumer、at-least-once 订单持久化与幂等消费。
