# 面试 13 问 × 项目实现对照

> 结论速览：本文按"能否用当前代码回答"逐题判定，标出代码位置、可直接背诵的话术，以及面试官追问时会被戳穿的缺口。
>
> 判定标记：✅ 有实现且能讲透 ／ ⚠️ 有实现但语义有缺口 ／ ❌ 当前没有实现（只能讲设计）

## 0. 总览

| #  | 问题关键词          | 判定 | 一句话结论                                        |
| -- | -------------- | -- | -------------------------------------------- |
| 36 | MySQL 扣减失败怎么办  | ✅  | Consumer 捕获后立即 Lua 补偿 + 幂等，失败则 offset 不提交靠重投 |
| 37 | 用户怎么知道结果       | ⚠️ | 只有轮询 `GET /orders/:id`，且"处理中"会返回 404，语义不完整   |
| 38 | 立即明确结果 + 付款即买到 | ❌  | 当前只有异步链路，同步实现已被移除，需要讲改造方案                    |
| 39 | DB 事务裁决 + 提前拦截 | ⚠️ | 组件齐了（Lua + Compensate + 条件更新 SQL），但没有同步路径    |
| 40 | 参与资格限制         | ❌  | 只有时间窗/状态/商品匹配 + 用户维度限流，没有资格体系                |
| 41 | Lua 检查了什么      | ✅  | 8 类检查，且全部在首次写入前完成，可讲得很细                      |
| 42 | 怎么记录已购买        | ⚠️ | 只有 MySQL 唯一键，**Redis 侧没有任何用户去重**             |
| 43 | 大 Key 怎么处理     | ❌  | 没实现（因为没做 Set），纯设计题，但要答出 Bitmap 这个最优解         |
| 44 | Redis 库存从哪来    | ⚠️ | 一次性 `SET NX` 预热，无定期同步、无版本号                   |
| 45 | 定期对账的作用        | ❌  | 项目完全没有对账模块，只在文档里写了"人工对账"                     |
| 46 | 两边何时扣减         | ✅  | Redis 先（准入），MySQL 后（裁决），取消时按状态位反向            |
| 47 | 立即回滚还是等对账      | ✅  | 立即回滚，且有"补偿失败→不提交 offset→重投"的兜底               |

**一句话总结给面试官**：这是个"Redis 做准入漏斗、MySQL 做最终裁决"的异步最终一致系统。13 个问题里，凡是围绕"异步链路如何保证不超卖/不丢单"的，代码都有对应实现；凡是围绕"同步强一致"和"运营治理（资格/对账）"的，就是我下一步要补的部分。

---

## 1. 当前真实链路（讲之前先在心里过一遍）

```text
Client
  → Gateway: 本地令牌桶 → Redis 限流(global/product/user)
  → validatePurchase: 查 MySQL 活动信息、校验时间窗          ← 瓶颈点，见 §5
  → Redis Lua: 校验库存 → 写 reservation Hash(prepared)
                → XADD Stream → 标记 reserved → DECR         ← 预占，同步完成
  → 返回 202 + order_id                                      ← 承诺是"已受理"，不是"已成单"
  → Outbox Publisher: Consumer Group 读 Stream → Kafka ack → XACK
  → Kafka order.created
  → Order Consumer: MySQL 事务
        processed_events 去重 → order_lifecycle 仲裁
        → INSERT orders → stock-1 → inventories-1 → 标记 applied → commit
  → Kafka order.succeeded → rank-consumer → Redis ZSet
```

关键事实：**202 的语义在 M9B-0 之后变弱了**。早期版本要等 Kafka broker ack 才返回 202；现在 Gateway 只等 Redis，Kafka 挂了也返回 202，消息堆在 Stream 里。可用性变好，但"已受理"不再等于"已进入 MQ"。

---

## 2. 逐题拆解

### Q36 Redis 扣减库存、进 MQ 后，MySQL 实际扣减失败怎么办

**判定：✅ 能答，这是你项目最扎实的一块**

**代码事实**（`internal/order/consumer.go`）

`persist()` 在同一个 MySQL 事务里做四件事：`INSERT orders`(L254) → `UPDATE seckill_activities SET stock=stock-1 WHERE stock>0`(L266) → `UPDATE inventories SET available=available-1, sold=sold+1 WHERE available>0`(L276) → 写 `processed_events`(L290)。失败分四条路径：

| 失败场景                     | 处理                         | 位置       |
| ------------------------ | -------------------------- | -------- |
| `orders` 唯一键冲突（同用户重复买）   | 回滚事务 → `compensateAndMark` | L257-262 |
| 活动库存条件更新 `affected != 1` | 回滚 → `compensateAndMark`   | L270-275 |
| 持久库存条件更新 `affected != 1` | 回滚 → `compensateAndMark`   | L281-286 |
| DB 连接断/超时/死锁等其他错误        | 返回 error → **不提交 offset**  | L136     |

`compensateAndMark`(L300) 的顺序很讲究：**先补偿 Redis，再开一个新事务写 `order_lifecycle.status='rejected'` + `processed_events`**。补偿本身由 Lua 保证幂等（`compensateScript`，`redis_store.go` L129-138）：只有 `state == 'reserved'` 才置为 `released` 并 `INCRBY 1`；已经是 `released` 直接返回 -1，不重复加。

**面试话术（可直接背）**：

> Redis 扣减和 MySQL 落库不在同一个事务里，所以我把"失败"分成可自动恢复和不可自动恢复两类。  
> 可自动恢复的是业务性失败：订单唯一键冲突、MySQL 条件更新影响行数为 0，这两类说明 Redis 多放行了，我立即用 Lua 把预占回补，脚本用 `reservation` 的状态机做幂等，只有 `reserved` 状态才能变 `released` 并加库存，重复调用不会多加。  
> 不可自动恢复的是基础设施失败：DB 超时、连接断。这时我不提交 Kafka offset，让消息重投，重投时 `processed_events` 和 `reservation` 状态两道幂等会把它收敛。代价是这个 partition 会阻塞，所以必须监控 consumer lag。  
> 补偿失败（比如 Redis 挂了）也走"不提交 offset + 重投"这条路，本质是"至少一次 + 幂等"，我不宣称跨 Redis/Kafka/MySQL 的 Exactly Once。

**缺口（主动承认，别等他问）**：补偿只覆盖"消息已被消费"的场景。如果 Stream 消息在 Redis 故障中整体丢失，MySQL 侧没有任何预占登记，无法自动补单，只能靠对账（见 Q45）。

---

### Q37 异步处理时用户怎么知道最终结果

**判定：⚠️ 有查询接口，但状态语义不完整**

**代码事实**：`GET /api/v1/orders/:id`（`gateway.go` L171）、`GET /api/v1/users/:user_id/orders`（L210）。`GetOrder`（`mysql_service.go` L174-200）策略是：先查 `orders`，查不到再回退查 `order_lifecycle`。

由此得到三种可见结果：

| 时刻                  | 返回                                | 用户理解                       |
| ------------------- | --------------------------------- | -------------------------- |
| 202 之后、Consumer 落库前 | `404 ORDER_NOT_FOUND`             | ❌ 无法区分"处理中"和"失败"           |
| 落库成功                | `status=created`                  | 抢到了（但这不是"处理中"，是"已占库存、待支付"） |
| 被补偿                 | `order_lifecycle.status=rejected` | 失败，已回补                     |

**这是本题最大的坑**：202 到落库之间是一个"查询盲区"。秒杀高并发下 Consumer 有积压时，用户可能轮询 10 秒都是 404，体验上等于"失败了"。

**面试话术**：

> 当前是前端拿 `order_id` 轮询 `GET /orders/:id`。落库成功是 `created`，`created` 就代表库存已经真实扣掉了，所以"查得到即抢到"；被补偿的单会落在 `order_lifecycle` 里状态 `rejected`，查询接口会回退到这张表，所以用户能看到明确的失败。  
> 但我知道这里有个语义缺口：202 到落库之间的窗口，两张表都没有记录，用户会拿到 404，分不清"处理中"还是"失败"。我的改进是让 202 同时返回 `reservation_id`，新增 `GET /seckill/result?reservation_id=` 直接读 Redis 的 reservation Hash，`reserved` 就是处理中，`released` 就是失败。这样是三态而不是二态。

**缺口清单**：

- 没有 WebSocket / SSE 推送，只能轮询。
- `orders.status` 只有 `created/succeeded/cancelling/cancelled`，没有 `failed/expired`。
- 没有"超时未支付自动关单"的定时任务（取消接口只能手动调，且只在 development + 开关下注册）。

---

### Q38 要求成功时立即给明确结果，付款成功即买到，怎么调整

**判定：❌ 当前代码答不了（同步链路已被移除），必须讲改造设计**

先确认事实：`grep "func (s *Service) Purchase"` 只有 `PurchaseAsync`，没有同步版本。`docs/consistency.md` 里描述的 M1 同步事务只存在于历史阶段，现在的 `m1_mysql_test.go` 也是直接调 `store.Reserve` + Consumer。

**改造方案（分四层讲，显得有体系）**

1. **同步化主链路**：Lua 预扣成功后，**同步**执行原来 Consumer 里的那个事务（`INSERT orders` + 两个条件 UPDATE），**事务提交才返回 200 + order_id**。失败就立即 Lua 补偿并返回失败。这样"接口返回成功"就等于"MySQL 已有订单"，结果明确。
2. **Kafka 降级为下游**：原来的 `order.created` 不再承担"下单"职责，只用来做风控、积分、发券、数据同步。即使 MQ 挂了，下单也不受影响。这是关键的解耦点。
3. **"付款成功即买到"的前提是库存已在支付前锁定**：订单在 `created` 时就已经扣了库存，支付回调只做 `created → succeeded`，不参与抢库存。所以必须配"超时未支付 → 取消 + 回补"，否则库存会被未支付订单长期占用。你现在的 `MarkOrderPaid`（`lifecycle.go` L14）和 `CancelOrder`（L73）已经是这个状态机的雏形，可以直接复用，只差一个定时扫描 `created` 超时订单的任务。
4. **性能代价必须主动交代**：同步事务把吞吐压到 MySQL 水平（几千 TPS）。能接受的原因是 Redis 前置漏斗把真正打到 DB 的请求压到了**库存数量级**——1000 件商品最多 1000 个事务，而不是 10 万个。所以"同步"和"高性能"不矛盾，前提是漏斗够狠 + 限流 + 库存小。

**一句话收尾**："异步方案牺牲的是结果的即时性，换来的是削峰和数据库保护；同步方案牺牲的是峰值吞吐，换来结果的确定性。我的项目现在走前者，改成后者只需要把 Consumer 的事务搬回 Gateway，并给 Kafka 换一个角色。"

---

### Q39 用数据库事务保证结果明确和数据一致，同时允许提前拦截超额请求

**判定：⚠️ 设计上就是 M2 的形态，组件都在，但没有拼成一条路**

**设计要点（这是本题的标准答案结构）**

1. **Redis 阶段只做"准入"，不做"裁决"**：Lua 里 `DECR` 成功只代表"你进了漏斗"，不承诺成功。超额请求在这里被挡掉，不碰 DB。
2. **MySQL 事务是唯一裁决者**：`UPDATE ... WHERE stock > 0` + `UNIQUE(activity_id, user_id)` 才是最终真相。条件更新的 `affected rows` 就是裁决结果，不需要先 `SELECT` 再判断（避免乐观锁 ABA 和额外一次查询）。
3. **单向容错原则**：允许 Redis 多放行（假阳性，后面补偿掉），**绝不允许 Redis 少放行导致 MySQL 超卖**。所以 Redis 库存可以 ≥ MySQL 库存，反过来就是事故。
4. **失败即回补**：事务失败 → Lua 补偿 → 返回失败。补偿幂等。
5. **对账兜底**：Redis 剩余与 MySQL 剩余差异超阈值告警。

**项目里已经有的零件**：

- 漏斗：`reserveScript`（`redis_store.go` L87）
- 裁决 SQL：`consumer.go` L266、L276 的条件更新，以及 `002_m1_schema.sql` 的 `uk_orders_activity_user`
- 回补：`compensateScript` L129
- 幂等：`processed_events` + reservation 状态机

**要补的一步**：把这些搬进一个 `PlaceOrderSync`，在 Gateway 里顺序调用，去掉 Kafka 这一跳。

**注意一个性能细节**：`validatePurchase`（`async.go` L87）每个请求都查一次 MySQL 拿活动配置。同步化之后这就是"每请求 2 次 DB"，必须把活动配置改成**本地缓存**（启动加载 + 变更事件刷新，或 Redis 缓存 + 秒级本地缓存），否则漏斗白做了。

---

### Q40 有没有参与资格限制？是否所有用户都能参与

**判定：❌ 没有资格体系**

**代码事实**：`validatePurchase`（`async.go` L87-113）只校验五件事：activity 存在、product 与 activity 匹配、`status == 'active'`、`now >= start_time`、`now < end_time`。没有任何用户维度校验。

唯一与"用户"相关的是限流（`limiter/service.go` L43）：`seckill:limit:user:{activityID}:{userID}` 按 QPS 限流。但这是**防刷**，不是**资格**——它只限制频率，不判断"你有没有资格"。

**面试话术**：

> 当前版本没有资格限制，所有登录用户都能参与，只做了活动时间窗校验和用户维度的 QPS 限流。  
> 如果要做，我会分两层：资格数据落 MySQL（`activity_eligibility(activity_id, user_id)`，来源可以是预约、会员等级、白名单），活动开始前预热到 Redis 一个 Set，Lua 里在扣库存之前做 `SISMEMBER`，不满足直接返回"无资格"，连库存都不看。这样超额流量在 Redis 层就被挡掉了，不会产生"占了库存又要回补"的浪费。  
> 另外，资格校验是只读判断，可以放在扣减之前，不需要和 `DECR` 严格原子；但"一人一单"的判重必须和 `DECR` 在同一个脚本里，因为那是写操作，有竞态。

---

### Q41 Redis 的 Lua 脚本具体检查哪些内容

**判定：✅ 能答，而且能答出"为什么这么排"**

`reserveScript`（`redis_store.go` L87-127）的检查序列：

| 顺序 | 检查                                                                                                                   | 失败码     |
| -- | -------------------------------------------------------------------------------------------------------------------- | ------- |
| 1  | `ARGV[3..6]`（order_id/activity_id/product_id/user_id）必须是 `^%d+$` 且 > 0                                               | -4      |
| 2  | `reservation_id`、`event_id`、`amount` 非空                                                                              | -4      |
| 3  | `KEYS[1]` 库存 key 存在且类型为 `string`（`none` → 未预热）                                                                       | -2 / -4 |
| 4  | `KEYS[2]` reservation 类型必须是 `none` 或 `hash`                                                                          | -4      |
| 5  | `KEYS[3]` stream 类型必须是 `none` 或 `stream`                                                                             | -4      |
| 6  | `GET` 库存值必须能 `tonumber`                                                                                              | -4      |
| 7  | reservation 现有状态：`released` → 拒绝（-3）；`reserved` → 逐字段比对前 7 个字段，一致则**幂等返回当前库存**（不重复扣），不一致 → -4；其他非 `prepared` 状态 → -4 | -3 / -4 |
| 8  | `stock <= 0` → 售罄                                                                                                    | -1      |
| 9  | 写入：`HSET prepared` → `XADD` → `HSET reserved` → `DECR`                                                               | 返回剩余库存  |

**最值得讲的一点**（代码注释 L83-86 已经写了）：**所有检查必须在第一次写之前完成**。因为 Redis 的 Lua 在运行期出错时**不会回滚已执行的命令**。所以顺序被刻意设计成 `prepared → XADD → reserved → DECR`：最坏情况是留下一条可检测的 `prepared` 孤立记录，而不是"扣了库存但没有预占元数据"——后者是不可恢复的隐性丢单。

**面试官可能追问**："Lua 保证原子性，那为什么还要 ORDER 这么讲究？" → 答："原子性指的是不被其他命令插入，不指失败回滚。运行期错误（比如对错误类型执行命令）已经写进去的改动是留着的。" 这一问一答能直接区分"用过 Redis"和"背过 Redis"。

**要主动承认**：脚本**没有做用户级去重**（这是 Q42 的缺口），也**没有校验活动时间窗和资格**（放在 Go 里的 `validatePurchase`，意味着每个请求多一次 MySQL 查询）。

---

### Q42 如何记录用户已经购买过？什么数据结构

**判定：⚠️ 只有 MySQL 唯一键，Redis 侧没有**

**代码事实**：全项目 `grep SADD|SISMEMBER|HSETNX` 结果为空。当前的"一人一单"完全靠 `scripts/mysql/002_m1_schema.sql` 里 `orders` 表的：

```sql
UNIQUE KEY uk_orders_activity_user (activity_id, user_id)
```

也就是在 **Consumer 异步落库时**才发现重复，然后 `compensateAndMark` 回补 Redis。`docs/consistency.md` L34 也写明了这个设计。

**这个设计的问题（必须主动说，否则被问穿）**：用户连点 100 次，Lua 会扣 100 次库存，才在落库时回补 99 次。这中间其他用户可能看到"售罄"——**少卖 + 误杀**。而且 100 条消息全部走完 Kafka + MySQL 事务 + 补偿，是纯浪费。

**应该怎么做**：

```lua
-- 放在同一脚本里，DECR 之前
if redis.call('SISMEMBER', KEYS.buyers, ARGV.user_id) == 1 then return -5 end
...
redis.call('SADD', KEYS.buyers, ARGV.user_id)
redis.call('DECR', KEYS.stock)
```

**数据结构选择**：

| 结构                                                    | 适用             | 备注                           |
| ----------------------------------------------------- | -------------- | ---------------------------- |
| **Set** `SADD seckill:buyers:{aid}`                   | 只需判断"是否买过"     | 语义最直白，O(1)，但有 Q43 的大 Key 问题  |
| **Hash** `HSETNX seckill:buyers:{aid} userID orderID` | 还要能查"我买的哪一单"   | 比 Set 多存一个 orderID，方便前端直接跳订单 |
| **Bitmap** `SETBIT key userID 1`                      | userID 紧凑、规模极大 | 内存最省，但要求 userID 可映射为 offset  |

**一个容易漏的细节**：取消订单后要不要 `SREM`？这是业务决策，不是技术决策。如果规则是"取消后可以重新抢"，那 release 补偿时必须同步 `SREM`，否则用户永远抢不到第二单。这个点讲出来很加分。

---

### Q43 十几万、二十万用户都在一个 Set 里形成大 Key，怎么处理

**判定：❌ 没实现（因为没做 Set），纯设计题**

先把量级说清楚，别空谈：

- Set 元素超过 `set-max-intset-entries`（默认 512）后转 hashtable 编码，每个成员约 60-100 字节（dictEntry + SDS + 哈希表负载因子）。
- 20 万用户 ≈ **15-25MB**，加上 bucket 数组（2^19 × 8B ≈ 4MB）。
- 业界大 Key 经验阈值是"集合元素 > 5000"，20 万已经远超。
- 危害不是"内存不够"，而是：① Cluster 下**只能落一个 slot/一个节点**，读写全打一个分片；② `SMEMBERS`/`DEL`/`RDB fork`/迁移时会**阻塞主线程**；③ 主从同步和扩容迁移都会卡。

**方案（按推荐度排序，能讲出取舍就是加分）**

1. **Bitmap（最优解）**：20 万用户只需 `200000 / 8 = 25KB`，比 Set 小三个数量级。代价是要求 userID 能映射成紧凑 offset：如果 userID 是全局自增且范围可控，直接 `SETBIT key userID 1`；如果 userID 稀疏（比如带业务前缀），需要"活动内序号映射"或按 `userID / 65536` 拆多个 Bitmap。
2. **分片 Set**：`seckill:buyers:{aid:s0}` … `{aid:s31}`，分片键用 `userID % 32`。每个 key 只有几千人，单 key 小，而且**每个分片用独立 hash tag，可以落到不同 slot**——这点很关键：因为每次判断"我买没买"只需要查自己那一片，是**单 key 操作**，不需要 CROSSSLOT，所以分片能真正分散到不同节点。（如果固定用 `{aid}` 做 hash tag，全部分片会挤在同一个 slot，等于没拆。）
3. **布隆过滤器**（RedisBloom）：内存极小，但有假阳性——会把没买过的误判为买过，造成误杀。只能当**前置快速拦截**，后面还得有 Set/DB 兜底。
4. **生命周期治理（治本）**：秒杀的购买集合是**临时热点数据**，不是长期存储。活动结束 + 对账完成后，归档到 MySQL 明细表，然后 `DEL` 或设 7 天 TTL。很多"大 Key 问题"本质是"忘了删"。
5. **本地缓存前置**：热门活动的判重可以在网关进程内存里放一层 Cuckoo Filter，命中"肯定没买"才去 Redis。

**收尾话术**："大 Key 的真正问题不是内存，是单分片热点和阻塞式操作。我会先用 Bitmap 把 25MB 压到 25KB；如果 userID 不连续就分片 Set；最后一定会给这个集合设生命周期，因为活动结束后它就是垃圾数据。"

---

### Q44 Redis 库存从哪里来？定期同步还是独立维护

**判定：⚠️ 一次性预热，无同步、无版本**

**代码事实**：`WarmupActivity`（`mysql_service.go` L143-172）：

```sql
SELECT a.product_id, a.stock, i.available, a.status
FROM seckill_activities a JOIN inventories i ON i.product_id = a.product_id
WHERE a.id = ?
```

取 `min(activity.stock, inventory.available)`，然后 `SetNX(key, stock, 0)`（`redis_store.go` L181，**ttl=0，永不过期**）。重复预热返回已存在的值，不覆盖（L187-198）。

**这个设计好在哪（主动讲）**：

- `SET NX` 保证幂等，运维重复调预热不会把正在销售的库存重置回初始值——这是很实际的线上事故点。
- 取 `min(活动库存, 商品可用库存)`，避免"活动说 100 件但仓库只有 80 件"的虚假库存。
- 库存 key 不存在时 **拒绝售卖**（Lua 返回 -2 → `SECKILL_STOCK_NOT_WARMED`），不会 fallback 到 MySQL 直接卖。这个"故障时保守"的选择是对的。

**缺口**：

- **没有定期同步**。活动进行中如果运营改库存、或有退款回补到 MySQL，Redis 不会感知。
- **没有版本号**。无法判断 Redis 里的值是不是针对当前活动配置的。如果活动被改了库存再重新预热，`SET NX` 会拒绝更新，Redis 与 MySQL 永久偏差。
- 没有预热失败告警。

**改进**：库存 key 改成 Hash 存 `{stock, version, warmed_at}`，预热时用 `version` 做 CAS；配一个"重新预热"接口（强制覆盖，仅活动未开始时允许）；对账任务定期比对两边剩余。

---

### Q45 秒杀几秒钟就结束，定期对账有什么作用

**判定：❌ 项目没有对账。这是最容易被问穿的一题**

先给事实：全仓库 `grep "reconcil|对账"` 只命中两处**文档**（`docs/code-reading-guide.md` L283、`docs/m9-final-cancellation.md` L61），说的是"依赖人工对账"，**没有任何代码实现**。

**为什么秒杀短，反而更需要对账（这是本题的核心论点）**

> 对账不是为了"实时正确"，实时正确靠的是即时补偿 + 幂等重试。对账解决的是两件实时机制解决不了的事：**发现不可自动修复的差异**，和**把钱/货的账做平**。

具体四类问题：

1. **漏单/少卖**：Redis 预扣成功，但 Stream 消息丢失或 Consumer 永久失败。Redis 少一个库存、MySQL 少一个订单——**两边"都少"，账面上是对得上的**，但用户付了钱没拿到货，或者名额被白吃了。只有按 `reservation` 记录去比对订单，才能发现。
2. **幽灵占用**：reservation 是 `reserved` 状态，但没有对应订单，且活动已结束。你现在的 release 机制只在"取消"时触发，**没有"超时扫描"任务**，这类记录会永久占着库存。
3. **补偿失败的长期残留**：Redis 挂 10 分钟，那段时间所有补偿都失败。虽然 offset 没提交会重投，但如果这期间活动结束、Consumer 被下线，这些预占就没人管了。
4. **账面校验（财务视角）**：活动结束后必须核 `初始库存 = MySQL sold + 有效取消回补数`，以及 `Redis 剩余 = 初始 - 有效 reservation 数`。秒杀涉及真金白银，差异必须出差异单。

**可以讲的实现方案**（一句话带过即可，别展开太细）：

> 一个独立的 `reconcile` 任务，两层：第一层扫 `order_lifecycle` 里 `reservation_state='reserved' AND persistent_inventory_state='not_applied'` 且超过 N 分钟的记录，生成补偿任务——这层有实时价值；第二层活动结束后做全量账面核对，比对 Redis 剩余库存和 MySQL `sold`/`available`，差异落差异表人工处理。对账**只读不写**，回补动作仍然走幂等的 release 任务，不能在对账脚本里直接改库存。

**最后一句保护自己**："我当前版本没实现对账，这是已知缺口，我把一致性边界写在了 DESIGN.md 里——不宣称跨系统 Exactly Once，Redis 整体丢数据时靠监控和人工恢复。"

---

### Q46 Redis 库存和 MySQL 库存分别什么时候扣

**判定：✅ 能答**

|       | 时机                                                 | 位置                                  | 角色                 |
| ----- | -------------------------------------------------- | ----------------------------------- | ------------------ |
| Redis | Gateway 同步路径，Lua 内 `DECR`，在写 reservation 和 XADD 之后 | `redis_store.go` L126               | **准入**：先占名额，拦截超额流量 |
| MySQL | Order Consumer 事务内，`INSERT orders` 之后              | `consumer.go` L266（活动库存）、L276（持久库存） | **裁决**：最终事实        |

顺序是 **Redis 先、MySQL 后**，中间隔一次 Kafka 投递，延迟从几十毫秒到数秒。

取消时是**反向且不对称**的（这个细节很加分）：`releaseScript` 先幂等 `INCRBY` 回补 Redis；MySQL 是否回补要看 `order_lifecycle.persistent_inventory_state` 是否为 `applied`（`005_m9b_outbox_inventory_state.sql`）——因为"提前取消"的订单可能根本没扣过 MySQL 库存，盲目回补会造成 MySQL 库存虚增。

**一句话**："Redis 是先手，负责挡人；MySQL 是后手，负责定案。取消时 Redis 一定回补，MySQL 要看它到底扣没扣过。"

---

### Q47 MySQL 扣减失败后，Redis 立即回滚还是等对账

**判定：✅ 能答，本项目选择立即回滚，且理由充分**

**代码事实**：`compensateAndMark`（`consumer.go` L300）在 MySQL 事务回滚后**同步**调用 `Compensate`（L303），2 秒超时（L301）。不是等对账。

**为什么必须立即回滚（三层理由）**

1. **时间窗**：秒杀只有几秒。等对账意味着这个名额在整个活动期间都是"死名额"，直接变成少卖。对账通常是 T+0/T+1，等到那时候活动早结束了。
2. **补偿是安全的**：Lua 用 reservation 状态机保证幂等，重复调用不会多加，所以"立即回滚 + 重试"没有副作用。
3. **有兜底**：补偿失败 → 返回 error → offset 不提交 → Kafka 重投 → 再次补偿。这是"立即回滚 + 有界重试"，对账只是最后一层。

**一个刻意设计的顺序，值得主动讲**：`compensateAndMark` 是**先补偿 Redis，再写 `rejected` 标记**，两者不在同一事务里。如果进程在中间崩溃，会出现"Redis 已回补、MySQL 还没有 rejected 标记"的窗口。重投后第二次 `Compensate` 返回 `-1`（already applied），代码把它当成功继续写标记，最终收敛。这个顺序是**故意选的**：宁可多一次空跑，也不能出现"标记了已处理但库存没还"——后者是永久少卖。

**什么场景才该"等对账"**：库存充裕、活动周期长（比如持续几天的预售）、回补动作会与新一轮预扣产生竞争、或者 Redis 短暂不可用时不希望阻塞主链路。这时候"先记账、后回补"更稳。但秒杀不属于这类场景。

---

## 3. 面试前最该补的 5 件事（按性价比排序）

| 优先级 | 事项                                                                                                       | 工作量   | 价值                             |
| --- | -------------------------------------------------------------------------------------------------------- | ----- | ------------------------------ |
| P0  | **Lua 里加用户去重**（Set/Bitmap + `SISMEMBER`/`SADD`），解决 Q42 的"扣 100 次再回补 99 次"                                | 半天    | 直接消掉本项目最明显的设计缺陷，且 Q42/Q43 一起答完 |
| P0  | **补一个 `GET /seckill/result?reservation_id=`**，读 reservation Hash 返回 `reserved/released`，消掉 202 后的 404 盲区 | 半天    | Q37 从 ⚠️ 变 ✅                   |
| P1  | **活动配置本地缓存**，去掉 `validatePurchase` 的每请求 MySQL 查询                                                         | 1 天   | 否则"Redis 扛 10w"这句话站不住          |
| P1  | **写一个 `cmd/reconcile`**：扫超时未落库的 reserved 记录 + 比对两边库存                                                     | 1-2 天 | Q45 从 ❌ 变 ✅，也是"有治理意识"的证明       |
| P2  | **同步下单分支** `PlaceOrderSync`（配置开关切换），支持 Q38/Q39 的对比讲解                                                     | 1-2 天 | 能讲出"两套方案的取舍"，面试叙事完整度翻倍         |

---

## 4. 代码里应该主动提的细节 vs 会被戳穿的坑

**主动提（这些是加分项，说明你真读懂了）**

1. **Lua 所有校验在首次写入前完成**（`redis_store.go` L83-86 注释）——因为 Redis Lua 运行期错误不回滚已执行命令。
2. **库存 key 不存在时拒绝售卖而不是 fallback 到 MySQL**（`DeductionNotWarmed` → 409）——故障时保守，不制造超卖。
3. **取消回补要判断 `persistent_inventory_state`**——提前取消的订单可能没扣过 MySQL 库存，盲目回补会虚增。
4. **`releaseScript` 用 `release_id` 做二次幂等**，同一 reservation 被不同 release 重复释放会返回 -5 冲突，而不是静默加库存。
5. **订单 ID 不编码用户 ID**（`internal/order/id.go`）——避免泄露和耦合，用户 ID 单独存字段。

**会被戳穿的坑（提前想好怎么说）**

1. **`validatePurchase` 每请求一次 MySQL**（`async.go` L92）。10w QPS 下这是明确瓶颈。话术："这是当前版本的已知瓶颈，改进是活动配置本地缓存 + 变更事件刷新。"
2. **`Compensate` 里有两处 `if result == -3`**（`redis_store.go` L345、L350），第二个永远不可达，是死代码。顺手删掉，别让面试官先看到。
3. **`compensateScript` 硬编码 `INCRBY 1`**，而 `releaseScript` 用 `tonumber(ARGV[5])`。因为 `Reserve` 强制 `Quantity == 1`（L204）所以不出错，但两个脚本风格不一致。可以统一成用 quantity。
4. **`Compensate` 不校验 productID 与 reservation 记录是否一致**（`releaseScript` 校验了 5 个字段，`compensateScript` 只靠 key 定位）。如果传入错误的 productID，会 `INCRBY` 到另一个商品的库存 key 上。虽然调用方是内部 Consumer、event 来自自己发的消息，风险低，但被问到"补偿会不会补错商品"时要能答上来。
5. **202 的语义变弱了**：M9B-0 之后 Gateway 不再等 Kafka ack。Kafka 挂了仍返回 202。这是可用性换承诺强度，要能说清取舍。
6. **没有超时未支付关单**，库存会被 `created` 状态长期占用。

---

## 5. 30 秒电梯陈述

> 这是一个 Go 实现的秒杀系统，核心是"Redis 做准入漏斗、MySQL 做最终裁决"。  
> 请求先过本地令牌桶和 Redis 三级限流，然后在**一个 Lua 脚本**里完成库存校验、预占记录、写入 Redis Stream 和扣减库存——四步原子，全部校验放在首次写入之前，因为 Redis Lua 出错不回滚。  
> 之后由 Outbox Publisher 可靠地转投 Kafka，Order Consumer 在一个 MySQL 事务里写订单、扣活动库存、扣持久库存、记幂等表，成功才提交 offset。  
> MySQL 扣减失败时立即用幂等 Lua 回补，补偿失败就不提交 offset 靠重投收敛，对账作为最后兜底。  
> 我知道现在的缺口是：用户去重只在 MySQL 唯一键上做、没有资格体系和自动对账、异步链路让用户在 202 到落库之间看不到状态——这三个是我下一步要补的。

