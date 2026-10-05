# message-fanout-gateway-go

本地可运行的长连接消息分发网关。内置可控事件源（`Source`）模拟上游变更，
在多订阅者场景下保证消息顺序、中断恢复与慢消费者处理具有一致且可解释的结果。

## 架构

```
Source (可控事件源) --> Broker --> topicLog (每主题保序日志, 固定保留窗口)
                        |
                        +-- Subscriber A (独立缓冲 + 处理 goroutine)
                        +-- Subscriber B (独立缓冲 + 处理 goroutine)
```

- `gateway/topicLog`：单主题消息日志，Seq 从 1 单调递增，超出 `RetainPerTopic` 的旧消息被裁剪。
- `gateway/Broker`：主题管理、订阅注册与分发。`Publish` 与 `Subscribe/Unsubscribe`
  在同一互斥锁下串行化，保证同一主题的消息按发布顺序进入每个订阅者缓冲，
  且并发订阅增删不会造成重复投递或漏投。
- `gateway/Subscriber`：独立缓冲 channel + 专属消费 goroutine，互不阻塞。

## 分发语义

- 同一主题的消息按发布顺序（Seq 升序）投递给**当前全部**订阅者。
- 投递对每个订阅者是**非阻塞**的：单个订阅者过慢不会阻塞或拖慢其它订阅者。
- 订阅者处理结果分为三类，且满足守恒不变式：

  ```
  Delivered + Dropped + Failed == 应投递给该订阅者的消息总数
  ```

  - `Delivered`：处理函数返回 nil。
  - `Failed`：处理函数返回 error（cursor 不推进）。
  - `Dropped`：丢弃总数，按原因细分为两类（见下）。

### 丢弃与失败的分类口径

- `Dropped == DroppedBackpressure + DroppedShutdown`：
  - `DroppedBackpressure`：缓冲满按策略丢弃（慢消费者背压）。
  - `DroppedShutdown`：订阅者下线收敛超时丢弃（见「优雅下线语义」）。
- `Failed == FailedBy 各类别之和`：处理函数返回的错误若实现
  `FailureCategorizer` 接口（可用 `NewHandlerError(category, err)` 构造），
  按类别计入 `FailedBy[category]`；否则归入默认类别 `handler_error`。

## 慢消费者策略（背压）

每个订阅者拥有容量为 `SubscriberBuffer` 的独立缓冲。缓冲满时按 `Policy` 处理：

| 策略 | 行为 |
|---|---|
| `DropNewest`（默认） | 丢弃新到的消息，计入 `Dropped`；保证老消息不丢 |
| `DropOldest` | 挤出缓冲中最旧的一条计入 `Dropped`，放入新消息；保证新消息尽快到达 |

被丢弃的消息**始终计入 `Dropped`**，不会凭空消失；任何路径都不会凭空增加消息。

## 优雅下线语义（退订收敛）

`Unsubscribe`（或同 ID 重订阅替换旧会话）触发优雅下线：

1. 立即停止接收新消息（已从 Broker 注销，`Publish` 不再分发给它）。
2. 进入宽限收敛：缓冲中排队等待处理的消息、以及正在处理的那一条，
   在宽限时间 `DrainTimeout` 内继续处理——处理成功计 `Delivered`，
   处理报错按类别计 `Failed`。
3. 宽限期到仍未处理完的（排队中 + 处理中），在结算时刻全部计为
   `DroppedShutdown`，不会凭空消失；处理中的 handler 晚些返回时
   不再重复计数（该消息已归属下线丢弃）。

结算方式记录在会话账目的 `Reason` 字段：`drained`（宽限期内排空）或
`drain_timeout`（超时强制结算）。

## 会话账目与主题聚合（审计）

- `Broker.SessionReports(topic, id)`：查询已结算的会话账目 `SessionReport`
  （成功/丢弃/失败各自数量与分类、最终游标、结算方式、结算时间）。
  订阅者离开后账目仍保留；`id` 传空串返回该主题全部已结算账目。
- `Broker.TopicStats(topic)`：按主题聚合的运营视角——累计发布量
  `Published`、累计应投递量 `Dispatched`、合计成功/丢弃/失败（含分类明细）、
  当前活跃订阅者数与累计会话数。已离开订阅者的贡献永久计入聚合。
- 聚合守恒口径（高频上下线、并发压力下始终成立）：

  ```
  Dispatched == Delivered + Dropped + Failed
  Dropped    == DroppedBackpressure + DroppedShutdown
  Failed     == sum(FailedBy)
  ```

  对单个从 0 开始且持续在线的订阅者，进一步有 `发布总量 == 成功 + 丢弃 + 失败`。

## 中断恢复语义

- 订阅者通过 `Cursor()` 获取已确认位置（最后一条成功处理消息的 Seq）。
- 重新接入时携带该 cursor 调用 `Subscribe`，从 `cursor+1` 继续，不重放、不跳过。
- 非法游标被**明确拒绝**，错误类型为 `*gateway.Error`，`Kind` 可判定：

| Kind | 含义 |
|---|---|
| `cursor_expired` | `cursor+1` 早于当前保留的最早 Seq，位置已被裁剪，无法恢复 |
| `cursor_regression` | `cursor+1` 超过下一条将分配的 Seq（位置在未来/发生倒退） |

被拒绝时**不会**从中间继续或静默重放，调用方须根据 `Kind` 决定重新全量订阅或报错。

## 可配置项

`gateway.Config`：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `RetainPerTopic` | 1024 | 每主题日志保留条数，即可恢复的游标窗口 |
| `SubscriberBuffer` | 64 | 每订阅者缓冲容量 |
| `Policy` | `DropNewest` | 缓冲满时的丢弃策略 |
| `DrainTimeout` | 2s | 订阅者下线宽限时间。`0` 表示未设置，取默认值；负数归一化为「不给予宽限」，退订后立即结算，排队与在途消息全部计为 `DroppedShutdown` |

命令行演示参数：`go run ./cmd/gateway -h`
（`-retain` / `-buffer` / `-n` / `-interval` / `-drain`）。

## 兼容范围

- 本次新增为**向后兼容**扩展：`Stats` 原有字段（`Delivered`/`Dropped`/`Failed`）
  与守恒不变式保持不变，仅新增分类明细字段（`DroppedBackpressure`/
  `DroppedShutdown`/`FailedBy`）。
- 行为变化：`Unsubscribe` 由「立即丢弃缓冲」改为「宽限收敛后结算」，
  宽限时间由 `DrainTimeout` 控制；需要旧的立即行为可将其置为负数。

## 本地验证

```bash
# 运行全部测试（含竞态检测）
go test ./... -race -v

# 运行本地演示：观察快/慢订阅者输出、游标恢复与守恒校验
go run ./cmd/gateway -n 30 -interval 50ms
```

测试覆盖三类场景，日志中打印输入条件与判定依据：

- `TestConcurrentSubscribeChurn`：8 个订阅者并发增删 + 持续发布，
  常驻订阅者收到的 Seq 必须严格等于 1..N（无重复、无漏投、无乱序）。
- `TestResumeFromCursor`：断开后携带 cursor 重连，从 cursor+1 继续；
  过期游标返回 `cursor_expired`，越界游标返回 `cursor_regression`。
- `TestSlowConsumerIsolation`：慢订阅者阻塞期间快订阅者收齐全部消息；
  慢订阅者发生可计数的 `Dropped`；放行后 `Delivered+Dropped+Failed == 发布总数`。
- `TestGracefulUnsubscribeDrain`：退订时缓冲中剩余消息在宽限期内全部处理完，
  账目 `Reason=drained`，最终游标正确，订阅者离开后账目仍可查询。
- `TestUnsubscribeDrainTimeout`：handler 阻塞导致宽限超时，排队与在途消息
  全部计为 `DroppedShutdown`（与 `DroppedBackpressure` 分别计数），
  结算后 handler 放行不改写已归档账目。
- `TestSessionReportFailureCategories`：`HandlerError` 按类别计入 `FailedBy`，
  普通错误归入 `handler_error`，`Failed == 各类别之和`。
- `TestNonPositiveDrainTimeout`：`DrainTimeout<0` 时退订立即结算，
  全部未处理消息计为 `DroppedShutdown`。
- `TestTopicStatsConservationUnderChurn`：6 个订阅者高频上下线 + 持续发布，
  聚合视图满足 `Dispatched == Delivered+Dropped+Failed`，
  已离开订阅者的贡献保留在聚合中。
