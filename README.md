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
  - `Dropped`：缓冲满时按策略丢弃（见下），或退订宽限期到期仍未处理（见「优雅下线」）。

### 丢弃与失败的分类口径

`Stats` 在总计之外提供分类计数，事后可区分一条消息的去向：

```
Dropped == DroppedBackpressure + DroppedDrain
Failed  == FailedOnline + FailedDrain
```

| 分类字段 | 含义 |
|---|---|
| `DroppedBackpressure` | 缓冲满按策略丢弃（背压扛不住） |
| `DroppedDrain` | 退订宽限期到期仍未处理完（下线收敛超时），含在途那一条 |
| `FailedOnline` | 在线期间处理函数返回错误 |
| `FailedDrain` | 退订宽限期内处理函数返回错误 |

## 优雅下线（退订收敛）

`Unsubscribe` 触发优雅关闭而非立即丢弃：订阅者停止接收新消息后，
缓冲中排队与正在处理的消息在 `DrainTimeout` 宽限时间内继续处理：

- 宽限期内处理成功计入 `Delivered`（cursor 照常推进）；
- 宽限期内处理报错计入 `Failed`（分类 `FailedDrain`）；
- 宽限到期仍未处理完的（**含正在处理的那一条**）计入 `Dropped`（分类 `DroppedDrain`），
  不会因下线凭空消失；到期后才返回的处理结果不再入账，账目定案后不可篡改。

`DrainTimeout` 边界取值：

| 取值 | 行为 |
|---|---|
| `> 0` | 宽限该时长；缓冲排空即提前定案，到期强制收敛 |
| `== 0` | **无宽限**：退订即刻定案，在途与缓冲消息立即计为 `DroppedDrain` |
| `< 0` | 未设置/非法，回落到默认值 `DefaultDrainTimeout = 2s` |

## 会话账目与主题聚合（审计）

- `Broker.SessionReport(topic, id)`：查询指定订阅者**最近一次会话**的最终账目
  （`Stats` 全部分类计数 + 最后确认位置 `Cursor`）。会话下线收敛后仍可查询；
  落账异步于定案（`Subscriber.Finalized()` 关闭后由后台回调写入），定案后短暂窗口内可轮询等待。
- `Broker.TopicStats(topic)`：主题维度聚合视角——`Published` 累计发布量，
  以及所有接入过的订阅者合计的 `Delivered/Dropped/Failed`（含全部分类计数）。
  聚合范围 = 活跃订阅者 + 收敛中订阅者 + 已离开会话的累计，
  已离开订阅者的贡献**永久保留**，且退订收敛的任意时刻账目连续、无空窗。

核对口径：对从 0 订阅、覆盖主题全生命周期的单个会话，
`Published == Delivered + Dropped + Failed` 恒成立；
多订阅者场景下聚合总量 = 各会话报告逐项之和（每个会话对自己应投的消息集合守恒）。

## 慢消费者策略（背压）

每个订阅者拥有容量为 `SubscriberBuffer` 的独立缓冲。缓冲满时按 `Policy` 处理：

| 策略 | 行为 |
|---|---|
| `DropNewest`（默认） | 丢弃新到的消息，计入 `Dropped`；保证老消息不丢 |
| `DropOldest` | 挤出缓冲中最旧的一条计入 `Dropped`，放入新消息；保证新消息尽快到达 |

被丢弃的消息**始终计入 `Dropped`**，不会凭空消失；任何路径都不会凭空增加消息。

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
| `DrainTimeout` | 2s（配负数时回落） | 退订宽限时间；`0` = 立即收敛，`<0` = 取默认值 |

命令行演示参数：`go run ./cmd/gateway -h`
（`-retain` / `-buffer` / `-n` / `-interval` / `-drain`）。

## 兼容范围

- `Stats` 原有的 `Delivered/Dropped/Failed` 字段语义不变（仍为总计），
  新增的分类字段为纯增量，原有守恒不变式与既有用例不受影响。
- `Unsubscribe` 由「立即关闭」变为「优雅关闭」：退订后订阅者进入收敛流程，
  残余消息按上述口径入账而非静默消失；`DrainTimeout` 零值（如未显式配置的旧代码）
  语义为立即收敛，残余消息会被计为 `DroppedDrain`——账目更完整，
  但与上一版「残余消息不计数」的行为存在可观察差异。
- 新增 API（`SessionReport` / `TopicStats` / `Subscriber.Finalized` / `Subscriber.Report`）
  均为纯增量，不影响既有接口。

## 本地验证

```bash
# 运行全部测试（含竞态检测）
go test ./... -race -v

# 运行本地演示：观察快/慢订阅者输出、游标恢复、优雅下线收敛与守恒校验
go run ./cmd/gateway -n 30 -interval 50ms -drain 2s
```

测试覆盖以下场景，日志中打印输入条件与判定依据：

- `TestConcurrentSubscribeChurn`：8 个订阅者并发增删 + 持续发布，
  常驻订阅者收到的 Seq 必须严格等于 1..N（无重复、无漏投、无乱序）。
- `TestResumeFromCursor`：断开后携带 cursor 重连，从 cursor+1 继续；
  过期游标返回 `cursor_expired`，越界游标返回 `cursor_regression`。
- `TestSlowConsumerIsolation`：慢订阅者阻塞期间快订阅者收齐全部消息；
  慢订阅者发生可计数的 `Dropped`；放行后 `Delivered+Dropped+Failed == 发布总数`。
- `TestGracefulUnsubscribeDrain`：退订后宽限期内积压消息被处理完才定案；
  定案前账目不可查，定案后 `SessionReport` 可查且 `Delivered`、cursor 符合预期。
- `TestDrainTimeoutDrops`：handler 卡死时宽限到期强制收敛，
  在途+缓冲全部计为 `DroppedDrain`；定案后 handler 再返回也不改账。
- `TestDrainTimeoutZeroAndNegative`：`DrainTimeout=0` 立即收敛；
  负数回落默认 2s（宽限期内不定案，放行后正常收敛）。
- `TestDrainFailureClassification`：在线失败计 `FailedOnline`、
  宽限内失败计 `FailedDrain`，总计与分类自洽。
- `TestDropReasonSeparation`：同一会话中背压丢弃（`DroppedBackpressure`）
  与收敛丢弃（`DroppedDrain`）分开计数，合计与发布总量对得上。
- `TestAggregateConservationUnderChurn`：8×20 个独立会话高频上下线 + 持续发布，
  全部收敛后 `TopicStats` 聚合 == 161 个 `SessionReport` 逐项之和，
  常驻会话 `Total == Published`，分类口径全程自洽。
