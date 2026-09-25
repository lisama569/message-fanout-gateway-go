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
  - `Dropped`：缓冲满时按策略丢弃（见下）。

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

命令行演示参数：`go run ./cmd/gateway -h`
（`-retain` / `-buffer` / `-n` / `-interval`）。

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
