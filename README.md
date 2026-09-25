# message-fanout-gateway-go

本地可运行的长连接消息分发网关。内置可控事件源（`Source`）模拟上游变更，
在多订阅者场景下保证消息顺序、中断恢复与慢消费者处理具有一致且可解释的结果。

## 结构

```
cmd/gateway/main.go        本地演示入口
internal/gateway/
  types.go                 Message / Cursor / Reason / Config 等基础类型
  errors.go                带可判定原因的 *Error 与 IsReason
  broker.go                Broker：主题日志、保留窗口、游标校验、扇出
  subscription.go          Subscription：独立缓冲、派发循环、ack 游标、统计
  source.go                Source：可控事件源（手动 Emit / 节拍 Run）
  broker_test.go           并发增删 / 中断恢复 / 慢消费者 / 错误归类测试
```

## 分发语义

- 同一主题的消息按发布顺序分配单调递增序号（从 1 开始），并按序投递给全部订阅者。
- 扇出时在主题锁内取订阅者快照：快照内的订阅者恰好收到本条消息一次，
  快照外（已退订或尚未订阅）的不会收到。订阅/退订与发布被主题锁串行化，
  因此并发增删不会出现重复投递或漏投。
- 每条消息对每个订阅者的投递结果必为以下三类之一，并分别计数：
  - `Delivered`：订阅者处理函数返回 nil；
  - `Dropped`：订阅者缓冲已满，按策略丢弃（原因 `buffer_overflow`）；
  - `Failed`：处理函数返回错误（原因 `handler_error`）或投递时订阅者已关闭
    （原因 `subscriber_closed`）。
- 不变式：在系统静止后，`published × 扇出订阅者数 == delivered + dropped + failed`，
  不允许消息凭空增加或消失。

## 中断恢复（游标语义）

- `Cursor` 是订阅者已确认的最后一条消息序号；`Subscription.Cursor()` 可随时读取。
- 重新接入时携带该游标调用 `Subscribe(topic, cursor, handler)`，
  网关从保留窗口中补发序号 `> cursor` 的全部消息，再进入实时流。
- `cursor = 0` 表示新订阅者，只接收订阅之后的新消息。
- 拒绝规则（返回 `*gateway.Error`，可用 `IsReason` 判定）：
  - `cursor > latest` → `cursor_regression`：请求的位置超过已发布的最新序号，
    视为位置倒退/数据不一致，明确拒绝，绝不从中间继续或静默重放；
  - `cursor + 1 < base`（保留窗口最旧序号）→ `cursor_expired`：
    所请求位置已超出保留范围，明确拒绝。

## 慢消费者策略（背压）

- 每个订阅者拥有独立的缓冲队列（容量 `SubscriberBuffer`）与专属派发 goroutine，
  发布路径只做**非阻塞**入队，因此单个订阅者过慢不会阻塞或拖慢其它订阅者。
- 缓冲已满时采用 **DropNewest** 策略：丢弃当前这条消息（仅对该订阅者），
  计入 `Dropped`，原因记为 `buffer_overflow`。该策略是确定的：
  丢弃与否只取决于入队瞬间该订阅者缓冲是否已满。
- 订阅者可通过序号是否连续检测自身发生了丢弃，并携带最后确认游标重新订阅
  以恢复（若游标仍在保留窗口内）。

## 可配置项

| 配置项 | 含义 | 默认值 |
| --- | --- | --- |
| `Config.Retention` | 每主题保留的最近消息条数（游标恢复窗口） | 1024 |
| `Config.SubscriberBuffer` | 每订阅者缓冲队列容量 | 64 |

## 本地验证

```bash
# 运行单元测试（日志会打印输入条件与判定依据）
go test ./internal/gateway -v -count=1

# 竞态检测
go test ./internal/gateway -race -count=1

# 运行本地演示（快/慢订阅者 + 统计不变式 + 游标恢复）
go run ./cmd/gateway -duration 3s -interval 5ms -slow-every 20ms
```

测试覆盖三类场景：

- `TestConcurrentSubscribeUnsubscribe`：并发订阅增删下无重复、无漏投；
- `TestResumeFromCursor`：中断后按游标精确恢复，过旧/越界游标被按原因拒绝；
- `TestSlowConsumerIsolation`：慢消费者被隔离并触发 DropNewest，
  快消费者不受影响，全局统计满足不变式；
- `TestErrorClassification`：错误原因可区分（`IsReason` / `errors.As`）。
