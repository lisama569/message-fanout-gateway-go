// Package gateway 实现本地可运行的长连接消息分发网关。
package gateway

import "time"

// Seq 是主题内单调递增的消息序号，从 1 开始。
type Seq uint64

// Cursor 表示订阅者已确认（ack）的最后一条消息序号。
// 0 表示尚未确认任何消息。
type Cursor uint64

// Message 是一条待分发的消息。
type Message struct {
	Topic     string
	Seq       Seq
	Payload   []byte
	Published time.Time
}

// Reason 是可区分的投递/拒绝原因，用于错误归类与统计。
type Reason string

const (
	// ReasonCursorExpired 请求的游标太旧，对应消息已超出保留窗口。
	ReasonCursorExpired Reason = "cursor_expired"
	// ReasonCursorRegression 请求的游标超过已发布的最新序号（发生倒退/数据不一致）。
	ReasonCursorRegression Reason = "cursor_regression"
	// ReasonBufferOverflow 订阅者缓冲已满，按策略丢弃。
	ReasonBufferOverflow Reason = "buffer_overflow"
	// ReasonSubscriberClosed 投递时订阅者已关闭。
	ReasonSubscriberClosed Reason = "subscriber_closed"
	// ReasonHandlerError 订阅者处理函数返回错误。
	ReasonHandlerError Reason = "handler_error"
)

// Outcome 是一条消息对某个订阅者的投递结果分类。
type Outcome int

const (
	OutcomeDelivered Outcome = iota // 处理成功
	OutcomeDropped                  // 缓冲满，按策略丢弃
	OutcomeFailed                   // 处理失败（handler 报错或订阅者关闭）
)

// Config 网关可配置项。
type Config struct {
	// Retention 每个主题保留的最近消息条数（用于游标恢复）。
	Retention int
	// SubscriberBuffer 每个订阅者的缓冲队列容量。
	SubscriberBuffer int
}

// DefaultConfig 返回本地运行的默认配置。
func DefaultConfig() Config {
	return Config{Retention: 1024, SubscriberBuffer: 64}
}
