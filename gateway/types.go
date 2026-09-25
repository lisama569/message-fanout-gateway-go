// Package gateway 实现本地可运行的长连接消息分发网关。
package gateway

// Message 表示一条已发布的消息。
type Message struct {
	Topic   string
	Seq     uint64 // 主题内单调递增序号，从 1 开始
	Payload []byte
}

// Cursor 表示订阅者已确认的位置（最后一条已成功处理消息的 Seq）。
// Cursor 为 0 表示从头开始。
type Cursor uint64

// DropPolicy 定义订阅者缓冲满时的处理策略。
type DropPolicy int

const (
	// DropNewest 缓冲满时丢弃新消息（计入 Dropped），保证老消息不丢。
	DropNewest DropPolicy = iota
	// DropOldest 缓冲满时挤出最旧消息（计入 Dropped），保证新消息尽快到达。
	DropOldest
)

// Stats 记录单个订阅者的投递统计。
// 不变式：Delivered + Dropped + Failed == 应投递给该订阅者的消息总数。
type Stats struct {
	Delivered uint64 // 成功投递并被处理
	Dropped   uint64 // 因缓冲满按策略丢弃
	Failed    uint64 // 处理函数返回错误
}

// Total 返回三类计数之和。
func (s Stats) Total() uint64 { return s.Delivered + s.Dropped + s.Failed }
