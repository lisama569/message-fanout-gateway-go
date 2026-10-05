// Package gateway 实现本地可运行的长连接消息分发网关。
package gateway

import "time"

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
// 分类口径：Dropped == DroppedBackpressure + DroppedShutdown；
// Failed == FailedBy 各类别之和。
type Stats struct {
	Delivered uint64 // 成功投递并被处理
	Dropped   uint64 // 丢弃总数（背压 + 下线收敛）
	Failed    uint64 // 处理函数返回错误

	// DroppedBackpressure 因缓冲满按策略丢弃（慢消费者背压）。
	DroppedBackpressure uint64
	// DroppedShutdown 因订阅者下线收敛超时丢弃：
	// 宽限期内仍未处理（排队中或处理中）的消息。
	DroppedShutdown uint64
	// FailedBy 按类别细分的失败计数，键为失败类别。
	FailedBy map[string]uint64
}

// Total 返回三类计数之和。
func (s Stats) Total() uint64 { return s.Delivered + s.Dropped + s.Failed }

// clone 返回深拷贝（含 FailedBy 映射），用于快照输出。
func (s Stats) clone() Stats {
	out := s
	if s.FailedBy != nil {
		out.FailedBy = make(map[string]uint64, len(s.FailedBy))
		for k, v := range s.FailedBy {
			out.FailedBy[k] = v
		}
	}
	return out
}

// FailureCategoryHandler 是未归类处理失败的默认类别。
const FailureCategoryHandler = "handler_error"

// FailureCategorizer 处理函数返回的错误若实现该接口，
// 失败计数按 FailureCategory() 返回的类别归类，否则归入 FailureCategoryHandler。
type FailureCategorizer interface {
	FailureCategory() string
}

// HandlerError 带类别的处理失败，便于按类别统计与排查。
type HandlerError struct {
	Category string // 失败类别，空串按 FailureCategoryHandler 处理
	Err      error  // 底层错误
}

func (e *HandlerError) Error() string { return e.Err.Error() }
func (e *HandlerError) Unwrap() error { return e.Err }
func (e *HandlerError) FailureCategory() string {
	if e.Category == "" {
		return FailureCategoryHandler
	}
	return e.Category
}

// NewHandlerError 构造带类别的处理失败。
func NewHandlerError(category string, err error) *HandlerError {
	return &HandlerError{Category: category, Err: err}
}

// failureCategory 提取错误的失败类别。
func failureCategory(err error) string {
	if c, ok := err.(FailureCategorizer); ok {
		if cat := c.FailureCategory(); cat != "" {
			return cat
		}
	}
	return FailureCategoryHandler
}

// FinalizeReason 标识会话账目以何种方式结算。
type FinalizeReason int

const (
	// FinalizeDrained 宽限期内缓冲排空，全部消息正常处理完毕。
	FinalizeDrained FinalizeReason = iota
	// FinalizeTimeout 宽限期到期仍有未处理消息，剩余部分计为 DroppedShutdown。
	FinalizeTimeout
)

func (r FinalizeReason) String() string {
	switch r {
	case FinalizeDrained:
		return "drained"
	case FinalizeTimeout:
		return "drain_timeout"
	default:
		return "unknown"
	}
}

// SessionReport 是一次订阅会话下线后的最终账目，可事后查询。
type SessionReport struct {
	Topic        string
	SubscriberID string
	Stats        Stats        // 最终统计（含丢弃/失败分类明细）
	Cursor       Cursor       // 最后确认位置（最后一条成功处理消息的 Seq）
	Reason       FinalizeReason
	ClosedAt     time.Time
}

// TopicStats 是按主题聚合的运营视角统计。
// 聚合范围为该主题所有接入过的订阅者（含已离开）：
// 已离开的会话贡献其最终账目，活跃订阅者贡献其当前快照。
// 守恒口径：Dispatched == Delivered + Dropped + Failed。
type TopicStats struct {
	Topic               string
	Published           uint64 // 累计发布条数
	Dispatched          uint64 // 累计应投递条数（按订阅者逐个计入）
	Delivered           uint64
	Dropped             uint64
	DroppedBackpressure uint64
	DroppedShutdown     uint64
	Failed              uint64
	FailedBy            map[string]uint64
	ActiveSubscribers   int // 当前活跃订阅者数
	Sessions            int // 累计会话数（含已离开）
}
