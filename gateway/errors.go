package gateway

import "fmt"

// ErrKind 可区分的错误类别。
type ErrKind int

const (
	ErrKindNone ErrKind = iota
	// ErrKindCursorExpired 请求的游标早于日志保留范围，无法恢复。
	ErrKindCursorExpired
	// ErrKindCursorRegression 请求的游标超过当前最新位置（倒退/越界）。
	ErrKindCursorRegression
	// ErrKindTopicNotFound 主题不存在。
	ErrKindTopicNotFound
	// ErrKindSubscriberClosed 订阅者已关闭。
	ErrKindSubscriberClosed
)

// Error 带类别的错误，便于调用方判定原因。
type Error struct {
	Kind    ErrKind
	Topic   string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("gateway: topic=%q kind=%s: %s", e.Topic, e.Kind, e.Message)
}

func (k ErrKind) String() string {
	switch k {
	case ErrKindCursorExpired:
		return "cursor_expired"
	case ErrKindCursorRegression:
		return "cursor_regression"
	case ErrKindTopicNotFound:
		return "topic_not_found"
	case ErrKindSubscriberClosed:
		return "subscriber_closed"
	default:
		return "none"
	}
}
