package gateway

import "fmt"

// Error 是带可判定原因的网关错误。
type Error struct {
	Reason Reason
	Topic  string
	Cursor Cursor
	Detail string
}

func (e *Error) Error() string {
	return fmt.Sprintf("gateway: topic=%q reason=%s cursor=%d: %s", e.Topic, e.Reason, e.Cursor, e.Detail)
}

// IsReason 判断 err 是否为指定原因的网关错误。
func IsReason(err error, r Reason) bool {
	ge, ok := err.(*Error)
	return ok && ge.Reason == r
}
