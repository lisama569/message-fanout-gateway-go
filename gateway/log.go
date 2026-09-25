package gateway

import (
	"fmt"
	"sync"
)

// topicLog 是单主题的保序消息日志，带固定保留窗口。
// Seq 从 1 开始单调递增；超出保留窗口的旧消息被裁剪，不可再用于恢复。
type topicLog struct {
	mu      sync.RWMutex
	msgs    []Message // 按 Seq 升序，msgs[i].Seq == baseSeq+i
	baseSeq uint64    // msgs[0] 对应的 Seq（无消息时为 nextSeq）
	nextSeq uint64    // 下一条将分配的 Seq
	retain  int       // 最大保留条数
}

func newTopicLog(retain int) *topicLog {
	if retain < 1 {
		retain = 1
	}
	return &topicLog{retain: retain, nextSeq: 1, baseSeq: 1}
}

// append 追加一条消息并返回该消息（含分配的 Seq）。
func (l *topicLog) append(topic string, payload []byte) Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := Message{Topic: topic, Seq: l.nextSeq, Payload: payload}
	l.nextSeq++
	l.msgs = append(l.msgs, m)
	if len(l.msgs) > l.retain {
		// 裁剪最旧的消息，推进 baseSeq
		over := len(l.msgs) - l.retain
		l.msgs = append([]Message(nil), l.msgs[over:]...)
		l.baseSeq += uint64(over)
	}
	return m
}

// from 返回 Seq >= start 的全部消息快照。
// start 超出保留范围时返回带类别的错误：
//   - start < baseSeq: ErrKindCursorExpired（位置已被裁剪）
//   - start > nextSeq: ErrKindCursorRegression（位置在未来，发生倒退/越界）
func (l *topicLog) from(topic string, start uint64) ([]Message, *Error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if start < l.baseSeq {
		return nil, &Error{Kind: ErrKindCursorExpired, Topic: topic,
			Message: fmt.Sprintf("requested seq %d < earliest retained seq %d", start, l.baseSeq)}
	}
	if start > l.nextSeq {
		return nil, &Error{Kind: ErrKindCursorRegression, Topic: topic,
			Message: fmt.Sprintf("requested seq %d > next seq %d", start, l.nextSeq)}
	}
	out := make([]Message, 0, l.nextSeq-start)
	for _, m := range l.msgs {
		if m.Seq >= start {
			out = append(out, m)
		}
	}
	return out, nil
}

// latest 返回当前最新 Seq（无消息时为 0）。
func (l *topicLog) latest() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextSeq - 1
}

// earliest 返回当前保留的最早 Seq。
func (l *topicLog) earliest() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseSeq
}
