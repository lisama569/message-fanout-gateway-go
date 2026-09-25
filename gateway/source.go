package gateway

import (
	"fmt"
	"sync"
)

// Source 是可控的内置事件源，模拟上游变更。
// 支持手动逐条注入（Emit）与轮询批量注入（EmitN），便于测试与本地演示。
type Source struct {
	broker *Broker
	topics []string

	mu  sync.Mutex
	seq int // 已注入事件总数
}

func NewSource(b *Broker, topics ...string) *Source {
	return &Source{broker: b, topics: topics}
}

// Emit 手动向指定主题注入一条事件，返回分配的 Seq。
func (s *Source) Emit(topic string, payload string) (uint64, error) {
	s.mu.Lock()
	s.seq++
	s.mu.Unlock()
	return s.broker.Publish(topic, []byte(payload))
}

// EmitN 依次向各主题轮询注入 n 条事件，payload 为 "event-<i>"。
func (s *Source) EmitN(n int) {
	for i := 0; i < n; i++ {
		topic := s.topics[i%len(s.topics)]
		_, _ = s.Emit(topic, fmt.Sprintf("event-%d", i))
	}
}

// Emitted 返回已注入事件总数。
func (s *Source) Emitted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}
