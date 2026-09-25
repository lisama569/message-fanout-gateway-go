package gateway

import (
	"fmt"
	"sync"
	"time"
)

// Source 是可控事件源，模拟上游变更，将事件按顺序发布到 Broker。
// 支持两种模式：
//   - Emit：手动逐条产生，完全可控，用于测试；
//   - Run：按固定节拍自动产生，用于本地演示，可用 Stop 停止。
type Source struct {
	broker *Broker
	topic  string

	mu      sync.Mutex
	counter uint64
	stop    chan struct{}
	stopped bool
}

// NewSource 创建一个绑定到指定主题的事件源。
func NewSource(b *Broker, topic string) *Source {
	return &Source{broker: b, topic: topic}
}

// Emit 同步产生一条事件（可控、可判定顺序），返回分配的序号。
func (s *Source) Emit(payload string) Seq {
	return s.broker.Publish(s.topic, []byte(payload))
}

// Run 按 interval 节拍自动产生事件，直到 Stop 被调用。
// 产生的事件内容为 "event-<n>"，n 从 1 单调递增，便于核对顺序。
func (s *Source) Run(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		select {
		case <-s.stopCh():
			return
		case <-t.C:
			s.mu.Lock()
			s.counter++
			n := s.counter
			s.mu.Unlock()
			s.Emit(fmt.Sprintf("event-%d", n))
		}
	}
}

func (s *Source) stopCh() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop == nil {
		s.stop = make(chan struct{})
	}
	return s.stop
}

// Stop 停止自动节拍模式；幂等。
func (s *Source) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.stopped = true
		if s.stop != nil {
			close(s.stop)
		}
	}
}
