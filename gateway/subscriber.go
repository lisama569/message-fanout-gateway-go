package gateway

import "sync"

// Handler 处理投递给订阅者的消息；返回错误计入 Failed。
type Handler func(Message) error

// Subscriber 表示一个订阅者，拥有独立缓冲，慢消费不影响他人。
type Subscriber struct {
	id     string
	topic  string
	policy DropPolicy
	buf    chan Message
	done   chan struct{}

	mu       sync.Mutex
	stats    Stats
	cursor   Cursor
	closed   bool
	closeOne sync.Once
}

func newSubscriber(id, topic string, bufSize int, policy DropPolicy) *Subscriber {
	if bufSize < 1 {
		bufSize = 1
	}
	return &Subscriber{
		id:     id,
		topic:  topic,
		policy: policy,
		buf:    make(chan Message, bufSize),
		done:   make(chan struct{}),
	}
}

// enqueue 非阻塞投递。缓冲满时按策略处理：
//   - DropNewest：丢弃新消息，计入 Dropped，返回 false；
//   - DropOldest：挤出缓冲中最旧的一条（计入 Dropped），放入新消息，返回 true。
//
// 无论哪种策略，被丢弃的消息都计入 Dropped，保证
// Delivered + Dropped + Failed == 应投递总数 的不变式。
func (s *Subscriber) enqueue(m Message) bool {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return false
	}
	select {
	case s.buf <- m:
		return true
	default:
	}
	switch s.policy {
	case DropOldest:
		select {
		case <-s.buf: // 挤出最旧
		default:
		}
		s.mu.Lock()
		s.stats.Dropped++
		s.mu.Unlock()
		select {
		case s.buf <- m:
			return true
		default:
			// 极端并发下仍未放入，按 DropNewest 处理
			s.mu.Lock()
			s.stats.Dropped++
			s.mu.Unlock()
			return false
		}
	default: // DropNewest
		s.mu.Lock()
		s.stats.Dropped++
		s.mu.Unlock()
		return false
	}
}

// run 消费缓冲并调用 handler，直到 Close。
// 处理成功：Delivered++ 并推进 cursor；处理失败：Failed++（cursor 不推进）。
func (s *Subscriber) run(h Handler) {
	for {
		select {
		case <-s.done:
			return
		case m := <-s.buf:
			if h == nil {
				s.record(m, nil)
				continue
			}
			s.record(m, h(m))
		}
	}
}

func (s *Subscriber) record(m Message, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.stats.Failed++
		return
	}
	s.stats.Delivered++
	s.cursor = Cursor(m.Seq)
}

// Close 关闭订阅者，停止消费。
func (s *Subscriber) Close() {
	s.closeOne.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
}

// Stats 返回当前统计快照。
func (s *Subscriber) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Cursor 返回当前已确认位置。
func (s *Subscriber) Cursor() Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

// ID 返回订阅者标识。
func (s *Subscriber) ID() string { return s.id }

// Topic 返回订阅主题。
func (s *Subscriber) Topic() string { return s.topic }
