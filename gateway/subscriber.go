package gateway

import (
	"sync"
	"time"
)

// Handler 处理投递给订阅者的消息；返回错误计入 Failed。
type Handler func(Message) error

// Subscriber 表示一个订阅者，拥有独立缓冲，慢消费不影响他人。
type Subscriber struct {
	id     string
	topic  string
	policy DropPolicy
	buf    chan Message
	done   chan struct{}

	// drainTimeout 是下线宽限时间；<=0 表示不给予宽限，立即结算。
	drainTimeout time.Duration
	// onFinalize 在账目结算完成时回调（由 Broker 注入，用于归档）。
	onFinalize func(SessionReport)

	mu        sync.Mutex
	stats     Stats
	cursor    Cursor
	closed    bool
	finalized bool
	inFlight  int // 正在 handler 中处理的消息数
	closeOne  sync.Once
}

func newSubscriber(id, topic string, bufSize int, policy DropPolicy, drainTimeout time.Duration) *Subscriber {
	if bufSize < 1 {
		bufSize = 1
	}
	return &Subscriber{
		id:           id,
		topic:        topic,
		policy:       policy,
		buf:          make(chan Message, bufSize),
		done:         make(chan struct{}),
		drainTimeout: drainTimeout,
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
		s.addDrop(true)
		select {
		case s.buf <- m:
			return true
		default:
			// 极端并发下仍未放入，按 DropNewest 处理
			s.addDrop(true)
			return false
		}
	default: // DropNewest
		s.addDrop(true)
		return false
	}
}

// addDrop 计入一次丢弃。backpressure=true 表示缓冲满背压丢弃，
// false 表示下线收敛超时丢弃。已结算后不重复计数。
func (s *Subscriber) addDrop(backpressure bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized {
		return
	}
	s.stats.Dropped++
	if backpressure {
		s.stats.DroppedBackpressure++
	} else {
		s.stats.DroppedShutdown++
	}
}

// run 消费缓冲并调用 handler，直到 Close。
// 处理成功：Delivered++ 并推进 cursor；处理失败：Failed++（cursor 不推进）。
// Close 后进入 drain：在宽限期内把缓冲中剩余消息处理完，然后结算账目。
func (s *Subscriber) run(h Handler) {
	for {
		select {
		case <-s.done:
			s.drain(h)
			return
		case m := <-s.buf:
			s.handle(h, m)
		}
	}
}

// drain 下线收敛：继续处理缓冲中剩余的消息，直到排空。
// 宽限期由 Close 时启动的强制结算定时器兜底；超时时 forceFinalize
// 会把仍排队/仍在处理的消息计为 DroppedShutdown。
func (s *Subscriber) drain(h Handler) {
	for {
		select {
		case m := <-s.buf:
			s.handle(h, m)
		default:
			s.finalize(FinalizeDrained)
			return
		}
	}
}

func (s *Subscriber) handle(h Handler, m Message) {
	s.mu.Lock()
	s.inFlight++
	s.mu.Unlock()
	var err error
	if h != nil {
		err = h(m)
	}
	s.record(m, err)
}

func (s *Subscriber) record(m Message, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.finalized {
		// 宽限期已截止：该消息在结算时已按在途计入 DroppedShutdown，
		// 此处不再重复计数，保证每条消息只有一个归属。
		return
	}
	if err != nil {
		s.stats.Failed++
		if s.stats.FailedBy == nil {
			s.stats.FailedBy = make(map[string]uint64)
		}
		s.stats.FailedBy[failureCategory(err)]++
		return
	}
	s.stats.Delivered++
	s.cursor = Cursor(m.Seq)
}

// finalize 结算账目并回调归档。幂等，先调用者生效。
func (s *Subscriber) finalize(reason FinalizeReason) {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return
	}
	s.finalized = true
	report := SessionReport{
		Topic:        s.topic,
		SubscriberID: s.id,
		Stats:        s.stats.clone(),
		Cursor:       s.cursor,
		Reason:       reason,
		ClosedAt:     time.Now(),
	}
	cb := s.onFinalize
	s.mu.Unlock()
	if cb != nil {
		cb(report)
	}
}

// forceFinalize 宽限期到期的强制结算：仍滞留在缓冲中的消息全部计为
// DroppedShutdown；正在处理中的消息在其 handler 返回时由 record 归为
// DroppedShutdown（因 finalized 已置位）。
// 拉取与计数在持锁状态下原子完成，与 drain 并发时每条消息只归属一方。
func (s *Subscriber) forceFinalize() {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return
	}
	s.finalized = true
	// 仍在 handler 中处理的消息在结算时刻即归为下线丢弃，
	// 避免 handler 晚些返回时账目已归档而漏记。
	s.stats.Dropped += uint64(s.inFlight)
	s.stats.DroppedShutdown += uint64(s.inFlight)
	for {
		select {
		case <-s.buf:
			s.stats.Dropped++
			s.stats.DroppedShutdown++
		default:
			report := SessionReport{
				Topic:        s.topic,
				SubscriberID: s.id,
				Stats:        s.stats.clone(),
				Cursor:       s.cursor,
				Reason:       FinalizeTimeout,
				ClosedAt:     time.Now(),
			}
			cb := s.onFinalize
			s.mu.Unlock()
			if cb != nil {
				cb(report)
			}
			return
		}
	}
}

// Close 关闭订阅者：停止接收新消息，进入宽限收敛。
// 宽限时间（DrainTimeout）内缓冲排空则正常结算；到期未排空强制结算，
// 剩余消息计为 DroppedShutdown。DrainTimeout<=0 表示不给予宽限，立即结算。
func (s *Subscriber) Close() {
	s.closeOne.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
		if s.drainTimeout <= 0 {
			go s.forceFinalize()
			return
		}
		time.AfterFunc(s.drainTimeout, s.forceFinalize)
	})
}

// Stats 返回当前统计快照（含失败分类明细的深拷贝）。
func (s *Subscriber) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats.clone()
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
