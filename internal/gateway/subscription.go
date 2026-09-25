package gateway

import "sync"

// Stats 是投递结果的分类统计。
type Stats struct {
	Published uint64
	Delivered uint64
	Dropped   uint64
	Failed    uint64
}

// Subscription 是一个订阅句柄：内置独立缓冲队列与派发循环，
// 单个订阅者处理过慢只会触发自身的丢弃策略，不会阻塞其它订阅者。
type Subscription struct {
	topic  string
	cursor Cursor // 上次确认位置（订阅建立时的基线）

	ch      chan Message
	handler Handler
	broker  *Broker
	t       *topic

	mu     sync.Mutex
	closed bool
	stats  Stats
	done   chan struct{}
}

func newSubscription(topic string, cursor Cursor, buf int, h Handler, b *Broker, t *topic) *Subscription {
	s := &Subscription{
		topic:   topic,
		cursor:  cursor,
		ch:      make(chan Message, buf),
		handler: h,
		broker:  b,
		t:       t,
		done:    make(chan struct{}),
	}
	go s.pump()
	return s
}

// replay 在订阅建立后同步补发历史消息（保持序号顺序）。
// 缓冲满时同样按丢弃策略处理，保证语义一致。
func (s *Subscription) replay(backlog []Message) {
	for _, m := range backlog {
		s.enqueue(m)
	}
}

// enqueue 由扇出调用。缓冲满时按 DropNewest 策略丢弃本条消息并计数，
// 该策略只影响本订阅者，发布者与其它订阅者不被阻塞。
func (s *Subscription) enqueue(m Message) {
	// 整个非阻塞发送在锁内完成，与 Unsubscribe 的 close 互斥，
	// 避免向已关闭 channel 发送导致 panic。
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.stats.Failed++
		s.broker.addFailed()
		return
	}
	select {
	case s.ch <- m:
	default:
		// 策略：DropNewest —— 缓冲已满时丢弃当前消息，原因记为 ReasonBufferOverflow。
		s.stats.Dropped++
		s.broker.addDropped()
	}
}

// pump 是订阅者专属的派发循环，按序处理缓冲中的消息。
func (s *Subscription) pump() {
	defer close(s.done)
	for m := range s.ch {
		if err := s.handler(m); err != nil {
			s.mu.Lock()
			s.stats.Failed++
			s.mu.Unlock()
			s.broker.addFailed()
			continue
		}
		s.mu.Lock()
		s.stats.Delivered++
		s.cursor = Cursor(m.Seq)
		s.mu.Unlock()
		s.broker.addDelivered()
	}
}

// Topic 返回订阅的主题名。
func (s *Subscription) Topic() string { return s.topic }

// Cursor 返回当前已确认的位置（最后一条处理成功的消息序号）。
func (s *Subscription) Cursor() Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

// Unsubscribe 取消订阅并释放资源；幂等。
func (s *Subscription) Unsubscribe() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.ch)
	s.mu.Unlock()

	s.broker.remove(s.t, s)
	<-s.done // 等待 pump 排空缓冲中的剩余消息
}

// Stats 返回该订阅的投递统计。
func (s *Subscription) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
