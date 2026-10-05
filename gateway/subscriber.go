package gateway

import (
	"sync"
	"time"
)

// Handler 处理投递给订阅者的消息；返回错误计入 Failed。
type Handler func(Message) error

// Subscriber 表示一个订阅者，拥有独立缓冲，慢消费不影响他人。
//
// 生命周期：online -> (Close) -> draining（宽限收敛） -> finalized（账目定案）。
// 关闭后不再接受新消息；缓冲中排队与正在处理的消息在宽限时间内继续处理：
//   - 宽限期内处理成功计入 Delivered，处理报错计入 FailedDrain；
//   - 宽限到期仍未处理完的（含在途那一条）计入 DroppedDrain，不凭空消失；
//   - DrainTimeout <= 0 表示无宽限：关闭即刻定案，残余消息立即计为 DroppedDrain。
type Subscriber struct {
	id     string
	topic  string
	policy DropPolicy
	buf    chan Message
	done   chan struct{}

	drainTimeout time.Duration // 退订宽限时间；<=0 表示无宽限，立即收敛
	finalizeCh   chan struct{} // 收敛完成（账目定案）时关闭

	mu         sync.Mutex
	stats      Stats
	cursor     Cursor
	closed     bool        // 已收到关闭请求，不再接受新消息
	draining   bool        // 处于宽限收敛阶段
	inflight   bool        // 有一条消息正在处理中
	finalized  bool        // 账目已定案，后续处理结果不再入账
	drainTimer *time.Timer // 宽限看门狗，到期强制收敛
	closeOne   sync.Once
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
		finalizeCh:   make(chan struct{}),
	}
}

// enqueue 非阻塞投递。缓冲满时按策略处理：
//   - DropNewest：丢弃新消息，计入 DroppedBackpressure，返回 false；
//   - DropOldest：挤出缓冲中最旧的一条（计入 DroppedBackpressure），放入新消息，返回 true。
//
// 无论哪种策略，被丢弃的消息都计入 Dropped（分类为背压丢弃），保证
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
		s.recordBackpressureDrop()
		select {
		case s.buf <- m:
			return true
		default:
			// 极端并发下仍未放入，按 DropNewest 处理
			s.recordBackpressureDrop()
			return false
		}
	default: // DropNewest
		s.recordBackpressureDrop()
		return false
	}
}

func (s *Subscriber) recordBackpressureDrop() {
	s.mu.Lock()
	s.stats.Dropped++
	s.stats.DroppedBackpressure++
	s.mu.Unlock()
}

// run 消费缓冲并调用 handler；收到关闭信号后进入宽限收敛。
// 每轮先非阻塞检查 done：关闭信号一旦发出即进入收敛阶段，
// 保证"在线/宽限"的阶段边界确定，后续消息都按宽限期口径入账。
func (s *Subscriber) run(h Handler) {
	for {
		select {
		case <-s.done:
			s.drainLoop(h)
			return
		default:
		}
		select {
		case <-s.done:
			s.drainLoop(h)
			return
		case m := <-s.buf:
			s.process(h, m)
		}
	}
}

// drainLoop 宽限收敛：继续处理缓冲中剩余消息，直到排空（收敛完成）
// 或宽限看门狗到期由 forceFinalize 强制定案。
// 关闭后不再有新消息进入缓冲，因此缓冲见底即收敛完成。
func (s *Subscriber) drainLoop(h Handler) {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	for {
		select {
		case m := <-s.buf:
			s.process(h, m)
		default:
			s.finalize()
			return
		}
	}
}

// process 处理一条消息并入账。
// 处理成功：Delivered++ 并推进 cursor；处理失败：Failed++（按所处阶段分类，cursor 不推进）。
// 若账目已定案（宽限期外），消息不再调用 handler，直接补记为 DroppedDrain，
// 保证任何路径下消息都不凭空消失。
func (s *Subscriber) process(h Handler, m Message) {
	s.mu.Lock()
	if s.finalized {
		// 宽限期外才轮到处理：该消息未被 forceFinalize 清扫到（已从缓冲取出），
		// 在此补记为下线收敛丢弃。
		s.stats.Dropped++
		s.stats.DroppedDrain++
		s.mu.Unlock()
		return
	}
	s.inflight = true
	draining := s.draining
	s.mu.Unlock()

	var err error
	if h != nil {
		err = h(m)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight = false
	if s.finalized {
		// 处理期间宽限到期，forceFinalize 已把该在途消息计为 DroppedDrain，不再重复入账
		return
	}
	if err != nil {
		s.stats.Failed++
		if draining {
			s.stats.FailedDrain++
		} else {
			s.stats.FailedOnline++
		}
		return
	}
	s.stats.Delivered++
	s.cursor = Cursor(m.Seq)
}

// finalize 正常定案：清扫缓冲残余（防御性，正常流程下已排空），关闭 finalizeCh。
func (s *Subscriber) finalize() {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return
	}
	s.finalized = true
	n := s.sweepLocked()
	if s.drainTimer != nil {
		s.drainTimer.Stop()
	}
	s.mu.Unlock()
	if n > 0 {
		s.recordDrainDrops(n)
	}
	close(s.finalizeCh)
}

// forceFinalize 宽限到期强制定案：在途那一条与缓冲中残余消息全部计为 DroppedDrain。
// 在途消息的处理函数若之后返回，其结果不再入账（process 中检查 finalized）。
func (s *Subscriber) forceFinalize() {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return
	}
	s.finalized = true
	n := s.sweepLocked()
	if s.inflight {
		n++
	}
	s.mu.Unlock()
	if n > 0 {
		s.recordDrainDrops(n)
	}
	close(s.finalizeCh)
}

// sweepLocked 排空缓冲并返回清扫到的条数。调用方须持有 s.mu。
// 与 run goroutine 的接收互斥：每条消息只会被一方取到，不会重复计数；
// 被 run 先取走的消息会在 process 中按 finalized 补记。
func (s *Subscriber) sweepLocked() uint64 {
	var n uint64
	for {
		select {
		case <-s.buf:
			n++
		default:
			return n
		}
	}
}

func (s *Subscriber) recordDrainDrops(n uint64) {
	s.mu.Lock()
	s.stats.Dropped += n
	s.stats.DroppedDrain += n
	s.mu.Unlock()
}

// Close 关闭订阅者：停止接受新消息，并在宽限时间内收敛剩余账目。
// DrainTimeout > 0：启动宽限看门狗，缓冲排空或到期后定案；
// DrainTimeout <= 0：无宽限，立即定案，残余消息计为 DroppedDrain。
func (s *Subscriber) Close() {
	s.closeOne.Do(func() {
		s.mu.Lock()
		s.closed = true
		timeout := s.drainTimeout
		if timeout > 0 {
			// 先装好定时器再发关闭信号，避免 drainLoop 读到 nil drainTimer
			s.drainTimer = time.AfterFunc(timeout, s.forceFinalize)
		}
		s.mu.Unlock()
		close(s.done)
		if timeout <= 0 {
			s.forceFinalize()
		}
	})
}

// Finalized 返回一个 channel，会话收敛完成（最终账目已定案）时关闭。
func (s *Subscriber) Finalized() <-chan struct{} { return s.finalizeCh }

// Report 返回会话当前账目快照；Finalized 关闭后即为最终账目。
func (s *Subscriber) Report() SessionReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionReport{Topic: s.topic, ID: s.id, Stats: s.stats, Cursor: s.cursor}
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
