package gateway

import (
	"fmt"
	"sync"
	"time"
)

// Handler 是订阅者的消息处理函数；返回错误计为一次投递失败。
type Handler func(Message) error

// topic 是单个主题的内部状态：只追加日志 + 订阅者集合。
type topic struct {
	mu     sync.Mutex
	log    []Message // 环形保留窗口，log[0] 的序号为 base
	base   Seq       // log[0] 对应的序号；窗口滑动时递增
	latest Seq       // 已发布的最大序号
	subs   map[*Subscription]struct{}
	closed bool
}

func (t *topic) append(m Message) {
	t.log = append(t.log, m)
	t.latest = m.Seq
}

// trim 将保留窗口裁剪到 retention 长度。
func (t *topic) trim(retention int) {
	for uint64(len(t.log)) > uint64(retention) {
		t.log = t.log[1:]
		t.base++
	}
}

// from 返回序号 > cursor 的全部保留消息；调用方须已持有锁且通过校验。
func (t *topic) from(cursor Cursor) []Message {
	start := int(uint64(cursor) + 1 - uint64(t.base)) // cursor+1 在 log 中的下标
	if start < 0 {
		start = 0
	}
	out := make([]Message, len(t.log)-start)
	copy(out, t.log[start:])
	return out
}

// Broker 管理主题、发布与订阅。
type Broker struct {
	cfg    Config
	mu     sync.Mutex
	topics map[string]*topic
	closed bool

	stats Stats
}

// NewBroker 创建一个网关 Broker。
func NewBroker(cfg Config) *Broker {
	if cfg.Retention <= 0 {
		cfg.Retention = 1
	}
	if cfg.SubscriberBuffer <= 0 {
		cfg.SubscriberBuffer = 1
	}
	return &Broker{cfg: cfg, topics: make(map[string]*topic)}
}

func (b *Broker) topicLocked(name string) *topic {
	t, ok := b.topics[name]
	if !ok {
		t = &topic{base: 1, subs: make(map[*Subscription]struct{})}
		b.topics[name] = t
	}
	return t
}

// Publish 向主题发布一条消息，返回分配的序号。
// 同一主题的消息按发布顺序投递给发布时刻的全部活跃订阅者。
func (b *Broker) Publish(topic string, payload []byte) Seq {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0
	}
	t := b.topicLocked(topic)
	b.mu.Unlock()

	t.mu.Lock()
	seq := t.latest + 1
	msg := Message{Topic: topic, Seq: seq, Payload: payload, Published: time.Now()}
	t.append(msg)
	t.trim(b.cfg.Retention)
	// 扇出快照：持有主题锁期间订阅增删被串行化，
	// 因此快照内的订阅者恰好收到本条消息一次，快照外的不会收到。
	subs := make([]*Subscription, 0, len(t.subs))
	for s := range t.subs {
		subs = append(subs, s)
	}
	t.mu.Unlock()

	b.mu.Lock()
	b.stats.Published++
	b.mu.Unlock()

	for _, s := range subs {
		s.enqueue(msg)
	}
	return seq
}

// Subscribe 以指定游标订阅主题，返回订阅句柄。
// cursor 为订阅者上次已确认的位置；传 0 表示从当前最新位置开始只收新消息。
// 游标校验规则（失败时返回可判定原因的 *Error）：
//   - cursor > latest            → ReasonCursorRegression（位置倒退/越界）
//   - cursor+1 < base（太旧）     → ReasonCursorExpired（超出保留窗口）
func (b *Broker) Subscribe(topic string, cursor Cursor, h Handler) (*Subscription, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, &Error{Reason: ReasonSubscriberClosed, Topic: topic, Detail: "broker closed"}
	}
	t := b.topicLocked(topic)
	b.mu.Unlock()

	t.mu.Lock()
	latest := t.latest
	switch {
	case uint64(cursor) > uint64(latest):
		t.mu.Unlock()
		return nil, &Error{
			Reason: ReasonCursorRegression, Topic: topic, Cursor: cursor,
			Detail: fmt.Sprintf("cursor ahead of latest seq %d", latest),
		}
	case uint64(cursor)+1 < uint64(t.base):
		t.mu.Unlock()
		return nil, &Error{
			Reason: ReasonCursorExpired, Topic: topic, Cursor: cursor,
			Detail: fmt.Sprintf("cursor behind retained window [base=%d, latest=%d]", t.base, latest),
		}
	}
	// cursor == 0 表示新订阅者只收新消息：从 latest 之后开始。
	startFrom := cursor
	if cursor == 0 {
		startFrom = Cursor(latest)
	}
	backlog := t.from(startFrom)
	s := newSubscription(topic, startFrom, b.cfg.SubscriberBuffer, h, b, t)
	t.subs[s] = struct{}{}
	t.mu.Unlock()

	// 先补发保留窗口内的历史消息（保持顺序），再进入实时流。
	s.replay(backlog)
	return s, nil
}

// remove 由 Subscription.Unsubscribe 调用。
func (b *Broker) remove(t *topic, s *Subscription) {
	t.mu.Lock()
	delete(t.subs, s)
	t.mu.Unlock()
}

func (b *Broker) addDelivered() { b.mu.Lock(); b.stats.Delivered++; b.mu.Unlock() }
func (b *Broker) addDropped()   { b.mu.Lock(); b.stats.Dropped++; b.mu.Unlock() }
func (b *Broker) addFailed()    { b.mu.Lock(); b.stats.Failed++; b.mu.Unlock() }

// Stats 返回全局统计快照。
// 不变式：对每条已发布消息 × 每个扇出快照内的订阅者，
// 其结果必为 Delivered / Dropped / Failed 三者之一。
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

// Close 关闭 Broker 及全部订阅。
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	topics := make([]*topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.Unlock()

	for _, t := range topics {
		t.mu.Lock()
		subs := make([]*Subscription, 0, len(t.subs))
		for s := range t.subs {
			subs = append(subs, s)
		}
		t.mu.Unlock()
		for _, s := range subs {
			s.Unsubscribe()
		}
	}
}
