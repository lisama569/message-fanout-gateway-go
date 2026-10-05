package gateway

import (
	"fmt"
	"sync"
	"time"
)

// DefaultDrainTimeout 是订阅者下线宽限时间的默认值。
const DefaultDrainTimeout = 2 * time.Second

// Config 网关可配置项。
type Config struct {
	RetainPerTopic   int        // 每主题日志保留条数（恢复窗口）
	SubscriberBuffer int        // 每订阅者缓冲容量
	Policy           DropPolicy // 缓冲满时的丢弃策略
	// DrainTimeout 订阅者下线时的宽限时间：宽限期内继续处理缓冲中
	// 剩余及正在处理的消息；到期未完成的计为 DroppedShutdown。
	// <=0 表示不给予宽限，立即结算。
	DrainTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.RetainPerTopic < 1 {
		c.RetainPerTopic = 1024
	}
	if c.SubscriberBuffer < 1 {
		c.SubscriberBuffer = 64
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	// DrainTimeout < 0 与显式的 0 语义不同：负数归一化为 0（立即结算）。
	if c.DrainTimeout < 0 {
		c.DrainTimeout = 0
	}
	return c
}

// Broker 负责主题管理、订阅与分发。
type Broker struct {
	cfg Config

	// mu 串行化 Publish 与 Subscribe/Unsubscribe 的注册路径，
	// 保证同一主题的消息按发布顺序进入每个订阅者的缓冲，
	// 且订阅增删不会造成重复投递或漏投。
	mu      sync.Mutex
	logs    map[string]*topicLog
	subs    map[string]map[string]*Subscriber // topic -> subID -> Subscriber
	reports map[string][]SessionReport        // topic -> 已结算会话账目（含已离开订阅者）
}

func NewBroker(cfg Config) *Broker {
	return &Broker{
		cfg:     cfg.withDefaults(),
		logs:    make(map[string]*topicLog),
		subs:    make(map[string]map[string]*Subscriber),
		reports: make(map[string][]SessionReport),
	}
}

// Publish 发布消息到主题，按发布顺序分发给当前全部订阅者。
// 返回分配的 Seq。分发对每个订阅者是非阻塞的：慢消费者按策略丢弃，不影响他人。
func (b *Broker) Publish(topic string, payload []byte) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logs[topic]
	if log == nil {
		log = newTopicLog(b.cfg.RetainPerTopic)
		b.logs[topic] = log
	}
	m := log.append(topic, payload)
	for _, sub := range b.subs[topic] {
		sub.enqueue(m)
	}
	return m.Seq, nil
}

// Subscribe 从 cursor 之后（cursor+1）开始订阅。
// cursor 为 0 表示从最早保留位置开始。
// 游标非法时返回可判定错误（ErrKindCursorExpired / ErrKindCursorRegression），
// 不会从中间继续或静默重放。
func (b *Broker) Subscribe(topic, id string, cursor Cursor, h Handler) (*Subscriber, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	log := b.logs[topic]
	if log == nil {
		log = newTopicLog(b.cfg.RetainPerTopic)
		b.logs[topic] = log
	}
	backlog, gerr := log.from(topic, uint64(cursor)+1)
	if gerr != nil {
		return nil, gerr
	}
	// 同 ID 重订阅：先移除旧订阅者，避免重复投递
	if old, ok := b.subs[topic][id]; ok {
		delete(b.subs[topic], id)
		old.Close()
	}
	sub := newSubscriber(id, topic, b.cfg.SubscriberBuffer, b.cfg.Policy, b.cfg.DrainTimeout)
	sub.onFinalize = b.archiveReport
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[string]*Subscriber)
	}
	b.subs[topic][id] = sub
	// 在持锁状态下回放积压，保证回放与后续实时投递的相对顺序
	for _, m := range backlog {
		sub.enqueue(m)
	}
	go sub.run(h)
	return sub, nil
}

// Unsubscribe 移除并关闭订阅者。
func (b *Broker) Unsubscribe(topic, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subs[topic][id]; ok {
		delete(b.subs[topic], id)
		sub.Close()
	}
}

// LatestSeq 返回主题最新 Seq（主题不存在或无消息时为 0）。
func (b *Broker) LatestSeq(topic string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if log := b.logs[topic]; log != nil {
		return log.latest()
	}
	return 0
}

// EarliestSeq 返回主题保留的最早 Seq。
func (b *Broker) EarliestSeq(topic string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if log := b.logs[topic]; log != nil {
		return log.earliest()
	}
	return 0
}

// SubscriberCount 返回主题当前订阅者数。
func (b *Broker) SubscriberCount(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs[topic])
}

// archiveReport 归档已结算的会话账目（由 Subscriber 结算时回调）。
func (b *Broker) archiveReport(r SessionReport) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reports[r.Topic] = append(b.reports[r.Topic], r)
}

// SessionReports 返回主题下指定订阅者已结算的会话账目（按结算时间升序）。
// 订阅者离开后账目仍保留，可事后审计。id 为空串时返回该主题全部已结算账目。
func (b *Broker) SessionReports(topic, id string) []SessionReport {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []SessionReport
	for _, r := range b.reports[topic] {
		if id == "" || r.SubscriberID == id {
			out = append(out, r)
		}
	}
	return out
}

// TopicStats 返回按主题聚合的统计视图：
// 已离开订阅者贡献其最终账目，活跃订阅者贡献其当前快照。
// 守恒口径：Dispatched == Delivered + Dropped + Failed。
func (b *Broker) TopicStats(topic string) TopicStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	ts := TopicStats{Topic: topic, FailedBy: make(map[string]uint64)}
	if log := b.logs[topic]; log != nil {
		ts.Published = log.latest()
	}
	add := func(st Stats) {
		ts.Dispatched += st.Total()
		ts.Delivered += st.Delivered
		ts.Dropped += st.Dropped
		ts.DroppedBackpressure += st.DroppedBackpressure
		ts.DroppedShutdown += st.DroppedShutdown
		ts.Failed += st.Failed
		for cat, n := range st.FailedBy {
			ts.FailedBy[cat] += n
		}
	}
	for _, r := range b.reports[topic] {
		add(r.Stats)
	}
	ts.Sessions = len(b.reports[topic])
	for _, sub := range b.subs[topic] {
		add(sub.Stats())
		ts.Sessions++
	}
	ts.ActiveSubscribers = len(b.subs[topic])
	return ts
}

// String 便于日志输出。
func (c Config) String() string {
	return fmt.Sprintf("retain=%d buffer=%d policy=%v drain=%v", c.RetainPerTopic, c.SubscriberBuffer, c.Policy, c.DrainTimeout)
}
