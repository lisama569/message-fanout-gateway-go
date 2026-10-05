package gateway

import (
	"fmt"
	"sync"
	"time"
)

// DefaultDrainTimeout 是 DrainTimeout 的默认宽限时间。
const DefaultDrainTimeout = 2 * time.Second

// Config 网关可配置项。
type Config struct {
	RetainPerTopic   int           // 每主题日志保留条数（恢复窗口）
	SubscriberBuffer int           // 每订阅者缓冲容量
	Policy           DropPolicy    // 缓冲满时的丢弃策略
	DrainTimeout     time.Duration // 退订宽限时间：>0 限时收敛；==0 立即收敛；<0 取默认值
}

func (c Config) withDefaults() Config {
	if c.RetainPerTopic < 1 {
		c.RetainPerTopic = 1024
	}
	if c.SubscriberBuffer < 1 {
		c.SubscriberBuffer = 64
	}
	if c.DrainTimeout < 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	return c
}

// Broker 负责主题管理、订阅与分发。
type Broker struct {
	cfg Config

	// mu 串行化 Publish 与 Subscribe/Unsubscribe 的注册路径，
	// 保证同一主题的消息按发布顺序进入每个订阅者的缓冲，
	// 且订阅增删不会造成重复投递或漏投。
	mu   sync.Mutex
	logs map[string]*topicLog
	subs map[string]map[string]*Subscriber // topic -> subID -> Subscriber

	// 审计与聚合：
	// closing 保存已退订但仍在宽限收敛的订阅者（其统计仍计入聚合，保证账目无空窗）；
	// reports 保存每个 (topic, id) 最近收敛会话的最终账目，退订后仍可查询；
	// departed 按主题累计所有已收敛会话的统计，已离开订阅者的贡献永久保留。
	closing         map[string]map[*Subscriber]struct{} // topic -> 收敛中的订阅者集合
	reports         map[string]map[string]SessionReport
	departed        map[string]Stats
	departedSession map[string]uint64 // topic -> 已收敛会话数
}

func NewBroker(cfg Config) *Broker {
	return &Broker{
		cfg:             cfg.withDefaults(),
		logs:            make(map[string]*topicLog),
		subs:            make(map[string]map[string]*Subscriber),
		closing:         make(map[string]map[*Subscriber]struct{}),
		reports:         make(map[string]map[string]SessionReport),
		departed:        make(map[string]Stats),
		departedSession: make(map[string]uint64),
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
	// 同 ID 重订阅：旧会话退入收敛表并关闭，避免重复投递
	if old, ok := b.subs[topic][id]; ok {
		b.retireLocked(topic, old)
	}
	sub := newSubscriber(id, topic, b.cfg.SubscriberBuffer, b.cfg.Policy, b.cfg.DrainTimeout)
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

// Unsubscribe 移除订阅者并触发优雅关闭：
// 订阅者退入收敛表，缓冲与在途消息在 DrainTimeout 宽限内继续处理，
// 收敛完成后其最终账目可通过 SessionReport 查询，并计入 TopicStats 聚合。
func (b *Broker) Unsubscribe(topic, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sub, ok := b.subs[topic][id]; ok {
		b.retireLocked(topic, sub)
	}
}

// retireLocked 将订阅者从活跃表移入收敛表，触发关闭并登记落账回调。
// 调用方须持有 b.mu。收敛期间订阅者统计仍计入 TopicStats，账目无空窗。
func (b *Broker) retireLocked(topic string, sub *Subscriber) {
	delete(b.subs[topic], sub.ID())
	if b.closing[topic] == nil {
		b.closing[topic] = make(map[*Subscriber]struct{})
	}
	b.closing[topic][sub] = struct{}{}
	sub.Close()
	go b.recordReport(sub)
}

// recordReport 等待订阅者收敛完成，将其最终账目落账：
// 从收敛表移除、累计进 departed 聚合、保存 SessionReport 供事后查询。
func (b *Broker) recordReport(sub *Subscriber) {
	<-sub.Finalized()
	rep := sub.Report()
	b.mu.Lock()
	defer b.mu.Unlock()
	topic, id := sub.Topic(), sub.ID()
	delete(b.closing[topic], sub)
	b.departed[topic] = addStats(b.departed[topic], rep.Stats)
	b.departedSession[topic]++
	if b.reports[topic] == nil {
		b.reports[topic] = make(map[string]SessionReport)
	}
	b.reports[topic][id] = rep
}

// addStats 返回两个统计的逐项之和。
func addStats(a, b Stats) Stats {
	return Stats{
		Delivered:           a.Delivered + b.Delivered,
		Dropped:             a.Dropped + b.Dropped,
		Failed:              a.Failed + b.Failed,
		DroppedBackpressure: a.DroppedBackpressure + b.DroppedBackpressure,
		DroppedDrain:        a.DroppedDrain + b.DroppedDrain,
		FailedOnline:        a.FailedOnline + b.FailedOnline,
		FailedDrain:         a.FailedDrain + b.FailedDrain,
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

// SessionReport 返回指定订阅者最近一次会话的最终账目。
// 第二个返回值表示该会话是否已收敛完成并落账。
// 注意：落账异步于定案（Subscriber.Finalized 关闭后由后台回调写入），
// 定案后短暂窗口内可能暂不可查，调用方可轮询等待。
func (b *Broker) SessionReport(topic, id string) (SessionReport, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rep, ok := b.reports[topic][id]
	return rep, ok
}

// TopicStats 返回主题维度的聚合统计。
// 聚合范围：活跃订阅者 + 收敛中订阅者 + 已收敛会话（ departed 累计），
// 因此退订收敛的任意时刻账目都连续、无空窗。
func (b *Broker) TopicStats(topic string) TopicStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	ts := TopicStats{}
	if log := b.logs[topic]; log != nil {
		ts.Published = log.latest()
	}
	agg := b.departed[topic]
	for _, sub := range b.subs[topic] {
		agg = addStats(agg, sub.Stats())
	}
	for sub := range b.closing[topic] {
		agg = addStats(agg, sub.Stats())
	}
	ts.Delivered = agg.Delivered
	ts.Dropped = agg.Dropped
	ts.Failed = agg.Failed
	ts.DroppedBackpressure = agg.DroppedBackpressure
	ts.DroppedDrain = agg.DroppedDrain
	ts.FailedOnline = agg.FailedOnline
	ts.FailedDrain = agg.FailedDrain
	ts.ActiveSubscribers = len(b.subs[topic])
	ts.DrainingSubscribers = len(b.closing[topic])
	ts.DepartedSessions = b.departedSession[topic]
	return ts
}

// String 便于日志输出。
func (c Config) String() string {
	return fmt.Sprintf("retain=%d buffer=%d policy=%v drainTimeout=%v",
		c.RetainPerTopic, c.SubscriberBuffer, c.Policy, c.DrainTimeout)
}
