package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestGracefulUnsubscribeDrain 优雅下线：宽限期内缓冲排空。
// 判定依据：退订后缓冲中剩余消息在宽限期内全部处理成功，
// 会话账目 Reason=drained，Delivered == 发布总数，DroppedShutdown == 0，
// 最终游标等于最后一条消息 Seq，且账目在订阅者离开后仍可查询。
func TestGracefulUnsubscribeDrain(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 16, Policy: DropNewest, DrainTimeout: 2 * time.Second}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 10
	t.Logf("输入条件: 发布=%d, buffer=%d, drain=%v, handler 每条耗时 5ms", total, cfg.SubscriberBuffer, cfg.DrainTimeout)

	sub, err := b.Subscribe("orders", "s1", 0, func(Message) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	// 消息仍在缓冲中排队时立即退订
	b.Unsubscribe("orders", "s1")
	t.Logf("退订时刻 Delivered=%d (剩余消息应在宽限期内继续处理)", sub.Stats().Delivered)

	waitFor(t, "session report archived", func() bool {
		return len(b.SessionReports("orders", "s1")) == 1
	})
	reps := b.SessionReports("orders", "s1")
	rep := reps[0]
	t.Logf("判定依据: 会话账目 reason=%s delivered=%d dropped=%d dropped_shutdown=%d failed=%d cursor=%d",
		rep.Reason, rep.Stats.Delivered, rep.Stats.Dropped, rep.Stats.DroppedShutdown, rep.Stats.Failed, rep.Cursor)
	if rep.Reason != FinalizeDrained {
		t.Fatalf("判定失败: 期望 reason=drained, 实际=%s", rep.Reason)
	}
	if rep.Stats.Delivered != total || rep.Stats.DroppedShutdown != 0 || rep.Stats.Total() != total {
		t.Fatalf("判定失败: 期望 delivered=%d 且无下线丢弃, 实际=%+v", total, rep.Stats)
	}
	if rep.Cursor != Cursor(total) {
		t.Fatalf("判定失败: 期望最终游标=%d, 实际=%d", total, rep.Cursor)
	}
	if b.SubscriberCount("orders") != 0 {
		t.Fatalf("判定失败: 订阅者应已移除, 实际活跃数=%d", b.SubscriberCount("orders"))
	}
}

// TestUnsubscribeDrainTimeout 宽限期超时：剩余消息明确归为下线丢弃。
// 判定依据：handler 阻塞导致宽限期内无法排空，到期后排队消息与在途消息
// 全部计为 DroppedShutdown，Reason=drain_timeout；背压丢弃与下线丢弃分别计数；
// 守恒 Delivered+Dropped+Failed == 发布总数 在结算时刻即成立。
func TestUnsubscribeDrainTimeout(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 4, Policy: DropNewest, DrainTimeout: 50 * time.Millisecond}
	b := NewBroker(cfg)
	src := NewSource(b, "events")
	const total = 10
	t.Logf("输入条件: 发布=%d, buffer=%d, drain=%v, handler 阻塞直至放行", total, cfg.SubscriberBuffer, cfg.DrainTimeout)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	_, err := b.Subscribe("events", "slow", 0, func(Message) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	<-entered // 确认第 1 条正在处理，其余排队或因背压丢弃
	b.Unsubscribe("events", "slow")

	waitFor(t, "forced finalize after drain timeout", func() bool {
		return len(b.SessionReports("events", "slow")) == 1
	})
	rep := b.SessionReports("events", "slow")[0]
	t.Logf("判定依据: reason=%s delivered=%d dropped_bp=%d dropped_shutdown=%d total=%d (published=%d)",
		rep.Reason, rep.Stats.Delivered, rep.Stats.DroppedBackpressure, rep.Stats.DroppedShutdown,
		rep.Stats.Total(), total)
	if rep.Reason != FinalizeTimeout {
		t.Fatalf("判定失败: 期望 reason=drain_timeout, 实际=%s", rep.Reason)
	}
	if rep.Stats.DroppedShutdown == 0 {
		t.Fatalf("判定失败: 期望存在下线丢弃, 实际=%+v", rep.Stats)
	}
	if rep.Stats.DroppedBackpressure == 0 {
		t.Fatalf("判定失败: 期望存在背压丢弃(buffer=%d < 发布=%d), 实际=%+v", cfg.SubscriberBuffer, total, rep.Stats)
	}
	if rep.Stats.Dropped != rep.Stats.DroppedBackpressure+rep.Stats.DroppedShutdown {
		t.Fatalf("判定失败: 丢弃分类不守恒: %+v", rep.Stats)
	}
	if rep.Stats.Total() != total {
		t.Fatalf("判定失败: 会话账目不守恒: total=%d, 期望=%d", rep.Stats.Total(), total)
	}

	// 放行 handler：在途消息晚于结算完成，不得改变已归档账目
	close(release)
	time.Sleep(20 * time.Millisecond)
	rep2 := b.SessionReports("events", "slow")[0]
	if rep2.Stats.Total() != total || rep2.Stats.Delivered != rep.Stats.Delivered ||
		rep2.Stats.Dropped != rep.Stats.Dropped || rep2.Stats.Failed != rep.Stats.Failed {
		t.Fatalf("判定失败: 结算后账目被改写: %+v -> %+v", rep.Stats, rep2.Stats)
	}
	t.Logf("判定依据: 在途 handler 放行后归档账目不变, total 仍为 %d", rep2.Stats.Total())
}

// TestSessionReportFailureCategories 失败分类与退订后账目可查。
// 判定依据：实现 FailureCategorizer 的错误按类别计数，普通错误归入
// handler_error；Failed == 各类别之和；退订后 SessionReports 仍可查到
// 完整账目与最终游标。
func TestSessionReportFailureCategories(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 16, Policy: DropNewest, DrainTimeout: time.Second}
	b := NewBroker(cfg)
	src := NewSource(b, "pay")
	const total = 9
	t.Logf("输入条件: 发布=%d, 每 3 条中 1 条 validation 失败、1 条普通失败、1 条成功", total)

	var n int
	var mu sync.Mutex
	_, err := b.Subscribe("pay", "audited", 0, func(Message) error {
		mu.Lock()
		n++
		i := n
		mu.Unlock()
		switch i % 3 {
		case 1:
			return NewHandlerError("validation", errors.New("bad payload"))
		case 2:
			return errors.New("boom")
		default:
			return nil
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	waitFor(t, "all processed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return n == total
	})
	b.Unsubscribe("pay", "audited")
	waitFor(t, "session report archived", func() bool {
		return len(b.SessionReports("pay", "audited")) == 1
	})

	rep := b.SessionReports("pay", "audited")[0]
	t.Logf("判定依据: delivered=%d failed=%d failed_by=%v cursor=%d",
		rep.Stats.Delivered, rep.Stats.Failed, rep.Stats.FailedBy, rep.Cursor)
	if rep.Stats.Delivered != 3 || rep.Stats.Failed != 6 {
		t.Fatalf("判定失败: 期望 delivered=3 failed=6, 实际=%+v", rep.Stats)
	}
	if rep.Stats.FailedBy["validation"] != 3 || rep.Stats.FailedBy[FailureCategoryHandler] != 3 {
		t.Fatalf("判定失败: 失败分类不符: %v", rep.Stats.FailedBy)
	}
	var sum uint64
	for _, v := range rep.Stats.FailedBy {
		sum += v
	}
	if sum != rep.Stats.Failed {
		t.Fatalf("判定失败: 失败分类之和 %d != Failed %d", sum, rep.Stats.Failed)
	}
	if rep.Cursor != Cursor(total) {
		t.Fatalf("判定失败: 期望最终游标=%d, 实际=%d", total, rep.Cursor)
	}
	if rep.Stats.Total() != total {
		t.Fatalf("判定失败: 会话账目不守恒: %+v", rep.Stats)
	}
}

// TestNonPositiveDrainTimeout 非正宽限时间：不给予宽限，立即结算。
// 判定依据：DrainTimeout<0 归一化为立即结算；退订后排队与在途消息
// 全部计为 DroppedShutdown，Reason=drain_timeout，账目守恒。
func TestNonPositiveDrainTimeout(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 64, Policy: DropNewest, DrainTimeout: -1}
	b := NewBroker(cfg)
	src := NewSource(b, "ticks")
	const total = 5
	t.Logf("输入条件: 发布=%d, drain=%v (非正 => 立即结算), handler 阻塞", total, cfg.DrainTimeout)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	_, err := b.Subscribe("ticks", "s1", 0, func(Message) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	<-entered
	b.Unsubscribe("ticks", "s1")

	waitFor(t, "immediate finalize", func() bool {
		return len(b.SessionReports("ticks", "s1")) == 1
	})
	rep := b.SessionReports("ticks", "s1")[0]
	t.Logf("判定依据: reason=%s delivered=%d dropped_shutdown=%d total=%d",
		rep.Reason, rep.Stats.Delivered, rep.Stats.DroppedShutdown, rep.Stats.Total())
	if rep.Reason != FinalizeTimeout {
		t.Fatalf("判定失败: 期望 reason=drain_timeout, 实际=%s", rep.Reason)
	}
	if rep.Stats.Delivered != 0 || rep.Stats.DroppedShutdown != total || rep.Stats.Total() != total {
		t.Fatalf("判定失败: 期望全部 %d 条计为下线丢弃, 实际=%+v", total, rep.Stats)
	}
	close(release)
}

// TestTopicStatsConservationUnderChurn 高频上下线下的聚合守恒。
// 判定依据：常驻订阅者 Delivered == 发布总数；聚合视图中
// Dispatched == Delivered+Dropped+Failed、Dropped == 两类丢弃之和、
// Failed == 各类别失败之和；已离开订阅者的贡献保留在聚合中。
func TestTopicStatsConservationUnderChurn(t *testing.T) {
	cfg := Config{RetainPerTopic: 8192, SubscriberBuffer: 64, Policy: DropNewest, DrainTimeout: 10 * time.Millisecond}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 300
	const churners = 6
	t.Logf("输入条件: 发布=%d, churn 订阅者=%d 高频上下线, buffer=%d, drain=%v",
		total, churners, cfg.SubscriberBuffer, cfg.DrainTimeout)

	stable, err := b.Subscribe("orders", "stable", 0, func(Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe stable: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < churners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("churn-%d", i)
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = b.Subscribe("orders", id, 0, func(Message) error {
					time.Sleep(time.Millisecond) // 慢处理，制造背压与收敛丢弃
					return nil
				})
				b.Unsubscribe("orders", id)
			}
		}(i)
	}
	// 逐条发布并留出调度间隙，模拟真实事件流而非瞬时突发
	for i := 0; i < total; i++ {
		_, _ = src.Emit("orders", fmt.Sprintf("order-%d", i))
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	waitFor(t, "stable receives all", func() bool { return stable.Stats().Delivered == total })
	// 等全部 churn 会话结算归档
	waitFor(t, "all churn sessions finalized", func() bool {
		ts := b.TopicStats("orders")
		return ts.ActiveSubscribers == 1 && ts.Dispatched == ts.Delivered+ts.Dropped+ts.Failed &&
			int(ts.Sessions) > churners
	})
	// 结算是异步的，再等一个宽限周期确保全部归档
	time.Sleep(5 * cfg.DrainTimeout)

	ts := b.TopicStats("orders")
	t.Logf("判定依据: published=%d dispatched=%d delivered=%d dropped=%d(bp=%d shutdown=%d) failed=%d sessions=%d active=%d",
		ts.Published, ts.Dispatched, ts.Delivered, ts.Dropped,
		ts.DroppedBackpressure, ts.DroppedShutdown, ts.Failed, ts.Sessions, ts.ActiveSubscribers)

	if ts.Published != total {
		t.Fatalf("判定失败: 期望 published=%d, 实际=%d", total, ts.Published)
	}
	if ts.Dispatched != ts.Delivered+ts.Dropped+ts.Failed {
		t.Fatalf("判定失败: 聚合不守恒 dispatched=%d != delivered+dropped+failed=%d",
			ts.Dispatched, ts.Delivered+ts.Dropped+ts.Failed)
	}
	if ts.Dropped != ts.DroppedBackpressure+ts.DroppedShutdown {
		t.Fatalf("判定失败: 丢弃分类不守恒: %+v", ts)
	}
	var fsum uint64
	for _, v := range ts.FailedBy {
		fsum += v
	}
	if fsum != ts.Failed {
		t.Fatalf("判定失败: 失败分类之和 %d != Failed %d", fsum, ts.Failed)
	}
	// 常驻订阅者视角：发布总量 == 成功+丢弃+失败
	if stable.Stats().Total() != total {
		t.Fatalf("判定失败: 常驻订阅者不守恒: %+v, 期望 total=%d", stable.Stats(), total)
	}
	// 已离开订阅者的贡献必须保留：存在已结算会话且其账目计入聚合
	reps := b.SessionReports("orders", "")
	if len(reps) == 0 {
		t.Fatalf("判定失败: 期望存在已归档会话账目")
	}
	var archivedTotal uint64
	for _, r := range reps {
		archivedTotal += r.Stats.Total()
	}
	if ts.Dispatched != archivedTotal+stable.Stats().Total() {
		t.Fatalf("判定失败: 聚合 dispatched=%d != 归档 %d + 活跃 %d",
			ts.Dispatched, archivedTotal, stable.Stats().Total())
	}
	t.Logf("判定依据: %d 个已离开会话贡献 %d 条投递, 全部保留在聚合视图中", len(reps), archivedTotal)
}
