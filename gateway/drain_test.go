package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitReport 等待指定会话的最终账目落账（落账异步于定案）并返回。
func waitReport(t *testing.T, b *Broker, topic, id string) SessionReport {
	t.Helper()
	var rep SessionReport
	waitFor(t, "session report recorded: "+topic+"/"+id, func() bool {
		var ok bool
		rep, ok = b.SessionReport(topic, id)
		return ok
	})
	return rep
}

// TestGracefulUnsubscribeDrain 优雅下线收敛：
// 判定依据：退订后宽限期内，在途与缓冲中消息被处理完才定案；
// 定案前 SessionReport 不可用，定案后可查，且 Delivered=5、Cursor=5、无丢弃；
// 主题聚合满足 Published == Delivered+Dropped+Failed。
func TestGracefulUnsubscribeDrain(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 8, Policy: DropNewest, DrainTimeout: 2 * time.Second}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 5
	t.Logf("输入条件: 发布=%d, buffer=%d, DrainTimeout=%v, handler 阻塞直至放行", total, cfg.SubscriberBuffer, cfg.DrainTimeout)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	sub, err := b.Subscribe("orders", "s1", 0, func(Message) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	<-started // 1 条在途 + 4 条在缓冲排队

	b.Unsubscribe("orders", "s1")

	// 宽限期内、消息未处理完前不应定案
	select {
	case <-sub.Finalized():
		t.Fatal("判定失败: 缓冲未排空且宽限未到期，不应提前定案")
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := b.SessionReport("orders", "s1"); ok {
		t.Fatal("判定失败: 会话未收敛完成前不应查到最终账目")
	}
	t.Logf("判定依据: 退订后 100ms 内未定案、账目不可查，宽限收敛正在进行")

	close(release) // 放行：在途 + 缓冲共 5 条在宽限期内处理完
	waitFor(t, "session finalized after drain", func() bool {
		select {
		case <-sub.Finalized():
			return true
		default:
			return false
		}
	})

	rep := waitReport(t, b, "orders", "s1")
	t.Logf("判定依据: 最终账目 %+v, cursor=%d", rep.Stats, rep.Cursor)
	if rep.Stats.Delivered != total || rep.Stats.Dropped != 0 || rep.Stats.Failed != 0 {
		t.Fatalf("判定失败: 期望 Delivered=%d Dropped=0 Failed=0, 实际 %+v", total, rep.Stats)
	}
	if rep.Cursor != Cursor(total) {
		t.Fatalf("判定失败: 期望 cursor=%d, 实际=%d", total, rep.Cursor)
	}

	ts := b.TopicStats("orders")
	t.Logf("判定依据: 主题聚合 %+v", ts)
	if ts.Published != total || ts.Total() != total || ts.DepartedSessions != 1 || ts.ActiveSubscribers != 0 {
		t.Fatalf("判定失败: 聚合口径异常: %+v", ts)
	}
}

// TestDrainTimeoutDrops 宽限超时丢弃：
// 判定依据：handler 一直阻塞，宽限 100ms 到期后在途 1 条 + 缓冲 4 条全部计为
// DroppedDrain（与背压丢弃分开），账目定案且退订后可查；放行 handler 后账目不再变化。
func TestDrainTimeoutDrops(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 8, Policy: DropNewest, DrainTimeout: 100 * time.Millisecond}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 5
	t.Logf("输入条件: 发布=%d, buffer=%d, DrainTimeout=%v, handler 永久阻塞(测试结束才放行)",
		total, cfg.SubscriberBuffer, cfg.DrainTimeout)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	sub, err := b.Subscribe("orders", "stuck", 0, func(Message) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	src.EmitN(total)
	<-started // 1 在途 + 4 缓冲

	b.Unsubscribe("orders", "stuck")
	waitFor(t, "force finalize after drain timeout", func() bool {
		select {
		case <-sub.Finalized():
			return true
		default:
			return false
		}
	})

	rep := waitReport(t, b, "orders", "stuck")
	t.Logf("判定依据: 最终账目 %+v (在途1+缓冲4 全部归为下线收敛丢弃)", rep.Stats)
	if rep.Stats.Dropped != total || rep.Stats.DroppedDrain != total || rep.Stats.DroppedBackpressure != 0 {
		t.Fatalf("判定失败: 期望 Dropped=DroppedDrain=%d, DroppedBackpressure=0, 实际 %+v", total, rep.Stats)
	}
	if rep.Stats.Delivered != 0 || rep.Cursor != 0 {
		t.Fatalf("判定失败: 期望 Delivered=0 cursor=0, 实际 %+v cursor=%d", rep.Stats, rep.Cursor)
	}

	ts := b.TopicStats("orders")
	if ts.Published != total || ts.Dropped != total || ts.DroppedDrain != total {
		t.Fatalf("判定失败: 聚合口径异常: %+v", ts)
	}
	t.Logf("判定依据: Published=%d == Dropped=%d (DroppedDrain=%d), 发布总量=成功+丢弃+失败 成立",
		ts.Published, ts.Dropped, ts.DroppedDrain)

	// 放行卡住的 handler：账目已定案，不应再变化
	close(release)
	time.Sleep(50 * time.Millisecond)
	if got := sub.Stats(); got != rep.Stats {
		t.Fatalf("判定失败: 定案后账目被改动: 前=%+v 后=%+v", rep.Stats, got)
	}
	t.Logf("判定依据: handler 放行后账目保持 %+v, 定案结果不可篡改", rep.Stats)
}

// TestDrainTimeoutZeroAndNegative 边界取值：
// 判定依据：DrainTimeout=0 立即定案，残余消息立刻计为 DroppedDrain；
// DrainTimeout<0 使用默认值 2s，宽限期内(200ms)不定案，放行后正常收敛。
func TestDrainTimeoutZeroAndNegative(t *testing.T) {
	t.Run("zero means immediate", func(t *testing.T) {
		cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 8, DrainTimeout: 0}
		b := NewBroker(cfg)
		src := NewSource(b, "t")
		const total = 3
		t.Logf("输入条件: DrainTimeout=0, 发布=%d, handler 阻塞", total)

		started := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		var once sync.Once
		sub, _ := b.Subscribe("t", "s", 0, func(Message) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		})
		src.EmitN(total)
		<-started
		b.Unsubscribe("t", "s")
		waitFor(t, "immediate finalize with zero timeout", func() bool {
			select {
			case <-sub.Finalized():
				return true
			default:
				return false
			}
		})
		rep := waitReport(t, b, "t", "s")
		t.Logf("判定依据: DrainTimeout=0 无宽限, 残余 %d 条立即计为 DroppedDrain: %+v", total, rep.Stats)
		if rep.Stats.DroppedDrain != total || rep.Stats.Delivered != 0 {
			t.Fatalf("判定失败: 期望 DroppedDrain=%d Delivered=0, 实际 %+v", total, rep.Stats)
		}
	})

	t.Run("negative uses default", func(t *testing.T) {
		cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 8, DrainTimeout: -1}
		b := NewBroker(cfg)
		src := NewSource(b, "t")
		const total = 2
		t.Logf("输入条件: DrainTimeout=-1, 期望取默认值 %v, 发布=%d, handler 阻塞后放行", DefaultDrainTimeout, total)

		started := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		sub, _ := b.Subscribe("t", "s", 0, func(Message) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		})
		src.EmitN(total)
		<-started
		b.Unsubscribe("t", "s")

		select {
		case <-sub.Finalized():
			t.Fatal("判定失败: 默认宽限 2s 内不应提前定案")
		case <-time.After(200 * time.Millisecond):
		}
		t.Logf("判定依据: 200ms 内未定案, 说明负数配置回落到默认宽限 %v", DefaultDrainTimeout)

		close(release)
		waitFor(t, "finalize after release", func() bool {
			select {
			case <-sub.Finalized():
				return true
			default:
				return false
			}
		})
		rep := waitReport(t, b, "t", "s")
		t.Logf("判定依据: 放行后宽限内处理完成, 账目 %+v", rep.Stats)
		if rep.Stats.Delivered != total || rep.Stats.Dropped != 0 {
			t.Fatalf("判定失败: 期望 Delivered=%d Dropped=0, 实际 %+v", total, rep.Stats)
		}
	})
}

// TestDrainFailureClassification 失败分类：
// 判定依据：在线期间失败计 FailedOnline，宽限期内失败计 FailedDrain，
// Failed == FailedOnline + FailedDrain，聚合口径同构。
func TestDrainFailureClassification(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 8, DrainTimeout: 2 * time.Second}
	b := NewBroker(cfg)
	src := NewSource(b, "t")
	t.Logf("输入条件: payload=bad 时 handler 报错, payload=block 时阻塞直至放行")

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	sub, _ := b.Subscribe("t", "s", 0, func(m Message) error {
		switch string(m.Payload) {
		case "bad":
			return errors.New("handler error")
		case "block":
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	})

	// 在线阶段：1 成功 + 1 失败
	_, _ = src.Emit("t", "ok-1")
	_, _ = src.Emit("t", "bad")
	waitFor(t, "online processing", func() bool {
		s := sub.Stats()
		return s.Delivered == 1 && s.FailedOnline == 1
	})
	t.Logf("判定依据: 在线阶段 Delivered=1 FailedOnline=1 FailedDrain=0: %+v", sub.Stats())

	// 制造退订时仍有积压：block 占住在途，bad/ok-2 排队在缓冲
	_, _ = src.Emit("t", "block")
	<-entered
	_, _ = src.Emit("t", "bad") // 将在宽限期内处理并失败
	_, _ = src.Emit("t", "ok-2")
	b.Unsubscribe("t", "s")
	close(release) // 放行：剩余消息在宽限期内处理

	waitFor(t, "finalize", func() bool {
		select {
		case <-sub.Finalized():
			return true
		default:
			return false
		}
	})
	rep := waitReport(t, b, "t", "s")
	t.Logf("判定依据: 最终账目 %+v", rep.Stats)
	if rep.Stats.Failed != 2 || rep.Stats.FailedOnline != 1 || rep.Stats.FailedDrain != 1 {
		t.Fatalf("判定失败: 期望 Failed=2 (Online=1 Drain=1), 实际 %+v", rep.Stats)
	}
	if rep.Stats.Delivered != 3 || rep.Stats.Total() != 5 {
		t.Fatalf("判定失败: 期望 Delivered=3 Total=5, 实际 %+v", rep.Stats)
	}
	ts := b.TopicStats("t")
	if ts.FailedOnline != 1 || ts.FailedDrain != 1 || ts.Published != 5 || ts.Total() != 5 {
		t.Fatalf("判定失败: 聚合口径异常: %+v", ts)
	}
	t.Logf("判定依据: 聚合 Failed==FailedOnline(%d)+FailedDrain(%d), Published=%d==Total=%d",
		ts.FailedOnline, ts.FailedDrain, ts.Published, ts.Total())
}

// TestDropReasonSeparation 丢弃原因分离：
// 判定依据：背压丢弃计 DroppedBackpressure，下线收敛丢弃计 DroppedDrain，
// 两者之和等于 Dropped，且与发布总量对得上。
func TestDropReasonSeparation(t *testing.T) {
	cfg := Config{RetainPerTopic: 64, SubscriberBuffer: 2, Policy: DropNewest, DrainTimeout: 100 * time.Millisecond}
	b := NewBroker(cfg)
	src := NewSource(b, "t")
	const total = 10
	t.Logf("输入条件: 发布=%d, buffer=%d, handler 阻塞: 期望 1 在途 + 2 缓冲 + 7 背压丢弃; 退订后在途+缓冲转为收敛丢弃",
		total, cfg.SubscriberBuffer)

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var once sync.Once
	sub, _ := b.Subscribe("t", "s", 0, func(Message) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	_, _ = src.Emit("t", "m-1")
	<-started // 第 1 条进入在途
	for i := 1; i < total; i++ {
		_, _ = src.Emit("t", fmt.Sprintf("m-%d", i+1))
	}
	waitFor(t, "backpressure drops", func() bool { return sub.Stats().DroppedBackpressure == 7 })
	t.Logf("判定依据: 缓冲满期间 DroppedBackpressure=7, DroppedDrain=0, 两类丢弃已分流: %+v", sub.Stats())

	b.Unsubscribe("t", "s")
	waitFor(t, "finalize", func() bool {
		select {
		case <-sub.Finalized():
			return true
		default:
			return false
		}
	})
	rep := waitReport(t, b, "t", "s")
	t.Logf("判定依据: 最终账目 %+v", rep.Stats)
	if rep.Stats.Dropped != total ||
		rep.Stats.DroppedBackpressure != 7 ||
		rep.Stats.DroppedDrain != 3 { // 1 在途 + 2 缓冲
		t.Fatalf("判定失败: 期望 Dropped=%d (背压7 + 收敛3), 实际 %+v", total, rep.Stats)
	}
	ts := b.TopicStats("t")
	if ts.Published != total || ts.Total() != total ||
		ts.Dropped != ts.DroppedBackpressure+ts.DroppedDrain {
		t.Fatalf("判定失败: 聚合口径异常: %+v", ts)
	}
	t.Logf("判定依据: Published=%d == DroppedBackpressure(%d)+DroppedDrain(%d), 丢弃原因完全可分",
		ts.Published, ts.DroppedBackpressure, ts.DroppedDrain)
}

// TestAggregateConservationUnderChurn 高频上下线下的聚合守恒：
// 判定依据：8 个 goroutine 各进行 20 次订阅/退订（每次独立 ID），
// 另有 1 个常驻订阅者；全部收敛后：
//   - 常驻会话 Total == Published（发布总量=成功+丢弃+失败）；
//   - TopicStats 合计 == 全部 161 个 SessionReport 之和；
//   - 分类口径 Dropped==DroppedBackpressure+DroppedDrain, Failed==FailedOnline+FailedDrain。
func TestAggregateConservationUnderChurn(t *testing.T) {
	cfg := Config{RetainPerTopic: 4096, SubscriberBuffer: 4, Policy: DropNewest, DrainTimeout: 50 * time.Millisecond}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 500
	const churners = 8
	const sessionsPerChurner = 20
	t.Logf("输入条件: 发布=%d, buffer=%d, DrainTimeout=%v, churn=%d×%d 会话(独立ID), handler 对 Seq%%7==0 报错",
		total, cfg.SubscriberBuffer, cfg.DrainTimeout, churners, sessionsPerChurner)

	stable, err := b.Subscribe("orders", "stable", 0, func(Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe stable: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < churners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < sessionsPerChurner; j++ {
				id := fmt.Sprintf("churn-%d-%d", i, j)
				_, _ = b.Subscribe("orders", id, 0, func(m Message) error {
					if m.Seq%7 == 0 {
						return errors.New("handler error")
					}
					return nil
				})
				b.Unsubscribe("orders", id)
			}
		}(i)
	}
	for i := 0; i < total; i++ {
		_, _ = src.Emit("orders", fmt.Sprintf("e-%d", i))
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	b.Unsubscribe("orders", "stable")

	const wantSessions = churners*sessionsPerChurner + 1
	waitFor(t, "all sessions finalized", func() bool {
		ts := b.TopicStats("orders")
		return ts.DrainingSubscribers == 0 && ts.DepartedSessions == wantSessions
	})

	// 常驻会话：发布总量 = 成功 + 丢弃 + 失败
	srep := waitReport(t, b, "orders", "stable")
	t.Logf("判定依据: 常驻会话账目 %+v", srep.Stats)
	if srep.Stats.Total() != total {
		t.Fatalf("判定失败: 常驻会话 Total=%d != Published=%d", srep.Stats.Total(), total)
	}

	// 聚合 == 全部会话报告之和（已离开订阅者的贡献全部保留）
	var sum Stats
	for i := 0; i < churners; i++ {
		for j := 0; j < sessionsPerChurner; j++ {
			rep, ok := b.SessionReport("orders", fmt.Sprintf("churn-%d-%d", i, j))
			if !ok {
				t.Fatalf("判定失败: 会话 churn-%d-%d 的最终账目丢失", i, j)
			}
			if rep.Stats.Dropped != rep.Stats.DroppedBackpressure+rep.Stats.DroppedDrain ||
				rep.Stats.Failed != rep.Stats.FailedOnline+rep.Stats.FailedDrain {
				t.Fatalf("判定失败: 会话 churn-%d-%d 分类口径不自洽: %+v", i, j, rep.Stats)
			}
			sum = addStats(sum, rep.Stats)
		}
	}
	sum = addStats(sum, srep.Stats)

	ts := b.TopicStats("orders")
	t.Logf("判定依据: 聚合 %+v", ts)
	t.Logf("判定依据: %d 个会话报告逐项求和 %+v", wantSessions, sum)
	if ts.Published != total {
		t.Fatalf("判定失败: Published=%d != %d", ts.Published, total)
	}
	if ts.Delivered != sum.Delivered || ts.Dropped != sum.Dropped || ts.Failed != sum.Failed ||
		ts.DroppedBackpressure != sum.DroppedBackpressure || ts.DroppedDrain != sum.DroppedDrain ||
		ts.FailedOnline != sum.FailedOnline || ts.FailedDrain != sum.FailedDrain {
		t.Fatalf("判定失败: 聚合与各会话报告之和不一致: 聚合=%+v 求和=%+v", ts, sum)
	}
	if ts.Dropped != ts.DroppedBackpressure+ts.DroppedDrain ||
		ts.Failed != ts.FailedOnline+ts.FailedDrain {
		t.Fatalf("判定失败: 聚合分类口径不自洽: %+v", ts)
	}
	if ts.ActiveSubscribers != 0 || ts.DrainingSubscribers != 0 || ts.DepartedSessions != wantSessions {
		t.Fatalf("判定失败: 会话计数异常: %+v", ts)
	}
	t.Logf("判定依据: Published=%d, 常驻会话Total=%d; 聚合Total=%d == %d个会话报告之和, 等式成立",
		ts.Published, srep.Stats.Total(), ts.Total(), wantSessions)
	_ = stable
}
