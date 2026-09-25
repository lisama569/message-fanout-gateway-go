package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitFor 轮询直到条件满足或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// TestConcurrentSubscribeChurn 并发订阅增删 + 持续发布：
// 判定依据：常驻订阅者收到的消息 Seq 严格递增、无重复、无漏投（Delivered == 发布总数）。
func TestConcurrentSubscribeChurn(t *testing.T) {
	cfg := Config{RetainPerTopic: 4096, SubscriberBuffer: 512, Policy: DropNewest}
	b := NewBroker(cfg)
	src := NewSource(b, "orders")
	const total = 500
	t.Logf("输入条件: 发布总数=%d, 常驻订阅者=1,  churn 订阅者=8 并发增删, buffer=%d", total, cfg.SubscriberBuffer)

	// 常驻订阅者：记录收到的 Seq 序列
	var mu sync.Mutex
	var got []uint64
	_, err := b.Subscribe("orders", "stable", 0, func(m Message) error {
		mu.Lock()
		got = append(got, m.Seq)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe stable: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// churn：并发订阅/退订
	for i := 0; i < 8; i++ {
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
				_, _ = b.Subscribe("orders", id, 0, func(Message) error { return nil })
				b.Unsubscribe("orders", id)
			}
		}(i)
	}
	// 持续发布
	src.EmitN(total)
	close(stop)
	wg.Wait()

	waitFor(t, "stable subscriber receives all", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == total
	})

	mu.Lock()
	defer mu.Unlock()
	for i, seq := range got {
		if seq != uint64(i+1) {
			t.Fatalf("判定失败: 位置 %d 期望 Seq=%d, 实际=%d（漏投/重复/乱序）", i, i+1, seq)
		}
	}
	t.Logf("判定依据: 常驻订阅者收到 %d 条, Seq 严格等于 1..%d, 无重复无漏投", len(got), total)
}

// TestResumeFromCursor 中断恢复：
// 判定依据：从 cursor+1 继续；过期游标与倒退游标被明确拒绝且原因可区分。
func TestResumeFromCursor(t *testing.T) {
	cfg := Config{RetainPerTopic: 10, SubscriberBuffer: 64, Policy: DropNewest}
	b := NewBroker(cfg)
	src := NewSource(b, "ticks")
	t.Logf("输入条件: retain=%d, 先发布 5 条, 订阅者确认到 cursor=5 后断开, 再发布 5 条", cfg.RetainPerTopic)

	src.EmitN(5)
	sub, err := b.Subscribe("ticks", "s1", 0, func(Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitFor(t, "consume first 5", func() bool { return sub.Stats().Delivered == 5 })
	cur := sub.Cursor()
	b.Unsubscribe("ticks", "s1")
	t.Logf("断开时 cursor=%d", cur)

	src.EmitN(5) // Seq 6..10
	var mu sync.Mutex
	var got []uint64
	_, err = b.Subscribe("ticks", "s1", cur, func(m Message) error {
		mu.Lock()
		got = append(got, m.Seq)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, "resume delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 5
	})
	mu.Lock()
	for i, seq := range got {
		if seq != uint64(6+i) {
			t.Fatalf("判定失败: 恢复后位置 %d 期望 Seq=%d, 实际=%d", i, 6+i, seq)
		}
	}
	mu.Unlock()
	t.Logf("判定依据: 恢复后收到 Seq=6..10, 与 cursor+1 一致, 无重放无跳过")

	// 过期游标：再发布使最早保留位置推进到 > cursor+1
	src.EmitN(10) // 共 20 条，retain=10，earliest=11
	_, err = b.Subscribe("ticks", "s2", 5, func(Message) error { return nil })
	var gerr *Error
	if !errors.As(err, &gerr) || gerr.Kind != ErrKindCursorExpired {
		t.Fatalf("判定失败: 期望 ErrKindCursorExpired, 实际 err=%v", err)
	}
	t.Logf("判定依据: cursor=5, earliest=%d, 拒绝原因=%s (cursor_expired)", b.EarliestSeq("ticks"), gerr.Kind)

	// 倒退/越界游标：cursor 超过最新位置
	_, err = b.Subscribe("ticks", "s3", Cursor(b.LatestSeq("ticks")+100), func(Message) error { return nil })
	if !errors.As(err, &gerr) || gerr.Kind != ErrKindCursorRegression {
		t.Fatalf("判定失败: 期望 ErrKindCursorRegression, 实际 err=%v", err)
	}
	t.Logf("判定依据: cursor=%d > latest=%d, 拒绝原因=%s (cursor_regression)",
		b.LatestSeq("ticks")+100, b.LatestSeq("ticks"), gerr.Kind)
}

// TestSlowConsumerIsolation 慢消费者隔离与背压：
// 判定依据：快订阅者收齐全部消息；慢订阅者 Delivered+Dropped+Failed == 发布总数；
// 处理失败的订阅者 Failed 计数正确，三类计数之和守恒。
func TestSlowConsumerIsolation(t *testing.T) {
	cfg := Config{RetainPerTopic: 4096, SubscriberBuffer: 4, Policy: DropNewest}
	b := NewBroker(cfg)
	src := NewSource(b, "events")
	const total = 100
	t.Logf("输入条件: 发布总数=%d, buffer=%d, 策略=DropNewest, 订阅者: fast/slow/failing", total, cfg.SubscriberBuffer)

	fast, err := b.Subscribe("events", "fast", 0, func(Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe fast: %v", err)
	}

	// 慢订阅者：处理永远阻塞在信号上，直到放行
	release := make(chan struct{})
	slow, err := b.Subscribe("events", "slow", 0, func(Message) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe slow: %v", err)
	}

	// 失败订阅者：一半消息处理失败
	var n int64
	var nmu sync.Mutex
	failing, err := b.Subscribe("events", "failing", 0, func(Message) error {
		nmu.Lock()
		n++
		fail := n%2 == 0
		nmu.Unlock()
		if fail {
			return errors.New("handler error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe failing: %v", err)
	}

	// 逐条发布并留出调度间隙，模拟真实事件流而非瞬时突发
	for i := 0; i < total; i++ {
		_, _ = src.Emit("events", fmt.Sprintf("e-%d", i))
		time.Sleep(time.Millisecond)
	}

	// 快订阅者不被慢订阅者拖慢：在 slow 仍阻塞时，fast 已收齐全部消息
	waitFor(t, "fast subscriber done while slow blocked", func() bool {
		return fast.Stats().Delivered == total
	})
	fst := fast.Stats()
	t.Logf("判定依据: slow 仍阻塞期间 fast Delivered=%d Dropped=%d, 慢消费者未阻塞他人",
		fst.Delivered, fst.Dropped)

	// 慢订阅者：缓冲已满，按 DropNewest 发生可计数的丢弃
	waitFor(t, "slow subscriber drops under backpressure", func() bool {
		return slow.Stats().Dropped > 0
	})
	t.Logf("判定依据: slow 阻塞期间 Dropped=%d > 0, 背压策略已生效", slow.Stats().Dropped)

	// 放行慢订阅者，处理中的消息完成后验证守恒不变式
	close(release)
	waitFor(t, "slow subscriber conservation after drain", func() bool {
		return slow.Stats().Total() == total
	})
	ss := slow.Stats()
	t.Logf("判定依据: slow Delivered=%d + Dropped=%d + Failed=%d == 发布总数 %d (buffer=%d, 策略=DropNewest)",
		ss.Delivered, ss.Dropped, ss.Failed, total, cfg.SubscriberBuffer)
	if ss.Total() != total {
		t.Fatalf("判定失败: slow 计数不守恒: %+v, 期望总和=%d", ss, total)
	}

	// 失败订阅者：Delivered+Failed == total
	waitFor(t, "failing subscriber accounting", func() bool {
		s := failing.Stats()
		return s.Delivered+s.Failed == total
	})
	fs := failing.Stats()
	t.Logf("判定依据: failing Delivered=%d, Failed=%d, 合计=%d == 发布总数 %d",
		fs.Delivered, fs.Failed, fs.Delivered+fs.Failed, total)
	if fs.Failed == 0 || fs.Total() != total {
		t.Fatalf("判定失败: failing 统计异常: %+v", fs)
	}
}
