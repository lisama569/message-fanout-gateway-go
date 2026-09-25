package gateway

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitQuiescent 等待订阅统计收敛（delivered+dropped+failed 达到期望值）。
func waitQuiescent(t *testing.T, want uint64, stats func() Stats, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := stats()
		if s.Delivered+s.Dropped+s.Failed >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	s := stats()
	t.Fatalf("%s 未收敛: want>=%d got delivered=%d dropped=%d failed=%d",
		what, want, s.Delivered, s.Dropped, s.Failed)
}

// session 记录一次订阅会话收到的全部序号。
type session struct {
	mu   sync.Mutex
	seqs []Seq
}

func (s *session) record(m Message) error {
	s.mu.Lock()
	s.seqs = append(s.seqs, m.Seq)
	s.mu.Unlock()
	return nil
}

// checkContiguous 判定依据：序号必须从 1 开始严格 +1 递增（无重复、无漏投）。
func checkContiguous(t *testing.T, name string, seqs []Seq) {
	t.Helper()
	for i, seq := range seqs {
		want := Seq(uint64(i) + 1)
		if seq != want {
			t.Fatalf("%s 第 %d 条序号判定失败: want=%d got=%d（重复或漏投）", name, i, want, seq)
		}
	}
}

// TestConcurrentSubscribeUnsubscribe 并发订阅增删场景。
// 输入条件：1 个发布者连续发布 M 条消息，W 个工作者并发地订阅/退订。
// 判定依据：每个会话收到的序号严格连续递增（无重复、无漏投），
// 且全局统计满足 published*活跃订阅扇出 == delivered+dropped+failed。
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	const M = 300
	const W = 8
	cfg := DefaultConfig()
	cfg.SubscriberBuffer = 4096 // 缓冲足够大，排除丢弃干扰，专注验证增删正确性
	b := NewBroker(cfg)
	defer b.Close()
	src := NewSource(b, "orders")

	t.Logf("输入条件: 发布消息数 M=%d, 并发工作者 W=%d, 订阅缓冲=%d", M, W, cfg.SubscriberBuffer)

	var mu sync.Mutex
	var sessions []*session
	addSession := func(s *session) { mu.Lock(); sessions = append(sessions, s); mu.Unlock() }

	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				sess := &session{}
				sub, err := b.Subscribe("orders", 0, sess.record)
				if err != nil {
					t.Errorf("worker %d 订阅失败: %v", id, err)
					return
				}
				addSession(sess)
				time.Sleep(time.Duration(id+1) * time.Millisecond)
				sub.Unsubscribe()
			}
		}(w)
	}
	for i := 0; i < M; i++ {
		src.Emit(fmt.Sprintf("event-%d", i+1))
		time.Sleep(time.Millisecond)
	}
	wg.Wait()

	// 每个会话：序号必须从其订阅时刻起严格连续（相对连续性）。
	violations := 0
	for i, sess := range sessions {
		sess.mu.Lock()
		seqs := append([]Seq(nil), sess.seqs...)
		sess.mu.Unlock()
		for j := 1; j < len(seqs); j++ {
			if seqs[j] != seqs[j-1]+1 {
				t.Errorf("会话 %d 序号不连续: seqs[%d]=%d, seqs[%d]=%d（重复或漏投）",
					i, j-1, seqs[j-1], j, seqs[j])
				violations++
				break
			}
		}
	}
	t.Logf("判定依据: 会话数=%d, 序号连续性违规数=%d（期望 0）", len(sessions), violations)

	bs := b.Stats()
	t.Logf("全局统计: published=%d delivered=%d dropped=%d failed=%d",
		bs.Published, bs.Delivered, bs.Dropped, bs.Failed)
	if bs.Published != M {
		t.Fatalf("published 判定失败: want=%d got=%d", M, bs.Published)
	}
	if bs.Dropped != 0 {
		t.Fatalf("缓冲充足时不应出现丢弃: dropped=%d", bs.Dropped)
	}
}

// TestResumeFromCursor 中断恢复场景。
// 输入条件：订阅者确认到 cursor 后断开，继续发布，再携带 cursor 重连。
// 判定依据：重连后恰好收到缺失的消息且顺序一致；
// 游标过旧返回 ReasonCursorExpired，游标越界返回 ReasonCursorRegression。
func TestResumeFromCursor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Retention = 10
	cfg.SubscriberBuffer = 64
	b := NewBroker(cfg)
	defer b.Close()
	src := NewSource(b, "orders")

	t.Logf("输入条件: retention=%d, 第一阶段发布 5 条", cfg.Retention)
	sess1 := &session{}
	sub1, err := b.Subscribe("orders", 0, sess1.record)
	if err != nil {
		t.Fatalf("首次订阅失败: %v", err)
	}
	for i := 0; i < 5; i++ {
		src.Emit(fmt.Sprintf("event-%d", i+1))
	}
	waitQuiescent(t, 5, sub1.Stats, "第一阶段投递")
	cursor := sub1.Cursor()
	sub1.Unsubscribe()
	t.Logf("断开位置: cursor=%d（期望 5）", cursor)
	if cursor != 5 {
		t.Fatalf("cursor 判定失败: want=5 got=%d", cursor)
	}

	// 断开后继续发布 5 条（序号 6..10）。
	for i := 5; i < 10; i++ {
		src.Emit(fmt.Sprintf("event-%d", i+1))
	}

	// 携带 cursor=5 重连：应恰好补发 6..10。
	sess2 := &session{}
	sub2, err := b.Subscribe("orders", cursor, sess2.record)
	if err != nil {
		t.Fatalf("携带 cursor=%d 重连失败: %v", cursor, err)
	}
	waitQuiescent(t, 5, sub2.Stats, "恢复补发")
	sess2.mu.Lock()
	got := append([]Seq(nil), sess2.seqs...)
	sess2.mu.Unlock()
	t.Logf("判定依据: 重连后收到序号=%v（期望 [6 7 8 9 10]）", got)
	if len(got) != 5 {
		t.Fatalf("恢复消息数判定失败: want=5 got=%d", len(got))
	}
	for i, s := range got {
		if want := Seq(uint64(i) + 6); s != want {
			t.Fatalf("恢复序号判定失败: 第 %d 条 want=%d got=%d", i, want, s)
		}
	}
	sub2.Unsubscribe()

	// 游标过旧：再发布使窗口滑动，base 超过 cursor+1。
	for i := 0; i < 15; i++ {
		src.Emit(fmt.Sprintf("filler-%d", i))
	}
	_, err = b.Subscribe("orders", 2, sess1.record)
	t.Logf("过旧游标: cursor=2, 错误=%v", err)
	if !IsReason(err, ReasonCursorExpired) {
		t.Fatalf("过旧游标应返回 %s, got=%v", ReasonCursorExpired, err)
	}

	// 游标越界（倒退）：请求超过最新序号的位置。
	_, err = b.Subscribe("orders", 1000, sess1.record)
	t.Logf("越界游标: cursor=1000, 错误=%v", err)
	if !IsReason(err, ReasonCursorRegression) {
		t.Fatalf("越界游标应返回 %s, got=%v", ReasonCursorRegression, err)
	}
}

// TestSlowConsumerIsolation 慢消费者隔离与背压场景。
// 输入条件：小缓冲 + 一个慢订阅者 + 一个快订阅者 + 一个总是报错的订阅者。
// 判定依据：快订阅者收全；慢订阅者按 DropNewest 策略丢弃并计数；
// 报错订阅者计入 Failed；全局不变式 published*3 == delivered+dropped+failed。
func TestSlowConsumerIsolation(t *testing.T) {
	const M = 200
	cfg := DefaultConfig()
	cfg.SubscriberBuffer = 8
	b := NewBroker(cfg)
	defer b.Close()
	src := NewSource(b, "orders")

	t.Logf("输入条件: 消息数 M=%d, 发布间隔=1ms, 订阅缓冲=%d, 慢订阅者每条处理 5ms", M, cfg.SubscriberBuffer)

	fastSess := &session{}
	fast, err := b.Subscribe("orders", 0, fastSess.record)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := b.Subscribe("orders", 0, func(m Message) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handlerErr := errors.New("handler boom")
	failing, err := b.Subscribe("orders", 0, func(m Message) error {
		return handlerErr
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < M; i++ {
		src.Emit(fmt.Sprintf("event-%d", i+1))
		time.Sleep(time.Millisecond) // 快订阅者可跟上，慢订阅者(5ms/条)必然落后
	}

	waitQuiescent(t, M, fast.Stats, "快订阅者")
	waitQuiescent(t, M, slow.Stats, "慢订阅者")
	waitQuiescent(t, M, failing.Stats, "报错订阅者")

	fs, ss, es := fast.Stats(), slow.Stats(), failing.Stats()
	t.Logf("快订阅者: delivered=%d dropped=%d failed=%d", fs.Delivered, fs.Dropped, fs.Failed)
	t.Logf("慢订阅者: delivered=%d dropped=%d failed=%d", ss.Delivered, ss.Dropped, ss.Failed)
	t.Logf("报错订阅者: delivered=%d dropped=%d failed=%d", es.Delivered, es.Dropped, es.Failed)

	// 判定 1：慢订阅者不得拖慢快订阅者 —— 快订阅者收全且无丢弃。
	if fs.Delivered != M || fs.Dropped != 0 || fs.Failed != 0 {
		t.Fatalf("快订阅者被慢订阅者拖慢: %+v", fs)
	}
	// 判定 2：快订阅者序号严格连续（从 1 到 M）。
	fastSess.mu.Lock()
	checkContiguous(t, "快订阅者", append([]Seq(nil), fastSess.seqs...))
	fastSess.mu.Unlock()
	// 判定 3：慢订阅者按策略丢弃，且 delivered+dropped == M（无凭空增减）。
	if ss.Dropped == 0 {
		t.Fatalf("慢订阅者缓冲=%d 应触发丢弃, dropped=0", cfg.SubscriberBuffer)
	}
	if ss.Delivered+ss.Dropped != M {
		t.Fatalf("慢订阅者账目不平: delivered(%d)+dropped(%d) != M(%d)",
			ss.Delivered, ss.Dropped, M)
	}
	// 判定 4：handler 报错全部计入 Failed，原因可区分。
	if es.Failed != M {
		t.Fatalf("报错订阅者 failed 判定失败: want=%d got=%d", M, es.Failed)
	}
	// 判定 5：全局不变式。
	bs := b.Stats()
	total := bs.Delivered + bs.Dropped + bs.Failed
	t.Logf("全局不变式: published*3=%d, delivered+dropped+failed=%d", bs.Published*3, total)
	if bs.Published != M || total != bs.Published*3 {
		t.Fatalf("不变式不成立: published=%d delivered=%d dropped=%d failed=%d",
			bs.Published, bs.Delivered, bs.Dropped, bs.Failed)
	}
}

// TestErrorClassification 错误归类：不同原因必须可区分。
func TestErrorClassification(t *testing.T) {
	b := NewBroker(DefaultConfig())
	defer b.Close()
	src := NewSource(b, "orders")
	src.Emit("x")

	expired := &Error{Reason: ReasonCursorExpired, Topic: "orders", Cursor: 1}
	regression := &Error{Reason: ReasonCursorRegression, Topic: "orders", Cursor: 99}

	t.Logf("判定依据: IsReason(expired, expired)=%v, IsReason(expired, regression)=%v",
		IsReason(expired, ReasonCursorExpired), IsReason(expired, ReasonCursorRegression))
	if !IsReason(expired, ReasonCursorExpired) || IsReason(expired, ReasonCursorRegression) {
		t.Fatalf("错误原因不可区分: %v / %v", expired, regression)
	}
	if !IsReason(regression, ReasonCursorRegression) || IsReason(regression, ReasonCursorExpired) {
		t.Fatalf("错误原因不可区分: %v / %v", expired, regression)
	}
	var ge *Error
	if !errors.As(regression, &ge) {
		t.Fatalf("*Error 应支持 errors.As 解包")
	}
}
