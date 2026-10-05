// 本地可运行的消息分发网关演示：
// 内置事件源持续注入事件，展示订阅分发、慢消费者背压与游标恢复。
package main

import (
	"flag"
	"fmt"
	"time"

	gateway "github.com/lisama569/message-fanout-gateway-go/gateway"
)

func main() {
	retain := flag.Int("retain", 64, "每主题日志保留条数（恢复窗口）")
	buf := flag.Int("buffer", 8, "每订阅者缓冲容量")
	total := flag.Int("n", 30, "注入事件总数")
	interval := flag.Duration("interval", 50*time.Millisecond, "事件注入间隔")
	drain := flag.Duration("drain", 2*time.Second, "退订宽限时间（0=立即收敛，<0=默认值2s）")
	flag.Parse()

	cfg := gateway.Config{
		RetainPerTopic:   *retain,
		SubscriberBuffer: *buf,
		Policy:           gateway.DropNewest,
		DrainTimeout:     *drain,
	}
	broker := gateway.NewBroker(cfg)
	src := gateway.NewSource(broker, "orders")
	fmt.Printf("[config] %s\n", cfg)

	// 快订阅者：实时打印
	_, err := broker.Subscribe("orders", "fast", 0, func(m gateway.Message) error {
		fmt.Printf("[fast]   seq=%d payload=%s\n", m.Seq, m.Payload)
		return nil
	})
	must(err)

	// 慢订阅者：每条处理耗时为注入间隔的 10 倍，必然触发背压
	slow, err := broker.Subscribe("orders", "slow", 0, func(m gateway.Message) error {
		time.Sleep(*interval * 10)
		fmt.Printf("[slow]   seq=%d payload=%s\n", m.Seq, m.Payload)
		return nil
	})
	must(err)

	// 可恢复订阅者：先消费一部分，断开后携带 cursor 重连
	resumable, err := broker.Subscribe("orders", "resumable", 0, func(m gateway.Message) error {
		return nil
	})
	must(err)

	for i := 0; i < *total/2; i++ {
		_, _ = src.Emit("orders", fmt.Sprintf("order-%d", i))
		time.Sleep(*interval)
	}

	// 模拟中断：记录 cursor 并断开
	cursor := resumable.Cursor()
	broker.Unsubscribe("orders", "resumable")
	fmt.Printf("[resume] 断开于 cursor=%d，继续注入事件...\n", cursor)

	for i := *total / 2; i < *total; i++ {
		_, _ = src.Emit("orders", fmt.Sprintf("order-%d", i))
		time.Sleep(*interval)
	}

	// 携带 cursor 重连，从 cursor+1 继续
	resub, err := broker.Subscribe("orders", "resumable", cursor, func(m gateway.Message) error {
		return nil
	})
	must(err)
	time.Sleep(200 * time.Millisecond)
	rs := resub.Stats()
	fmt.Printf("[resume] 从 cursor=%d 恢复：delivered=%d dropped=%d（backlog=%d 条，buffer=%d，超出部分按策略丢弃）\n",
		cursor, rs.Delivered, rs.Dropped, broker.LatestSeq("orders")-uint64(cursor), *buf)

	// 演示非法游标被明确拒绝：越界（倒退）游标
	_, err = broker.Subscribe("orders", "bad", gateway.Cursor(broker.LatestSeq("orders")+100), nil)
	if gerr, ok := err.(*gateway.Error); ok {
		fmt.Printf("[resume] 非法游标被拒绝: kind=%s (%v)\n", gerr.Kind, gerr.Message)
	} else {
		fmt.Println("[resume] 意外：非法游标未被拒绝")
	}
	// 优雅下线：退订 slow，宽限期内继续处理积压，到期未做完的计为收敛丢弃
	fmt.Printf("[drain] 退订 slow，宽限 %v 收敛积压...\n", *drain)
	broker.Unsubscribe("orders", "slow")
	<-slow.Finalized()

	// 会话最终账目：退订收敛后仍可查询
	var rep gateway.SessionReport
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok := broker.SessionReport("orders", "slow"); ok {
			rep = r
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	published := broker.LatestSeq("orders")
	fmt.Printf("[drain] slow 最终账目: delivered=%d failed=%d dropped=%d (背压=%d 收敛=%d) cursor=%d\n",
		rep.Stats.Delivered, rep.Stats.Failed, rep.Stats.Dropped,
		rep.Stats.DroppedBackpressure, rep.Stats.DroppedDrain, rep.Cursor)
	if rep.Stats.Total() == published {
		fmt.Println("[done] 守恒校验通过: delivered+dropped+failed == published")
	} else {
		fmt.Printf("[done] 守恒校验失败! total=%d published=%d\n", rep.Stats.Total(), published)
	}

	// 主题聚合视角：含已离开订阅者的贡献
	ts := broker.TopicStats("orders")
	fmt.Printf("[stats] topic 聚合: published=%d delivered=%d dropped=%d(背压=%d 收敛=%d) failed=%d(在线=%d 宽限=%d) 活跃=%d 收敛中=%d 已离开会话=%d\n",
		ts.Published, ts.Delivered, ts.Dropped, ts.DroppedBackpressure, ts.DroppedDrain,
		ts.Failed, ts.FailedOnline, ts.FailedDrain,
		ts.ActiveSubscribers, ts.DrainingSubscribers, ts.DepartedSessions)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
