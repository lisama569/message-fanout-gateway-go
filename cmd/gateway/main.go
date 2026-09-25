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
	flag.Parse()

	cfg := gateway.Config{
		RetainPerTopic:   *retain,
		SubscriberBuffer: *buf,
		Policy:           gateway.DropNewest,
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
	// 等待 slow 排空后再校验守恒
	published := broker.LatestSeq("orders")
	deadline := time.Now().Add(10 * time.Second)
	for slow.Stats().Total() != published && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	ss := slow.Stats()
	fmt.Printf("[stats] slow: delivered=%d dropped=%d failed=%d total=%d (published=%d)\n",
		ss.Delivered, ss.Dropped, ss.Failed, ss.Total(), published)
	if ss.Total() == published {
		fmt.Println("[done] 守恒校验通过: delivered+dropped+failed == published")
	} else {
		fmt.Println("[done] 守恒校验失败!")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
