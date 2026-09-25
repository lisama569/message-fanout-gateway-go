// 本地演示：启动网关，注册快/慢两个订阅者，运行可控事件源，
// 展示顺序投递、慢消费者隔离与统计不变式。
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/lisama569/message-fanout-gateway-go/internal/gateway"
)

func main() {
	duration := flag.Duration("duration", 3*time.Second, "事件源运行时长")
	interval := flag.Duration("interval", 5*time.Millisecond, "事件产生间隔")
	slowEvery := flag.Duration("slow-every", 20*time.Millisecond, "慢订阅者每条消息的处理耗时")
	flag.Parse()

	cfg := gateway.DefaultConfig()
	cfg.SubscriberBuffer = 16 // 小缓冲，便于演示慢消费者丢弃策略
	broker := gateway.NewBroker(cfg)
	defer broker.Close()

	fast, err := broker.Subscribe("orders", 0, func(m gateway.Message) error {
		return nil
	})
	must(err)
	slow, err := broker.Subscribe("orders", 0, func(m gateway.Message) error {
		time.Sleep(*slowEvery) // 模拟慢消费者
		return nil
	})
	must(err)

	src := gateway.NewSource(broker, "orders")
	done := make(chan struct{})
	go func() { src.Run(*interval); close(done) }()
	time.Sleep(*duration)
	src.Stop()
	<-done

	// 等待慢订阅者排空缓冲：直到其 delivered+dropped 覆盖全部已发布消息。
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := slow.Stats()
		if s.Delivered+s.Dropped >= broker.Stats().Published {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	fs, ss, bs := fast.Stats(), slow.Stats(), broker.Stats()
	fmt.Printf("fast subscriber : delivered=%d dropped=%d failed=%d\n", fs.Delivered, fs.Dropped, fs.Failed)
	fmt.Printf("slow subscriber : delivered=%d dropped=%d failed=%d\n", ss.Delivered, ss.Dropped, ss.Failed)
	fmt.Printf("broker          : published=%d delivered=%d dropped=%d failed=%d\n",
		bs.Published, bs.Delivered, bs.Dropped, bs.Failed)
	fmt.Printf("invariant       : published*subscribers(%d) == delivered+dropped+failed(%d)\n",
		bs.Published*2, bs.Delivered+bs.Dropped+bs.Failed)

	// 演示游标恢复：用快订阅者的确认位置重新订阅。
	cur := fast.Cursor()
	resumed, err := broker.Subscribe("orders", cur, func(m gateway.Message) error { return nil })
	must(err)
	fmt.Printf("resume ok       : cursor=%d new-subscription-cursor=%d\n", cur, resumed.Cursor())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
