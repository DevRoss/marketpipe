// A native Go SUB example. Run separately from the publisher process.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
	"marketpipe/pkg/market"
)

func main() {
	endpoint := flag.String("endpoint", "tcp://127.0.0.1:5556", "publisher address")
	topic := flag.String("topic", "md.v1.binance.usdm.BTCUSDT.quote|", "ZeroMQ topic prefix (empty = all)")
	flag.Parse()
	if err := run(*endpoint, *topic); err != nil {
		log.Fatal(err)
	}
}
func run(endpoint, topic string) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	zctx, err := zmq.NewContext()
	if err != nil {
		return err
	}
	defer zctx.Term()
	zctx.SetRetryAfterEINTR(true)
	sub, err := zctx.NewSocket(zmq.SUB)
	if err != nil {
		return err
	}
	defer sub.Close()
	for _, f := range []func() error{
		func() error { return sub.SetLinger(0) }, func() error { return sub.SetRcvhwm(10000) },
		func() error { return sub.SetRcvtimeo(500 * time.Millisecond) }, func() error { return sub.SetMaxmsgsize(1 << 20) },
		func() error { return sub.SetSubscribe(topic) },
		func() error { return sub.Connect(endpoint) },
	} {
		if err := f(); err != nil {
			return err
		}
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	type cursor struct {
		instance, session string
		seq               uint64
	}
	last := map[string]cursor{}
	lastMessage := time.Now()
	warned := false
	for {
		select {
		case <-signals:
			return nil
		default:
		}
		frames, err := sub.RecvMessageBytes(0)
		if err != nil {
			if zmq.AsErrno(err) == zmq.Errno(syscall.EAGAIN) {
				if !warned && time.Since(lastMessage) > 5*time.Second {
					log.Print("no messages on selected channel for 5 seconds; check heartbeat or HTTP health")
					warned = true
				}
				continue
			}
			return err
		}
		lastMessage = time.Now()
		warned = false
		if len(frames) != 2 {
			return fmt.Errorf("expected 2 frames, got %d", len(frames))
		}
		var e market.Event
		if err := json.Unmarshal(frames[1], &e); err != nil {
			return err
		}
		if e.Schema != 1 || e.Topic() != string(frames[0]) {
			return fmt.Errorf("invalid schema/topic")
		}
		key := string(frames[0])
		prev, exists := last[key]
		if exists {
			if prev.instance != e.Instance {
				log.Printf("gateway restarted: %s", key)
			} else if e.Seq != prev.seq+1 {
				log.Printf("sequence gap/reorder %s: %d -> %d", key, prev.seq, e.Seq)
			}
			if prev.session != e.SourceSession {
				log.Printf("source reconnected: %s; upstream data may be missing", key)
			}
		}
		last[key] = cursor{e.Instance, e.SourceSession, e.Seq}
		fmt.Printf("%s %s\n", frames[0], frames[1])
	}
}
