// Package publisher owns the ZeroMQ socket on exactly one locked OS thread.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

type Config struct {
	Endpoint string `json:"endpoint"`
	SendHWM  int    `json:"send_hwm"`
}

func (c Config) Validate() error {
	if (!strings.HasPrefix(c.Endpoint, "tcp://") && !strings.HasPrefix(c.Endpoint, "ipc://")) || c.SendHWM < 1 || c.SendHWM > 1000000 {
		return errors.New("require tcp:// or ipc:// ZMQ endpoint and send_hwm in 1..1000000")
	}
	return nil
}

// Run signals ready only once the socket is bound. Errors are fatal: a supervisor
// should restart the process, rather than repeatedly losing multipart framing.
func Run(ctx context.Context, c Config, in <-chan market.Event, s *monitor.Stats, ready chan<- error) (result error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	notified := false
	defer func() {
		if !notified {
			ready <- result
		}
	}()
	if err := c.Validate(); err != nil {
		return err
	}
	zctx, err := zmq.NewContext()
	if err != nil {
		return err
	}
	defer zctx.Term()
	zctx.SetRetryAfterEINTR(true)
	pub, err := zctx.NewSocket(zmq.PUB)
	if err != nil {
		return err
	}
	defer pub.Close()
	for _, set := range []func() error{
		func() error { return pub.SetLinger(0) },
		func() error { return pub.SetSndhwm(c.SendHWM) },
		func() error { return pub.SetSndtimeo(100 * time.Millisecond) },
		func() error { return pub.SetTcpKeepalive(1) },
		func() error { return pub.SetMaxmsgsize(1 << 20) },
	} {
		if err := set(); err != nil {
			return err
		}
	}
	if err := pub.Bind(c.Endpoint); err != nil {
		return err
	}
	s.PublisherUp.Store(true)
	defer s.PublisherUp.Store(false)
	ready <- nil
	notified = true
	for {
		// Bound shutdown time even when the input channel remains nonempty.
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-in:
			if !ok {
				return nil
			}
			b, err := json.Marshal(e)
			if err != nil {
				s.PublisherErrors.Add(1)
				return err
			}
			// PUB drops for a subscriber whose HWM is full, and never waits for it.
			// Success means accepted by libzmq, NOT acknowledged by subscribers.
			if _, err := pub.SendMessage(e.Topic(), b); err != nil {
				s.PublisherErrors.Add(1)
				return err
			}
			s.Sent.Add(1)
		}
	}
}
