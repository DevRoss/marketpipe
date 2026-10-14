package publisher

import (
	"context"
	"errors"
	"fmt"

	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

func Defaults() Config {
	return Config{Endpoint: "tcp://127.0.0.1:5556", SendHWM: 10000}
}

// RunAll drains independent ingress queues through one socket owned by one
// locked OS thread. Subscribers select event topics on the same endpoint.
func RunAll(ctx context.Context, c Config, inputs map[string]<-chan market.Event, stats *monitor.Stats, ready chan<- error) error {
	fail := func(err error) error { ready <- err; return err }
	if err := c.Validate(); err != nil {
		return fail(err)
	}
	if len(inputs) == 0 {
		return fail(errors.New("no publisher inputs"))
	}
	for kind, in := range inputs {
		switch kind {
		case "quote", "trade", "agg_trade", "heartbeat":
		default:
			return fail(fmt.Errorf("unknown publisher kind %q", kind))
		}
		if in == nil || stats.Pipelines[kind] == nil {
			return fail(fmt.Errorf("missing queue or stats for %q", kind))
		}
	}
	return run(ctx, c, inputs, stats, ready)
}

// next selects among enabled queues without an intermediate shared queue.
// Closed inputs are disabled so remaining queues are drained on shutdown.
func next(ctx context.Context, inputs map[string]<-chan market.Event) (market.Event, bool) {
	for len(inputs) > 0 && ctx.Err() == nil {
		var e market.Event
		var ok bool
		var kind string
		select {
		case <-ctx.Done():
			return market.Event{}, false
		case e, ok = <-inputs["quote"]:
			kind = "quote"
		case e, ok = <-inputs["trade"]:
			kind = "trade"
		case e, ok = <-inputs["agg_trade"]:
			kind = "agg_trade"
		case e, ok = <-inputs["heartbeat"]:
			kind = "heartbeat"
		}
		if ok {
			return e, true
		}
		delete(inputs, kind)
	}
	return market.Event{}, false
}
