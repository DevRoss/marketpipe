package publisher

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

// Outputs assigns a distinct socket and HWM to each enabled event kind.
type Outputs struct {
	Endpoints map[string]string `json:"endpoints"`
	SendHWM   int               `json:"send_hwm"`
}

func Defaults() Outputs {
	return Outputs{Endpoints: map[string]string{
		"quote": "tcp://127.0.0.1:5556", "trade": "tcp://127.0.0.1:5557",
		"agg_trade": "tcp://127.0.0.1:5558", "heartbeat": "tcp://127.0.0.1:5559",
	}, SendHWM: 10000}
}
func (o Outputs) Validate() error {
	if len(o.Endpoints) == 0 {
		return errors.New("zmq.endpoints must not be empty")
	}
	seen := make(map[string]bool)
	for kind, endpoint := range o.Endpoints {
		switch kind {
		case "quote", "trade", "agg_trade", "heartbeat":
		default:
			return fmt.Errorf("unknown ZMQ event kind %q", kind)
		}
		if err := (Config{Endpoint: endpoint, SendHWM: o.SendHWM}).Validate(); err != nil {
			return fmt.Errorf("zmq.endpoints.%s: %w", kind, err)
		}
		if seen[endpoint] {
			return fmt.Errorf("ZMQ endpoint %q is shared: each kind requires a distinct endpoint", endpoint)
		}
		seen[endpoint] = true
	}
	return nil
}

// RunAll owns the lifecycle of independent publishers. Startup succeeds only
// after every bind. Any failure cancels and joins all siblings. Closing all
// inputs drains all queues; cancelling ctx bounds the total drain time.
func RunAll(ctx context.Context, c Outputs, inputs map[string]<-chan market.Event, stats *monitor.Stats, ready chan<- error) error {
	if err := c.Validate(); err != nil {
		ready <- err
		return err
	}
	if len(inputs) == 0 {
		err := errors.New("no publisher inputs")
		ready <- err
		return err
	}
	kinds := make([]string, 0, len(inputs))
	for kind, in := range inputs {
		if c.Endpoints[kind] == "" || in == nil || stats.Pipelines[kind] == nil {
			err := fmt.Errorf("missing endpoint, queue or stats for %q", kind)
			ready <- err
			return err
		}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, len(kinds))
	started := 0
	for _, kind := range kinds {
		bound := make(chan error, 1)
		go func(kind string) {
			err := Run(ctx, Config{Endpoint: c.Endpoints[kind], SendHWM: c.SendHWM}, inputs[kind], stats.Pipelines[kind], bound)
			if err != nil {
				err = fmt.Errorf("publisher %s: %w", kind, err)
				cancel()
			}
			done <- err
		}(kind)
		started++
		if err := <-bound; err != nil {
			cancel()
			for i := 0; i < started; i++ {
				<-done
			}
			err = fmt.Errorf("publisher %s start: %w", kind, err)
			ready <- err
			return err
		}
	}
	ready <- nil
	var result error
	for i := 0; i < started; i++ {
		if err := <-done; err != nil && result == nil {
			result = err
		}
	}
	return result
}
