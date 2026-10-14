package publisher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"marketpipe/internal/hub"
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

// Keep UNIX socket paths below macOS's sockaddr_un limit.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "mp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func awaitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("publisher failed to stop")
		return nil
	}
}

func TestGroupDrainAndCancel(t *testing.T) {
	if os.Getenv("MARKETPIPE_SKIP_ZMQ_INTEGRATION") == "1" {
		t.Skip("native libzmq socket binding explicitly disabled")
	}
	for _, drain := range []bool{true, false} {
		t.Run(map[bool]string{true: "drain", false: "cancel"}[drain], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stats := monitor.New()
			kinds := []string{"quote", "trade"}
			h := hub.NewRouter(16, kinds, stats)
			c := Outputs{Endpoints: map[string]string{}, SendHWM: 100}
			for _, kind := range kinds {
				c.Endpoints[kind] = "ipc://" + filepath.Join(socketDir(t), kind+".sock")
				h.Emit(market.Event{Kind: kind})
			}
			ready, done := make(chan error, 1), make(chan error, 1)
			go func() { done <- RunAll(ctx, c, h.Inputs(), stats, ready) }()
			if err := awaitResult(t, ready); err != nil {
				t.Fatal(err)
			}
			if !stats.PublishersReady() {
				t.Fatal("not all publishers ready")
			}
			if drain {
				h.Close()
			} else {
				cancel()
			}
			if err := awaitResult(t, done); err != nil {
				t.Fatal(err)
			}
			for _, s := range stats.Pipelines {
				if s.PublisherUp.Load() {
					t.Fatal("publisher still up")
				}
			}
			if drain && stats.TotalSent() != 2 {
				t.Fatal("did not drain", stats.TotalSent())
			}
		})
	}
}

func TestGroupPartialStartupFailure(t *testing.T) {
	if os.Getenv("MARKETPIPE_SKIP_ZMQ_INTEGRATION") == "1" {
		t.Skip("native libzmq socket binding explicitly disabled")
	}
	stats := monitor.New()
	h := hub.NewRouter(1, []string{"quote", "trade"}, stats)
	// quote sorts first and binds; trade then fails on a nonexistent IPC directory.
	c := Outputs{Endpoints: map[string]string{
		"quote": "ipc://" + filepath.Join(socketDir(t), "quote.sock"),
		"trade": "ipc://" + filepath.Join(socketDir(t), "missing", "trade.sock"),
	}, SendHWM: 10}
	ready, done := make(chan error, 1), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- RunAll(ctx, c, h.Inputs(), stats, ready) }()
	if awaitResult(t, ready) == nil || awaitResult(t, done) == nil {
		t.Fatal("expected bind failure")
	}
	for _, s := range stats.Pipelines {
		if s.PublisherUp.Load() {
			t.Fatal("sibling leaked")
		}
	}
}

func TestGroupValidation(t *testing.T) {
	for _, missing := range []string{"inputs", "endpoint", "queue", "stats", "config"} {
		t.Run(missing, func(t *testing.T) {
			stats := monitor.New()
			h := hub.NewRouter(1, []string{"quote"}, stats)
			in := h.Inputs()
			c := Defaults()
			switch missing {
			case "inputs":
				in = nil
			case "endpoint":
				delete(c.Endpoints, "quote")
			case "queue":
				in["quote"] = nil
			case "stats":
				delete(stats.Pipelines, "quote")
			case "config":
				c.SendHWM = 0
			}
			ready := make(chan error, 1)
			if err := RunAll(context.Background(), c, in, stats, ready); err == nil {
				t.Fatal("expected error")
			}
			if <-ready == nil {
				t.Fatal("startup must report failure")
			}
		})
	}
}
