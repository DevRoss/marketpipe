package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

type discard struct{}

func (discard) Emit(market.Event) bool { return true }
func testConfig() Config {
	c := Defaults()
	c.PingInterval = Duration(10 * time.Millisecond)
	c.ReadTimeout = Duration(60 * time.Millisecond)
	c.BackoffMin = Duration(5 * time.Millisecond)
	c.BackoffMax = Duration(10 * time.Millisecond)
	c.DialInterval = Duration(5 * time.Millisecond)
	c.WriteTimeout = Duration(20 * time.Millisecond)
	return c
}

func TestReconnectSilentPeerAndCancel(t *testing.T) {
	var connections atomic.Int32
	up := websocket.Upgrader{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		connections.Add(1)
		// No reads: peer never processes ping, so manager must detect timeout.
		<-ctx.Done()
	}))
	defer srv.Close()
	m := Manager{Config: testConfig(), Stats: monitor.New()}
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, []Spec{{Name: "silent", URL: "ws" + strings.TrimPrefix(srv.URL, "http"), Decode: func([]byte, int64) (market.Event, error) { return market.Event{}, nil }}}, discard{})
	}()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for connections.Load() < 2 {
		select {
		case <-deadline.C:
			cancel()
			t.Fatal("silent peer was not reconnected")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation leaked a worker")
	}
	if m.Stats.Connections()[0].Connected {
		t.Fatal("stale connected state")
	}
}

func TestPongAndAgeRotation(t *testing.T) {
	var connections atomic.Int32
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		connections.Add(1)
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	cfg := testConfig()
	cfg.MaxAge = Duration(200 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := Manager{Config: cfg, Stats: monitor.New()}
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- m.Run(ctx, []Spec{{Name: "rotate", URL: "ws" + strings.TrimPrefix(srv.URL, "http"), Decode: func([]byte, int64) (market.Event, error) { return market.Event{}, nil }}}, discard{})
	}()
	for connections.Load() < 2 && time.Since(start) < 2*time.Second {
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	cancel()
	<-done
	if connections.Load() < 2 {
		t.Fatal("age rotation did not reconnect")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("pong failed to keep session alive: %s", elapsed)
	}
}
