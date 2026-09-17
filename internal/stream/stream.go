// Package stream manages independent reconnecting WebSocket shards.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Config struct {
	DialTimeout  Duration `json:"dial_timeout"`
	PingInterval Duration `json:"ping_interval"`
	ReadTimeout  Duration `json:"read_timeout"`
	WriteTimeout Duration `json:"write_timeout"`
	MaxAge       Duration `json:"max_age"`
	BackoffMin   Duration `json:"backoff_min"`
	BackoffMax   Duration `json:"backoff_max"`
	DialInterval Duration `json:"dial_interval"`
	ReadLimit    int64    `json:"read_limit"`
}

func Defaults() Config {
	return Config{
		DialTimeout: Duration(10 * time.Second), PingInterval: Duration(30 * time.Second),
		ReadTimeout: Duration(90 * time.Second), WriteTimeout: Duration(5 * time.Second),
		MaxAge: Duration(23*time.Hour + 50*time.Minute), BackoffMin: Duration(time.Second),
		BackoffMax: Duration(30 * time.Second), DialInterval: Duration(250 * time.Millisecond), ReadLimit: 1 << 20,
	}
}
func (c Config) Validate() error {
	if c.DialTimeout <= 0 || c.WriteTimeout <= 0 || c.PingInterval <= 0 || c.ReadTimeout <= c.PingInterval || c.MaxAge <= 0 || c.MaxAge >= Duration(24*time.Hour) || c.BackoffMin <= 0 || c.BackoffMax < c.BackoffMin || c.BackoffMax > Duration(5*time.Minute) || c.DialInterval <= 0 || c.ReadLimit <= 0 || c.ReadLimit > 16<<20 {
		return errors.New("invalid websocket timeouts, backoff, max_age (<24h), or read_limit (<=16MiB)")
	}
	return nil
}

type Spec struct {
	Name   string
	URL    string
	Decode func([]byte, int64) (market.Event, error)
}

type Manager struct {
	Config Config
	Stats  *monitor.Stats
}

func (m *Manager) Run(ctx context.Context, specs []Spec, sink market.Sink) error {
	if err := m.Config.Validate(); err != nil {
		return err
	}
	// One shared dial token source bounds reconnect storms across all shards.
	limiter := time.NewTicker(time.Duration(m.Config.DialInterval))
	defer limiter.Stop()
	var wg sync.WaitGroup
	for _, spec := range specs {
		m.Stats.Register(spec.Name)
		wg.Add(1)
		go func(s Spec) { defer wg.Done(); m.supervise(ctx, s, sink, limiter.C) }(spec)
	}
	wg.Wait()
	return nil
}

func (m *Manager) supervise(ctx context.Context, s Spec, sink market.Sink, tokens <-chan time.Time) {
	backoff := time.Duration(m.Config.BackoffMin)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-tokens:
		}
		start := time.Now()
		err := m.session(ctx, s, sink)
		m.Stats.Disconnected(s.Name, err)
		if ctx.Err() != nil {
			return
		}
		m.Stats.Reconnects.Add(1)
		slog.Warn("websocket reconnect", "connection", s.Name, "error", err)
		if time.Since(start) >= time.Minute {
			backoff = time.Duration(m.Config.BackoffMin)
		}
		// Equal jitter: [backoff/2, backoff], avoiding a tight reconnect loop.
		delay := backoff/2 + time.Duration(rand.Int64N(int64(backoff-backoff/2)+1))
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(backoff*2, time.Duration(m.Config.BackoffMax))
	}
}

func (m *Manager) session(ctx context.Context, s Spec, sink market.Sink) error {
	cfg := m.Config
	dialer := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: time.Duration(cfg.DialTimeout), EnableCompression: false}
	conn, resp, err := dialer.DialContext(ctx, s.URL, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(cfg.ReadLimit)
	refresh := func() error { return conn.SetReadDeadline(time.Now().Add(time.Duration(cfg.ReadTimeout))) }
	if err := refresh(); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error { return refresh() })
	conn.SetPingHandler(func(payload string) error {
		if err := refresh(); err != nil {
			return err
		}
		return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(time.Duration(cfg.WriteTimeout)))
	})
	session := market.NewID()
	m.Stats.Connected(s.Name, session)
	slog.Info("websocket connected", "connection", s.Name, "session", session)
	// Gorilla permits concurrent Close and WriteControl with one reader.
	// Stop and join controller before returning to avoid leaked timers/goroutines.
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ping := time.NewTicker(time.Duration(cfg.PingInterval))
		defer ping.Stop()
		age := time.Duration(cfg.MaxAge)
		jitter := min(age/100, 5*time.Minute)
		if jitter > 0 {
			age -= time.Duration(rand.Int64N(int64(jitter)))
		}
		rotation := time.NewTimer(age)
		defer rotation.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				conn.Close()
				return
			case <-rotation.C:
				conn.Close()
				return
			case <-ping.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte("mp"), time.Now().Add(time.Duration(cfg.WriteTimeout))); err != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	defer func() { close(done); <-stopped }()
	for {
		kind, b, err := conn.ReadMessage()
		recv := time.Now().UnixNano()
		if err != nil {
			return err
		}
		if err := refresh(); err != nil {
			return err
		}
		if kind != websocket.TextMessage {
			return errors.New("expected JSON text frame")
		}
		e, err := s.Decode(b, recv)
		if err != nil {
			m.Stats.ParseErrors.Add(1)
			// Bad/control frames cannot silently mark a subscription healthy.
			return fmt.Errorf("decode: %w", err)
		}
		e.SourceSession = session
		m.Stats.Tick(s.Name)
		sink.Emit(e)
	}
}
