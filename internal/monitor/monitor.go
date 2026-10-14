package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"marketpipe/pkg/market"
)

type Stats struct {
	// Pipelines is populated before serving or starting producers and is then immutable.
	Pipelines       map[string]*Stats
	Received        atomic.Uint64
	Dropped         atomic.Uint64
	Sent            atomic.Uint64
	ParseErrors     atomic.Uint64
	Reconnects      atomic.Uint64
	PublisherErrors atomic.Uint64
	PublisherUp     atomic.Bool
	QueueLen        func() int
	mu              sync.RWMutex
	connections     map[string]market.Connection
}

func New() *Stats {
	return &Stats{Pipelines: make(map[string]*Stats), connections: make(map[string]market.Connection)}
}

func (s *Stats) Register(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[name] = market.Connection{Name: name}
}
func (s *Stats) Connected(name, session string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[name] = market.Connection{Name: name, Session: session, Connected: true}
}
func (s *Stats) Disconnected(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.connections[name]
	c.Connected = false
	c.ReceivedInSession = false
	if err != nil {
		c.LastError = err.Error()
	}
	s.connections[name] = c
}
func (s *Stats) Tick(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.connections[name]
	c.LastEventMS = time.Now().UnixMilli()
	c.ReceivedInSession = true
	s.connections[name] = c
}
func (s *Stats) Connections() []market.Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]market.Connection, 0, len(s.connections))
	for _, c := range s.connections {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (s *Stats) Ready() bool {
	if !s.PublishersReady() {
		return false
	}
	cs := s.Connections()
	if len(cs) == 0 {
		return false
	}
	for _, c := range cs {
		if !c.Connected || !c.ReceivedInSession {
			return false
		}
	}
	return true
}
func (s *Stats) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.Ready() {
			http.Error(w, "waiting for publisher and data on every shard", 503)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Connections())
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, v := range []struct {
			name  string
			value uint64
		}{
			{"received_total", s.TotalReceived()}, {"ingress_dropped_total", s.TotalDropped()},
			{"publish_calls_total", s.TotalSent()}, {"parse_errors_total", s.ParseErrors.Load()},
			{"reconnects_total", s.Reconnects.Load()}, {"publisher_errors_total", s.TotalPublisherErrors()},
		} {
			fmt.Fprintf(w, "# TYPE marketpipe_%s counter\nmarketpipe_%s %d\n", v.name, v.name, v.value)
		}
		fmt.Fprintf(w, "# TYPE marketpipe_queue_length gauge\nmarketpipe_queue_length %d\n", s.TotalQueueLen())
		up := 0
		if s.PublishersReady() {
			up = 1
		}
		fmt.Fprintf(w, "marketpipe_publisher_up %d\n", up)
		for kind, lane := range s.Pipelines {
			laneUp := 0
			if lane.PublisherUp.Load() {
				laneUp = 1
			}
			fmt.Fprintf(w, "marketpipe_channel_publisher_up{kind=%q} %d\nmarketpipe_channel_queue_length{kind=%q} %d\n", kind, laneUp, kind, lane.TotalQueueLen())
			fmt.Fprintf(w, "marketpipe_channel_received_total{kind=%q} %d\nmarketpipe_channel_ingress_dropped_total{kind=%q} %d\nmarketpipe_channel_publish_calls_total{kind=%q} %d\nmarketpipe_channel_publisher_errors_total{kind=%q} %d\n", kind, lane.Received.Load(), kind, lane.Dropped.Load(), kind, lane.Sent.Load(), kind, lane.PublisherErrors.Load())
		}
		for _, c := range s.Connections() {
			up = 0
			if c.Connected {
				up = 1
			}
			fmt.Fprintf(w, "marketpipe_connection_up{name=%q} %d\nmarketpipe_last_event_time_seconds{name=%q} %.3f\n", c.Name, up, c.Name, float64(c.LastEventMS)/1000)
		}
	})
	return mux
}

func (s *Stats) PublishersReady() bool {
	if len(s.Pipelines) == 0 {
		return s.PublisherUp.Load()
	}
	for _, lane := range s.Pipelines {
		if !lane.PublisherUp.Load() {
			return false
		}
	}
	return true
}
func (s *Stats) total(get func(*Stats) uint64) uint64 {
	total := get(s)
	for _, lane := range s.Pipelines {
		total += get(lane)
	}
	return total
}
func (s *Stats) TotalReceived() uint64 {
	return s.total(func(s *Stats) uint64 { return s.Received.Load() })
}
func (s *Stats) TotalDropped() uint64 {
	return s.total(func(s *Stats) uint64 { return s.Dropped.Load() })
}
func (s *Stats) TotalSent() uint64 { return s.total(func(s *Stats) uint64 { return s.Sent.Load() }) }
func (s *Stats) TotalPublisherErrors() uint64 {
	return s.total(func(s *Stats) uint64 { return s.PublisherErrors.Load() })
}
func (s *Stats) TotalQueueLen() int {
	n := 0
	if s.QueueLen != nil {
		n = s.QueueLen()
	}
	for _, lane := range s.Pipelines {
		if lane.QueueLen != nil {
			n += lane.QueueLen()
		}
	}
	return n
}
