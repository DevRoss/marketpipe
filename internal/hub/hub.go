package hub

import (
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
	"sync"
)

// Hub serializes sequence assignment AND enqueue to preserve per-topic order.
// It never blocks the WebSocket reader on serialization or consumer IO.
type Hub struct {
	mu       sync.Mutex
	seq      map[string]uint64
	instance string
	queue    chan market.Event
	stats    *monitor.Stats
}

func New(capacity int, stats *monitor.Stats) *Hub {
	h := &Hub{seq: make(map[string]uint64), instance: market.NewID(), queue: make(chan market.Event, capacity), stats: stats}
	stats.QueueLen = func() int { return len(h.queue) }
	return h
}
func (h *Hub) Events() <-chan market.Event { return h.queue }

// Close must be called only after all producers have stopped.
func (h *Hub) Close() { close(h.queue) }
func (h *Hub) Emit(e market.Event) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	topic := e.Topic()
	h.seq[topic]++
	e.Schema = 1
	e.Instance = h.instance
	e.Seq = h.seq[topic]
	h.stats.Received.Add(1)
	select {
	case h.queue <- e:
		return true
	default:
		h.stats.Dropped.Add(1)
		return false
	}
}
