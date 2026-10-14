package hub

import (
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

// Router is immutable after construction. Each kind owns its queue, sequence
// lock and counters; no shared dispatch queue or lock sits on the hot path.
type Router struct{ lanes map[string]*Hub }

func NewRouter(capacity int, kinds []string, stats *monitor.Stats) *Router {
	r := &Router{lanes: make(map[string]*Hub, len(kinds))}
	instance := market.NewID()
	for _, kind := range kinds {
		s := monitor.New()
		stats.Pipelines[kind] = s
		h := New(capacity, s)
		h.instance = instance
		r.lanes[kind] = h
	}
	return r
}

func (r *Router) Emit(e market.Event) bool {
	h, ok := r.lanes[e.Kind]
	if !ok {
		return false
	}
	return h.Emit(e)
}
func (r *Router) Inputs() map[string]<-chan market.Event {
	out := make(map[string]<-chan market.Event, len(r.lanes))
	for kind, h := range r.lanes {
		out[kind] = h.Events()
	}
	return out
}

// Close must be called only after all producers have stopped.
func (r *Router) Close() {
	for _, h := range r.lanes {
		h.Close()
	}
}
