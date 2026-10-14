package hub

import (
	"sync"
	"testing"

	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
)

func TestRouterIsolation(t *testing.T) {
	stats := monitor.New()
	r := NewRouter(1, []string{"quote", "trade", "agg_trade", "heartbeat"}, stats)
	inputs := r.Inputs()
	if !r.Emit(market.Event{Kind: "quote"}) || r.Emit(market.Event{Kind: "quote"}) {
		t.Fatal("expected quote overflow")
	}
	var instance string
	for _, kind := range []string{"trade", "agg_trade", "heartbeat", "quote"} {
		if kind != "quote" && !r.Emit(market.Event{Kind: kind}) {
			t.Fatalf("quote overflow affected %s", kind)
		}
		e := <-inputs[kind]
		if e.Kind != kind || e.Seq != 1 || e.Schema != 1 {
			t.Fatalf("incorrect route: %+v", e)
		}
		if instance != "" && instance != e.Instance {
			t.Fatal("lanes must share a process instance")
		}
		instance = e.Instance
	}
	if !r.Emit(market.Event{Kind: "quote"}) {
		t.Fatal("queue did not recover")
	}
	if e := <-inputs["quote"]; e.Seq != 3 {
		t.Fatal("missing gap after drop", e)
	}
	if r.Emit(market.Event{Kind: "unknown"}) {
		t.Fatal("unknown type accepted")
	}
	if stats.TotalDropped() != 1 || stats.TotalReceived() != 6 || stats.Pipelines["trade"].Dropped.Load() != 0 {
		t.Fatal("incorrect per-kind counters")
	}
	r.Close()
	for kind, in := range inputs {
		if _, ok := <-in; ok {
			t.Fatalf("%s not closed", kind)
		}
	}
}

func TestRouterConcurrentOrdering(t *testing.T) {
	kinds := []string{"quote", "trade", "agg_trade", "heartbeat"}
	r := NewRouter(1000, kinds, monitor.New())
	var wg sync.WaitGroup
	for _, kind := range kinds {
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(kind string) {
				defer wg.Done()
				for j := 0; j < 250; j++ {
					if !r.Emit(market.Event{Kind: kind, Symbol: "BTCUSDT"}) {
						t.Error("unexpected drop")
					}
				}
			}(kind)
		}
	}
	wg.Wait()
	r.Close()
	for kind, in := range r.Inputs() {
		var seq uint64
		for e := range in {
			seq++
			if e.Kind != kind || e.Seq != seq {
				t.Fatalf("misroute/reorder: %+v", e)
			}
		}
		if seq != 1000 {
			t.Fatal("lost events", kind, seq)
		}
	}
}
