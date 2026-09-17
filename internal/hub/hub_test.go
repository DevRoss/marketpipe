package hub

import (
	"marketpipe/internal/monitor"
	"marketpipe/pkg/market"
	"sync"
	"testing"
)

func TestOverflowGap(t *testing.T) {
	s := monitor.New()
	h := New(1, s)
	e := market.Event{Symbol: "BTCUSDT", Kind: "quote"}
	if !h.Emit(e) || h.Emit(e) {
		t.Fatal("expected bounded nonblocking overflow")
	}
	if v := <-h.Events(); v.Seq != 1 {
		t.Fatal(v)
	}
	if !h.Emit(e) {
		t.Fatal("queue did not recover")
	}
	if v := <-h.Events(); v.Seq != 3 {
		t.Fatal("drop must be visible as sequence gap", v)
	}
	if s.Dropped.Load() != 1 {
		t.Fatal(s.Dropped.Load())
	}
}
func TestConcurrentTopicOrdering(t *testing.T) {
	h := New(8000, monitor.New())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				h.Emit(market.Event{Symbol: "BTCUSDT", Kind: "quote"})
			}
		}()
	}
	wg.Wait()
	h.Close()
	var previous uint64
	for e := range h.Events() {
		if e.Seq != previous+1 {
			t.Fatalf("out of order: %d -> %d", previous, e.Seq)
		}
		previous = e.Seq
	}
	if previous != 8000 {
		t.Fatal(previous)
	}
}
