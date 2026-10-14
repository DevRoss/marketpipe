package monitor

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPipelineReadinessAndMetrics(t *testing.T) {
	s := New()
	q, tr := New(), New()
	s.Pipelines["quote"], s.Pipelines["trade"] = q, tr
	s.Register("source")
	s.Connected("source", "session")
	s.Tick("source")
	q.PublisherUp.Store(true)
	if s.Ready() {
		t.Fatal("all publishers must be ready")
	}
	tr.PublisherUp.Store(true)
	if !s.Ready() {
		t.Fatal("expected ready")
	}
	q.Received.Add(3)
	q.Dropped.Add(1)
	tr.Received.Add(2)
	tr.Sent.Add(2)
	q.QueueLen = func() int { return 2 }
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{"marketpipe_received_total 5\n", "marketpipe_ingress_dropped_total 1\n", "marketpipe_queue_length 2\n", `marketpipe_channel_ingress_dropped_total{kind="quote"} 1`, `marketpipe_channel_ingress_dropped_total{kind="trade"} 0`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("missing %s in %s", want, w.Body.String())
		}
	}
	q.PublisherUp.Store(false)
	if s.Ready() {
		t.Fatal("stopped publisher must make service unready")
	}
}
