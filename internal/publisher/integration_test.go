package publisher

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"marketpipe/internal/binance"
	"marketpipe/internal/hub"
	"marketpipe/internal/monitor"
	"marketpipe/internal/stream"
	"marketpipe/pkg/market"
)

// Requires pyzmq; CI installs it explicitly. This exercises real TCP/ZMTP frames
// through the same adapter -> hub -> native libzmq publisher used by marketd.
func TestPythonEndToEnd(t *testing.T) {
	if os.Getenv("MARKETPIPE_SKIP_ZMQ_INTEGRATION") == "1" {
		t.Skip("explicitly disabled: runtime does not permit native libzmq socket binding")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		python = "python3"
	}
	if err := exec.Command(python, "-c", "import zmq").Run(); err != nil {
		t.Skip("pyzmq not installed; install examples/requirements.txt")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		// Force reconnects after a handful of ticks, proving session propagation.
		for i := 0; i < 10; i++ {
			var data string
			if strings.Contains(r.URL.RawQuery, "bookTicker") {
				data = fmt.Sprintf(`{"e":"bookTicker","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","u":%d,"b":"60000.12345678","B":"0.01000000","a":"60000.12345679","A":"2.00000000"}`, 9007199254740993+int64(i))
			} else if strings.Contains(r.URL.RawQuery, "@trade") {
				data = fmt.Sprintf(`{"e":"trade","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","t":%d,"p":"60000.12345678","q":"0.01000000","m":true,"X":"MARKET"}`, 9007199254740993+int64(i))
			} else {
				data = fmt.Sprintf(`{"e":"aggTrade","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","a":%d,"p":"60000.12345678","q":"0.01000000","f":10,"l":12,"m":true}`, 9007199254740993+int64(i))
			}
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"data":`+data+`}`)); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}))
	defer srv.Close()
	kinds := []string{"quote", "trade", "agg_trade", "heartbeat"}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "tcp://" + ln.Addr().String()
	ln.Close()
	if os.Getenv("MARKETPIPE_TEST_TRANSPORT") == "ipc" {
		endpoint = "ipc://" + filepath.Join(socketDir(t), "pub.sock")
	}
	outputs := Config{Endpoint: endpoint, SendHWM: 100}

	stats := monitor.New()
	h := hub.NewRouter(64, kinds, stats)
	ready := make(chan error, 1)
	pubDone := make(chan error, 1)
	go func() { pubDone <- RunAll(ctx, outputs, h.Inputs(), stats, ready) }()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	cfg := binance.Defaults()
	cfg.Symbols = []string{"BTCUSDT"}
	cfg.Channels = []string{"bookTicker", "trade", "aggTrade"}
	cfg.PublicURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/public/stream"
	cfg.MarketURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/market/stream"
	wcfg := stream.Defaults()
	wcfg.DialInterval = stream.Duration(10 * time.Millisecond)
	wcfg.BackoffMin = stream.Duration(10 * time.Millisecond)
	wcfg.BackoffMax = stream.Duration(20 * time.Millisecond)
	a := binance.Adapter{Config: cfg, Manager: &stream.Manager{Config: wcfg, Stats: stats}}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.Run(ctx, h); err != nil {
			t.Error(err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				h.Emit(market.Event{Kind: "heartbeat", Exchange: "system", Market: "gateway", Symbol: "ALL"})
			}
		}
	}()
	defer func() {
		cancel()
		wg.Wait()
		if err := <-pubDone; err != nil {
			t.Error(err)
		}
	}()
	pyctx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	code := `
import json, sys, time, zmq
c=zmq.Context(); poller=zmq.Poller(); sockets={}
for kind in ['quote','trade','agg_trade','heartbeat']:
    s=c.socket(zmq.SUB); s.setsockopt(zmq.LINGER,0)
    topic='md.v1.system.gateway.ALL.heartbeat|' if kind=='heartbeat' else 'md.v1.binance.usdm.BTCUSDT.'+kind+'|'
    s.subscribe(topic); s.connect(sys.argv[1]); poller.register(s,zmq.POLLIN); sockets[s]=kind
seen=set(); sessions=set(); last={}; instances=set(); deadline=time.monotonic()+7
while time.monotonic()<deadline:
    for s,_ in poller.poll(500):
        frames=s.recv_multipart(); assert len(frames)==2
        topic,payload=frames; e=json.loads(payload); kind=sockets[s]
        assert e['kind']==kind, (kind,e) # Topic filters must isolate types on the same endpoint.
        assert e['schema']==1 and topic.endswith(('.'+kind+'|').encode())
        assert isinstance(e['seq'],str) and isinstance(e['receive_time_ns'],str)
        seq=int(e['seq'])
        if topic in last: assert seq==last[topic]+1
        last[topic]=seq; seen.add(kind); instances.add(e['instance'])
        if kind=='heartbeat':
            assert e['symbol']=='ALL'
            continue
        assert e['symbol']=='BTCUSDT' and int(e['source_id'])>=9007199254740993
        if kind=='quote':
            assert e['quote']['bid_price']=='60000.12345678'
            sessions.add(e['source_session'])
        else:
            assert e['trade']['side']=='sell'
    if len(seen)==4 and len(sessions)>=2: break
assert len(seen)==4 and len(sessions)>=2, (seen,sessions)
assert len(instances)==1, instances
for s in sockets: s.close()
c.term(); print('PASS: four topic subscriptions on one endpoint, two-frame ZMTP, precision, sequence, shared instance, reconnect')
`
	args := []string{"-c", code, outputs.Endpoint}
	out, err := exec.CommandContext(pyctx, python, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("Python subscriber: %v\n%s", err, out)
	}
	t.Log(string(out))
}
