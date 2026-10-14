package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValidation(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{}`, true}, {`{"queue_capacity":0}`, false}, {`{"quue_capacity":10}`, false},
		{`{"websocket":{"max_age":"24h"}}`, false}, {`{"binance":{"symbols":["BTCUSDT","btcusdt"]}}`, false},
		{`{"binance":{"channels":["depth"]}}`, false}, {`{} {}`, false},
	} {
		p := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(p)
		if (err == nil) != tc.valid {
			t.Errorf("%s: %v", tc.body, err)
		}
	}
}

func TestChannelConfig(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"zmq":{"endpoint":"tcp://127.0.0.1:5556"}}`, true},
		{`{"zmq":{"endpoints":{"trade":"tcp://127.0.0.1:5556"}}}`, false},
		{`{"zmq":{"endpoints":{"typo":"tcp://127.0.0.1:5560"}}}`, false},
		{`{"zmq":{"endpoints":null}}`, false},
		{`{"zmq":{"send_hwm":0}}`, false},
		{`{"zmq":{"endpoint":"ipc:///tmp/quotes.sock"}}`, true},
		{`{"binance":{"channels":["bookTicker","trade","aggTrade"]}}`, true},
	} {
		p := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(p)
		if (err == nil) != tc.valid {
			t.Errorf("%s: %v", tc.body, err)
		}
	}
	for _, path := range []string{"../../configs/local.json", "../../configs/docker.json"} {
		c, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		got := c.Kinds()
		if len(got) != 3 || got[0] != "quote" || got[1] != "agg_trade" || got[2] != "heartbeat" {
			t.Fatal(got)
		}
	}
}
