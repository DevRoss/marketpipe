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
