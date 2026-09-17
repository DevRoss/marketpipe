// Package market defines the exchange-independent, versioned wire contract.
package market

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type Quote struct {
	BidPrice string `json:"bid_price"`
	BidQty   string `json:"bid_qty"`
	AskPrice string `json:"ask_price"`
	AskQty   string `json:"ask_qty"`
}

type Trade struct {
	Price        string `json:"price"`
	Qty          string `json:"qty"`
	Side         string `json:"side"` // aggressor side: buy or sell
	FirstTradeID string `json:"first_trade_id"`
	LastTradeID  string `json:"last_trade_id"`
}

// Event is one update, NOT a synthetic merge of asynchronous quote/trade streams.
// Nanoseconds, sequence and exchange IDs are strings on the wire for JavaScript.
type Event struct {
	Schema        int          `json:"schema"`
	Instance      string       `json:"instance"`
	Seq           uint64       `json:"seq,string"`
	Exchange      string       `json:"exchange"`
	Market        string       `json:"market"`
	Symbol        string       `json:"symbol"`
	Kind          string       `json:"kind"`
	SourceSession string       `json:"source_session,omitempty"`
	SourceID      string       `json:"source_id,omitempty"`
	EventTimeMS   int64        `json:"event_time_ms"`
	TradeTimeMS   int64        `json:"trade_time_ms,omitempty"`
	ReceiveTimeNS int64        `json:"receive_time_ns,string"`
	Quote         *Quote       `json:"quote,omitempty"`
	Trade         *Trade       `json:"trade,omitempty"`
	Connections   []Connection `json:"connections,omitempty"`
}

type Connection struct {
	Name              string `json:"name"`
	Connected         bool   `json:"connected"`
	Session           string `json:"session"`
	ReceivedInSession bool   `json:"received_in_session"`
	LastEventMS       int64  `json:"last_event_ms"`
	LastError         string `json:"last_error,omitempty"`
}

// Topic has a trailing separator to permit exact ZeroMQ prefix subscriptions.
func (e Event) Topic() string {
	return fmt.Sprintf("md.v1.%s.%s.%s.%s|", e.Exchange, e.Market, e.Symbol, e.Kind)
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Sink must return immediately; false means the bounded ingress queue was full.
// Implementations own any pointers in Event after calling Emit; do not mutate them.
type Sink interface{ Emit(Event) bool }

// Adapter owns its goroutines and must join all of them before Run returns.
// Recoverable connection errors are retried internally; fatal setup errors return.
type Adapter interface {
	Name() string
	Run(context.Context, Sink) error
}
