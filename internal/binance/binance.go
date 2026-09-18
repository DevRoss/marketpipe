package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"marketpipe/internal/stream"
	"marketpipe/pkg/market"
)

type Config struct {
	PublicURL            string   `json:"public_url"`
	MarketURL            string   `json:"market_url"`
	Symbols              []string `json:"symbols"`
	Channels             []string `json:"channels"`
	StreamsPerConnection int      `json:"streams_per_connection"`
}

func Defaults() Config {
	return Config{
		PublicURL: "wss://fstream.binance.com/public/stream", MarketURL: "wss://fstream.binance.com/market/stream",
		Symbols: []string{"BTCUSDT", "ETHUSDT"}, Channels: []string{"bookTicker", "aggTrade"}, StreamsPerConnection: 128,
	}
}

var symbolPattern = regexp.MustCompile(`^[A-Z0-9_]{2,40}$`)

func (c *Config) Validate() error {
	if len(c.Symbols) == 0 || len(c.Symbols) > 4096 || len(c.Channels) == 0 || c.StreamsPerConnection < 1 || c.StreamsPerConnection > 128 {
		return errors.New("require 1..4096 symbols, channels, and 1..128 streams_per_connection")
	}
	seen := map[string]bool{}
	for i, s := range c.Symbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		if !symbolPattern.MatchString(s) || seen[s] {
			return fmt.Errorf("invalid or duplicate symbol %q", s)
		}
		seen[s] = true
		c.Symbols[i] = s
	}
	seen = map[string]bool{}
	for _, c := range c.Channels {
		if (c != "bookTicker" && c != "aggTrade" && c != "trade") || seen[c] {
			return fmt.Errorf("invalid or duplicate channel %q", c)
		}
		seen[c] = true
	}
	for _, s := range []string{c.PublicURL, c.MarketURL} {
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "wss" && u.Scheme != "ws") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return fmt.Errorf("invalid websocket base URL %q", s)
		}
	}
	return nil
}

type Adapter struct {
	Config  Config
	Manager *stream.Manager
}

func (*Adapter) Name() string { return "binance_usdm" }
func (a *Adapter) Run(ctx context.Context, sink market.Sink) error {
	specs, err := a.Specs()
	if err != nil {
		return err
	}
	return a.Manager.Run(ctx, specs, sink)
}
func (a *Adapter) Specs() ([]stream.Spec, error) {
	if err := a.Config.Validate(); err != nil {
		return nil, err
	}
	var out []stream.Spec
	for _, channel := range a.Config.Channels {
		base := a.Config.PublicURL
		if channel == "aggTrade" {
			base = a.Config.MarketURL
		}
		for i := 0; i < len(a.Config.Symbols); i += a.Config.StreamsPerConnection {
			symbols := a.Config.Symbols[i:min(i+a.Config.StreamsPerConnection, len(a.Config.Symbols))]
			streams := make([]string, 0, len(symbols))
			allowed := map[string]bool{}
			for _, s := range symbols {
				streams = append(streams, strings.ToLower(s)+"@"+channel)
				allowed[s] = true
			}
			endpoint := base + "?streams=" + strings.Join(streams, "/")
			if len(endpoint) > 8000 {
				return nil, errors.New("combined stream URL too long: reduce streams_per_connection")
			}
			ch := channel
			out = append(out, stream.Spec{Name: fmt.Sprintf("binance_usdm_%s_%d", ch, i/a.Config.StreamsPerConnection), URL: endpoint,
				Decode: func(b []byte, recv int64) (market.Event, error) {
					e, err := Decode(b, recv)
					if err != nil {
						return e, err
					}
					expected := "quote"
					if ch == "aggTrade" {
						expected = "agg_trade"
					} else if ch == "trade" {
						expected = "trade"
					}
					if !allowed[e.Symbol] || e.Kind != expected {
						return e, errors.New("event does not match configured subscription")
					}
					return e, nil
				}})
		}
	}
	return out, nil
}

// Separate wire types are essential: Binance reuses a for ask price and trade ID.
type header struct {
	Event      string `json:"e"`
	Symbol     string `json:"s"`
	EventMS    int64  `json:"E"`
	TradeMS    int64  `json:"T"`
	TradeID    int64  `json:"t"` // absorb exact lowercase key; encoding/json otherwise matches T
	SymbolType int    `json:"st"`
}

func Decode(b []byte, recv int64) (market.Event, error) {
	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return market.Event{}, err
	}
	if len(wrapper.Data) > 0 {
		b = wrapper.Data
	}
	var h header
	if err := json.Unmarshal(b, &h); err != nil {
		return market.Event{}, err
	}
	if !symbolPattern.MatchString(h.Symbol) || h.EventMS <= 0 || h.TradeMS <= 0 || (h.SymbolType != 0 && h.SymbolType != 1) {
		return market.Event{}, errors.New("missing event metadata or non-UM symbol type")
	}
	e := market.Event{Exchange: "binance", Market: "usdm", Symbol: h.Symbol, EventTimeMS: h.EventMS, TradeTimeMS: h.TradeMS, ReceiveTimeNS: recv}
	switch h.Event {
	case "bookTicker":
		var q struct {
			Update *int64 `json:"u"`
			Bid    string `json:"b"`
			BidQty string `json:"B"`
			Ask    string `json:"a"`
			AskQty string `json:"A"`
		}
		if err := json.Unmarshal(b, &q); err != nil {
			return e, err
		}
		if q.Update == nil || *q.Update < 0 || !decimal(q.Bid) || !decimal(q.BidQty) || !decimal(q.Ask) || !decimal(q.AskQty) {
			return e, errors.New("invalid book ticker fields")
		}
		e.Kind = "quote"
		e.SourceID = strconv.FormatInt(*q.Update, 10)
		e.Quote = &market.Quote{BidPrice: q.Bid, BidQty: q.BidQty, AskPrice: q.Ask, AskQty: q.AskQty}
	case "trade":
		var t struct {
			ID            *int64 `json:"t"`
			TradeMS       int64  `json:"T"` // prevent uppercase T from overwriting ID
			Price         string `json:"p"`
			Qty           string `json:"q"`
			Maker         *bool  `json:"m"`
			ExecutionType string `json:"X"`
		}
		if err := json.Unmarshal(b, &t); err != nil {
			return e, err
		}
		if t.ID == nil || *t.ID < 0 || t.Maker == nil || !decimal(t.Price) || !decimal(t.Qty) {
			return e, errors.New("invalid raw trade fields")
		}
		side := "buy"
		if *t.Maker {
			side = "sell"
		}
		e.Kind = "trade"
		e.SourceID = strconv.FormatInt(*t.ID, 10)
		e.Trade = &market.Trade{Price: t.Price, Qty: t.Qty, Side: side, FirstTradeID: e.SourceID, LastTradeID: e.SourceID, ExecutionType: t.ExecutionType}
	case "aggTrade":
		var t struct {
			ID    *int64 `json:"a"`
			Price string `json:"p"`
			Qty   string `json:"q"`
			First *int64 `json:"f"`
			Last  *int64 `json:"l"`
			Maker *bool  `json:"m"`
		}
		if err := json.Unmarshal(b, &t); err != nil {
			return e, err
		}
		if t.ID == nil || t.First == nil || t.Last == nil || t.Maker == nil || *t.ID < 0 || *t.First < 0 || *t.Last < *t.First || !decimal(t.Price) || !decimal(t.Qty) {
			return e, errors.New("invalid aggregate trade fields")
		}
		side := "buy"
		if *t.Maker {
			side = "sell"
		}
		e.Kind = "agg_trade"
		e.SourceID = strconv.FormatInt(*t.ID, 10)
		e.Trade = &market.Trade{Price: t.Price, Qty: t.Qty, Side: side, FirstTradeID: strconv.FormatInt(*t.First, 10), LastTradeID: strconv.FormatInt(*t.Last, 10)}
	default:
		return e, fmt.Errorf("unsupported event %q", h.Event)
	}
	return e, nil
}

// Validate nonnegative fixed-point decimals without float parsing or allocation.
func decimal(s string) bool {
	if len(s) == 0 || len(s) > 80 {
		return false
	}
	dot := false
	for i, c := range s {
		if c == '.' {
			if dot || i == 0 || i == len(s)-1 {
				return false
			}
			dot = true
		} else if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
