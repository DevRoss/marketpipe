package binance

import (
	"strings"
	"testing"
)

const quoteFixture = `{"e":"bookTicker","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","u":9007199254740993,"b":"60000.12345678","B":"0.01000000","a":"60000.12345679","A":"2.00000000","st":1}`
const tradeFixture = `{"e":"aggTrade","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","a":9007199254740993,"p":"60000.12345678","q":"0.01000000","f":10,"l":12,"m":true}`

func TestDecode(t *testing.T) {
	for _, wire := range []string{quoteFixture, `{"stream":"btcusdt@bookTicker","data":` + quoteFixture + `}`} {
		e, err := Decode([]byte(wire), 123)
		if err != nil {
			t.Fatal(err)
		}
		if e.Quote.BidPrice != "60000.12345678" || e.SourceID != "9007199254740993" || e.ReceiveTimeNS != 123 || e.Topic() != "md.v1.binance.usdm.BTCUSDT.quote|" {
			t.Fatalf("wrong event: %+v", e)
		}
	}
	e, err := Decode([]byte(tradeFixture), 123)
	if err != nil {
		t.Fatal(err)
	}
	if e.Trade.Side != "sell" || e.Trade.LastTradeID != "12" || e.SourceID != "9007199254740993" {
		t.Fatalf("wrong trade: %+v", e)
	}
	e, err = Decode([]byte(strings.Replace(tradeFixture, `"m":true`, `"m":false`, 1)), 123)
	if err != nil || e.Trade.Side != "buy" {
		t.Fatal("taker buy mapping", err)
	}
}
func TestRejectMalformed(t *testing.T) {
	for _, s := range []string{`{`, `null`, `{"result":null,"id":1}`, strings.Replace(quoteFixture, `"st":1`, `"st":2`, 1), strings.Replace(quoteFixture, `"b":"60000.12345678"`, `"b":"NaN"`, 1), strings.Replace(tradeFixture, `,"m":true`, "", 1)} {
		if _, err := Decode([]byte(s), 1); err == nil {
			t.Errorf("accepted %s", s)
		}
	}
}
func TestRoutesAndShards(t *testing.T) {
	c := Defaults()
	c.Symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	c.StreamsPerConnection = 2
	a := Adapter{Config: c}
	specs, err := a.Specs()
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 4 {
		t.Fatal(len(specs))
	}
	if !strings.Contains(specs[0].URL, "/public/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker") || !strings.Contains(specs[2].URL, "/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade") {
		t.Fatal(specs)
	}
	if _, err := specs[1].Decode([]byte(quoteFixture), 1); err == nil {
		t.Fatal("accepted symbol belonging to a different shard")
	}
	if _, err := specs[0].Decode([]byte(tradeFixture), 1); err == nil {
		t.Fatal("accepted wrong channel")
	}
}
func BenchmarkDecodeQuote(b *testing.B) {
	data := []byte(quoteFixture)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRawTradeRouteAndDecode(t *testing.T) {
	wire := `{"e":"trade","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","t":8089430918,"p":"76248.10","q":"0.001","X":"MARKET","m":false,"st":1}`
	c := Defaults()
	c.Symbols = []string{"BTCUSDT"}
	c.Channels = []string{"trade"}
	a := Adapter{Config: c}
	specs, err := a.Specs()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specs[0].URL, "/public/stream?streams=btcusdt@trade") {
		t.Fatal(specs[0].URL)
	}
	e, err := specs[0].Decode([]byte(wire), 123)
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != "trade" || e.SourceID != "8089430918" || e.Trade.Side != "buy" || e.Trade.FirstTradeID != e.Trade.LastTradeID || e.TradeTimeMS != 1700000000000 {
		t.Fatalf("wrong raw trade %+v", e)
	}
	wire = strings.Replace(wire, `"T":1700000000000,`, "", 1)
	wire = strings.Replace(wire, `"t":8089430918,`, `"t":8089430918,"T":1700000000000,`, 1)
	e, err = specs[0].Decode([]byte(wire), 123)
	if err != nil || e.SourceID != "8089430918" || e.TradeTimeMS != 1700000000000 {
		t.Fatalf("case-sensitive t/T mapping: %+v %v", e, err)
	}
	if _, err := specs[0].Decode([]byte(tradeFixture), 123); err == nil {
		t.Fatal("accepted aggregated trade on raw route")
	}
}
