package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/api"
	"github.com/AmrMady/brokerd/internal/marketdata"
)

// Without a database, the API only serves market data.
func TestMarketData(t *testing.T) {
	t.Parallel()
	quotes, bars := &marketdata.QuoteCache{}, marketdata.NewBarStore(10)
	s, err := api.New(api.Config{Quotes: quotes, Bars: bars})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	c := &client{t: t, url: srv.URL}

	start := time.Date(2026, 10, 16, 13, 30, 0, 0, time.UTC)
	d := decimal.RequireFromString
	quotes.HandleQuote(marketdata.Quote{Symbol: "AAPL", Bid: d("227.15"), BidSize: d("300"), Ask: d("227.17"), AskSize: d("200"), Timestamp: start, Seq: 7})
	for n := range 3 {
		bars.HandleBar(marketdata.Bar{
			Symbol: "AAPL", Interval: time.Minute, Start: start.Add(time.Duration(n) * time.Minute),
			Open: d("1"), High: d("2"), Low: d("1"), Close: d("2"), Volume: d("10"), Trades: n + 1,
		})
	}

	q := c.do("GET", "/v1/quotes/AAPL", "")
	wantStatus(t, q, http.StatusOK, "")
	if field(q, "bid") != "227.15" || field(q, "ask_size") != "200" || field(q, "seq") != 7.0 {
		t.Errorf("quote %v", q.body)
	}
	wantStatus(t, c.do("GET", "/v1/quotes/MSFT", ""), http.StatusNotFound, "not_found")

	b := c.do("GET", "/v1/bars/AAPL?limit=2", "")
	if len(b.list) != 2 || field(item(b, 0), "trades") != 2.0 || field(item(b, 1), "trades") != 3.0 {
		t.Errorf("bars %v, want the last two, oldest first", b.list)
	}
	if none := c.do("GET", "/v1/bars/AAPL?timeframe=1s", ""); none.status != http.StatusOK || len(none.list) != 0 {
		t.Errorf("1s bars %d %v, want none", none.status, none.list)
	}
	wantStatus(t, c.do("GET", "/v1/bars/AAPL?timeframe=5m", ""), http.StatusUnprocessableEntity, "invalid_request")
	wantStatus(t, c.do("GET", "/readyz", ""), http.StatusOK, "")
	wantStatus(t, c.do("POST", "/v1/accounts", ""), http.StatusNotFound, "not_found")
}
