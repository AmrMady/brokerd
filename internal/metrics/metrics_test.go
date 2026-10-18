package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/metrics"
	"github.com/AmrMady/brokerd/internal/stream"
)

func scrape(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape: %d", rec.Code)
	}
	return rec.Body.String()
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	hub, err := stream.NewHub(stream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := hub.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	agg, err := marketdata.NewAggregator(time.Second, 0, hub)
	if err != nil {
		t.Fatal(err)
	}
	seq := marketdata.NewSequencer(marketdata.Handlers{agg, hub})

	r := metrics.New()
	r.MarketData(seq, map[string]*marketdata.Aggregator{"1s": agg})
	r.Stream(hub)

	start := time.Date(2026, 10, 16, 13, 30, 0, 0, time.UTC)
	for i, n := range []uint64{1, 2, 5, 5} { // a gap of two, then a duplicate
		seq.HandleTrade(marketdata.Trade{
			Symbol: "AAPL", Price: decimal.NewFromInt(10), Size: decimal.NewFromInt(1),
			Timestamp: start.Add(time.Duration(i) * time.Second), Seq: n,
		})
	}
	r.ObserveFill(20 * time.Millisecond)
	r.ObservePost(2 * time.Millisecond)
	r.ObserveRequest("GET /healthz", 200, time.Millisecond)

	out := scrape(t, r)
	for _, line := range []string{
		"brokerd_marketdata_events_total 3",
		"brokerd_marketdata_sequence_gaps_total 1",
		"brokerd_marketdata_missed_events_total 2",
		"brokerd_marketdata_stale_events_total 1",
		`brokerd_marketdata_bars_total{timeframe="1s"} 2`,
		`brokerd_marketdata_late_trades_total{timeframe="1s"} 0`,
		"brokerd_stream_clients 0",
		"brokerd_stream_dropped_messages_total 0",
		"brokerd_order_fill_seconds_count 1",
		"brokerd_ledger_post_seconds_count 1",
		`brokerd_http_request_seconds_count{code="200",route="GET /healthz"} 1`,
		"go_goroutines ",
	} {
		if !strings.Contains(out, line) {
			t.Errorf("metrics have no %q", line)
		}
	}
}
