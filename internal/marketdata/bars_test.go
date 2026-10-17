package marketdata_test

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

type barRecorder struct {
	bars []marketdata.Bar
}

func (r *barRecorder) HandleBar(b marketdata.Bar) { r.bars = append(r.bars, b) }

// take returns the bars recorded since the last call, one line each.
func (r *barRecorder) take() []string {
	lines := barLines(r.bars)
	r.bars = nil
	return lines
}

func barLines(bars []marketdata.Bar) []string {
	lines := make([]string, len(bars))
	for i, b := range bars {
		lines[i] = fmt.Sprintf("%s %s/%s O%s H%s L%s C%s V%s n%d", b.Symbol, b.Start.Format("15:04:05"), b.Interval,
			b.Open, b.High, b.Low, b.Close, b.Volume, b.Trades)
	}
	return lines
}

func at(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }

func tradeAt(symbol string, ms int, price, size string) marketdata.Trade {
	return marketdata.Trade{Symbol: symbol, Price: dec(price), Size: dec(size), Timestamp: at(ms)}
}

func quoteAt(symbol string, ms int) marketdata.Quote {
	return marketdata.Quote{Symbol: symbol, Timestamp: at(ms)}
}

func newAggregator(t *testing.T, interval, grace time.Duration) (*marketdata.Aggregator, *barRecorder) {
	t.Helper()
	var r barRecorder
	a, err := marketdata.NewAggregator(interval, grace, &r)
	if err != nil {
		t.Fatal(err)
	}
	return a, &r
}

func TestAggregatorBuildsBars(t *testing.T) {
	t.Parallel()
	a, r := newAggregator(t, time.Second, 0)
	a.HandleTrade(tradeAt("AAPL", 0, "10", "1"))
	a.HandleTrade(tradeAt("MSFT", 100, "20", "5"))
	a.HandleTrade(tradeAt("AAPL", 200, "12", "2"))
	a.HandleTrade(tradeAt("AAPL", 700, "9", "1"))
	a.HandleTrade(tradeAt("AAPL", 999, "11", "3"))
	wantRows(t, "bars before the window closes", r.take(), nil)

	a.HandleTrade(tradeAt("AAPL", 1000, "13", "1"))
	wantRows(t, "bars once the window closes", r.take(), []string{
		"AAPL 13:30:00/1s O10 H12 L9 C11 V7 n4",
		"MSFT 13:30:00/1s O20 H20 L20 C20 V5 n1",
	})

	// A quote moves time on as well. The window with no trades has no bar.
	a.HandleQuote(quoteAt("MSFT", 3500))
	wantRows(t, "bars after a quote", r.take(), []string{
		"AAPL 13:30:01/1s O13 H13 L13 C13 V1 n1",
	})
	if s := a.Stats(); s != (marketdata.AggregatorStats{Bars: 3}) {
		t.Errorf("stats = %+v, want 3 bars", s)
	}
}

func TestAggregatorGrace(t *testing.T) {
	t.Parallel()
	a, r := newAggregator(t, time.Second, 500*time.Millisecond)
	a.HandleTrade(tradeAt("AAPL", 100, "10", "1"))
	a.HandleTrade(tradeAt("AAPL", 900, "11", "1"))
	a.HandleTrade(tradeAt("AAPL", 1200, "12", "1"))
	// Within the grace period, so they still count, by their own times: the
	// first opens the bar and the second closes it.
	a.HandleTrade(tradeAt("AAPL", 50, "9", "1"))
	a.HandleTrade(tradeAt("AAPL", 950, "8", "1"))
	wantRows(t, "bars within the grace period", r.take(), nil)

	a.HandleQuote(quoteAt("AAPL", 1500))
	wantRows(t, "bars after the grace period", r.take(), []string{
		"AAPL 13:30:00/1s O9 H11 L8 C8 V4 n4",
	})

	a.HandleTrade(tradeAt("AAPL", 990, "7", "1")) // too late
	a.HandleTrade(tradeAt("AAPL", 1300, "13", "2"))
	a.Flush()
	wantRows(t, "flushed bars", r.take(), []string{
		"AAPL 13:30:01/1s O12 H13 L12 C13 V3 n2",
	})
	if s := a.Stats(); s != (marketdata.AggregatorStats{Bars: 2, Late: 1}) {
		t.Errorf("stats = %+v, want 2 bars and 1 late trade", s)
	}
}

func TestAggregatorSameTimeTrades(t *testing.T) {
	t.Parallel()
	a, r := newAggregator(t, time.Second, 0)
	a.HandleTrade(tradeAt("AAPL", 500, "12", "1"))
	a.HandleTrade(tradeAt("AAPL", 500, "13", "1"))
	a.HandleTrade(tradeAt("AAPL", 0, "10", "1"))
	a.HandleTrade(tradeAt("AAPL", 0, "11", "1"))
	a.Flush()
	wantRows(t, "bars", r.take(), []string{"AAPL 13:30:00/1s O10 H13 L10 C13 V4 n4"})
}

func TestAggregatorFlush(t *testing.T) {
	t.Parallel()
	a, r := newAggregator(t, time.Second, time.Hour)
	a.HandleTrade(tradeAt("MSFT", 1100, "20", "1"))
	a.HandleTrade(tradeAt("AAPL", 100, "10", "1"))
	a.Flush()
	wantRows(t, "flushed bars", r.take(), []string{
		"AAPL 13:30:00/1s O10 H10 L10 C10 V1 n1",
		"MSFT 13:30:01/1s O20 H20 L20 C20 V1 n1",
	})
	a.HandleTrade(tradeAt("AAPL", 1500, "10", "1"))
	if s := a.Stats(); s.Late != 1 {
		t.Errorf("a trade for a flushed window: stats = %+v, want it late", s)
	}
}

// A trade can be too late for its 1s bar and still be in time for its 1m bar.
func TestAggregatorIntervalsAreSeparate(t *testing.T) {
	t.Parallel()
	seconds, _ := newAggregator(t, time.Second, 0)
	minutes, r := newAggregator(t, time.Minute, 0)
	for _, tr := range []marketdata.Trade{
		tradeAt("AAPL", 0, "10", "1"),
		tradeAt("AAPL", 2000, "11", "1"),
		tradeAt("AAPL", 500, "12", "1"),
	} {
		seconds.HandleTrade(tr)
		minutes.HandleTrade(tr)
	}
	minutes.Flush()
	wantRows(t, "minute bars", r.take(), []string{"AAPL 13:30:00/1m0s O10 H12 L10 C11 V3 n3"})
	if s := seconds.Stats(); s.Late != 1 {
		t.Errorf("second bars: stats = %+v, want 1 late trade", s)
	}
}

// Trades arriving out of order, but within the grace period, give the same
// bars as grouping all the trades by window.
func TestAggregatorMatchesReference(t *testing.T) {
	t.Parallel()
	r := simulate(t, marketdata.SimulatorConfig{
		Symbols: []string{"AAPL", "MSFT", "NVDA"}, Seed: 7, Rate: 1000, Start: start, Limit: 20000,
	})
	// Shuffle each run of 100 events, which span 100ms.
	events := r.events
	rng := rand.New(rand.NewPCG(7, 7))
	for i := 0; i < len(events); i += 100 {
		block := events[i:min(i+100, len(events))]
		rng.Shuffle(len(block), func(a, b int) { block[a], block[b] = block[b], block[a] })
	}

	for _, interval := range []time.Duration{time.Second, time.Minute} {
		a, got := newAggregator(t, interval, time.Second)
		var trades []marketdata.Trade
		for _, e := range events {
			switch e := e.(type) {
			case marketdata.Trade:
				a.HandleTrade(e)
				trades = append(trades, e)
			case marketdata.Quote:
				a.HandleQuote(e)
			}
		}
		a.Flush()
		wantRows(t, interval.String()+" bars", got.take(), referenceBars(trades, interval))
		if s := a.Stats(); s.Late != 0 {
			t.Errorf("%s: %d late trades, want none", interval, s.Late)
		}
	}
}

// referenceBars groups trades by symbol and window and builds each bar from
// its trades sorted by time.
func referenceBars(trades []marketdata.Trade, interval time.Duration) []string {
	type key struct {
		symbol string
		start  time.Time
	}
	groups := map[key][]marketdata.Trade{}
	for _, t := range trades {
		k := key{t.Symbol, t.Timestamp.Truncate(interval)}
		groups[k] = append(groups[k], t)
	}
	var bars []marketdata.Bar
	for k, g := range groups {
		slices.SortStableFunc(g, func(a, b marketdata.Trade) int { return a.Timestamp.Compare(b.Timestamp) })
		b := marketdata.Bar{
			Symbol: k.symbol, Start: k.start, Interval: interval,
			Open: g[0].Price, High: g[0].Price, Low: g[0].Price, Close: g[len(g)-1].Price, Trades: len(g),
		}
		for _, t := range g {
			b.High = decimal.Max(b.High, t.Price)
			b.Low = decimal.Min(b.Low, t.Price)
			b.Volume = b.Volume.Add(t.Size)
		}
		bars = append(bars, b)
	}
	slices.SortFunc(bars, func(a, b marketdata.Bar) int {
		return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Symbol, b.Symbol))
	})
	return barLines(bars)
}

func TestNewAggregatorRejectsBadConfig(t *testing.T) {
	t.Parallel()
	var r barRecorder
	if _, err := marketdata.NewAggregator(0, 0, &r); err == nil {
		t.Error("NewAggregator accepted a zero interval")
	}
	if _, err := marketdata.NewAggregator(time.Second, -1, &r); err == nil {
		t.Error("NewAggregator accepted a negative grace")
	}
}

type discardBars struct{}

func (discardBars) HandleBar(marketdata.Bar) {}

func BenchmarkAggregator(b *testing.B) {
	symbols := []string{"AAPL", "AMZN", "GOOG", "META", "MSFT", "NVDA", "TSLA", "AMD", "INTC", "NFLX"}
	trades := make([]marketdata.Trade, 4096)
	for i := range trades {
		trades[i] = marketdata.Trade{
			Symbol: symbols[i%len(symbols)],
			Price:  decimal.New(int64(10000+i%97), -2),
			Size:   decimal.NewFromInt(int64(1 + i%500)),
		}
	}
	for _, interval := range []time.Duration{time.Second, time.Minute} {
		b.Run(interval.String(), func(b *testing.B) {
			a, err := marketdata.NewAggregator(interval, time.Second, discardBars{})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				t := trades[i%len(trades)]
				t.Timestamp = start.Add(time.Duration(i) * 20 * time.Microsecond) // 50k trades/s
				a.HandleTrade(t)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "trades/s")
		})
	}
}
