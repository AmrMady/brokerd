package marketdata_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

var start = time.Date(2026, 10, 16, 13, 30, 0, 0, time.UTC)

func simulate(t *testing.T, cfg marketdata.SimulatorConfig) *recorder {
	t.Helper()
	sim, err := marketdata.NewSimulator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var r recorder
	if err = sim.Run(t.Context(), &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

func TestSimulatorIsDeterministic(t *testing.T) {
	t.Parallel()
	cfg := marketdata.SimulatorConfig{Symbols: []string{"AAPL", "MSFT"}, Seed: 42, Rate: 1000, Start: start, Limit: 1000}
	a, b := simulate(t, cfg).lines(), simulate(t, cfg).lines()
	if !slices.Equal(a, b) {
		t.Error("two runs with the same config produced different events")
	}
	cfg.Seed = 43
	if c := simulate(t, cfg).lines(); slices.Equal(a, c) {
		t.Error("runs with different seeds produced the same events")
	}
}

func TestSimulatorEvents(t *testing.T) {
	t.Parallel()
	r := simulate(t, marketdata.SimulatorConfig{
		Symbols: []string{"AAPL", "MSFT", "NVDA"}, Seed: 1, Rate: 1000, Start: start, Limit: 10000,
	})
	if len(r.events) != 10000 {
		t.Fatalf("got %d events, want 10000", len(r.events))
	}

	type book struct {
		seq      uint64
		bid, ask decimal.Decimal
	}
	books := map[string]*book{}
	cent, fourCents := decimal.New(1, -2), decimal.New(4, -2)
	trades, quotes := 0, 0
	for i, e := range r.events {
		var (
			symbol string
			seq    uint64
			at     time.Time
		)
		switch e := e.(type) {
		case marketdata.Trade:
			symbol, seq, at = e.Symbol, e.Seq, e.Timestamp
		case marketdata.Quote:
			symbol, seq, at = e.Symbol, e.Seq, e.Timestamp
		}
		b := books[symbol]
		if b == nil {
			b = &book{}
			books[symbol] = b
		}
		if seq != b.seq+1 {
			t.Fatalf("event %d: %s seq %d after %d", i, symbol, seq, b.seq)
		}
		b.seq = seq
		if want := start.Add(time.Duration(i) * time.Millisecond); !at.Equal(want) {
			t.Fatalf("event %d at %s, want %s", i, at, want)
		}

		switch e := e.(type) {
		case marketdata.Quote:
			spread := e.Ask.Sub(e.Bid)
			if !e.Bid.IsPositive() || spread.LessThan(cent) || spread.GreaterThan(fourCents) {
				t.Fatalf("event %d: quote %+v, want a positive bid and a spread of 1 to 4 cents", i, e)
			}
			b.bid, b.ask = e.Bid, e.Ask
			quotes++
		case marketdata.Trade:
			if !e.Size.IsPositive() {
				t.Fatalf("event %d: trade %+v, want a positive size", i, e)
			}
			if !b.bid.IsZero() && !e.Price.Equal(b.bid) && !e.Price.Equal(b.ask) {
				t.Fatalf("event %d: trade at %s, want the bid %s or the ask %s", i, e.Price, b.bid, b.ask)
			}
			trades++
		}
	}
	if trades == 0 || quotes == 0 {
		t.Errorf("got %d trades and %d quotes, want both", trades, quotes)
	}
}

// Event times don't drift when the rate doesn't divide a second evenly.
func TestSimulatorEventTimes(t *testing.T) {
	t.Parallel()
	r := simulate(t, marketdata.SimulatorConfig{Symbols: []string{"AAPL"}, Rate: 3, Start: start, Limit: 5})
	var got []time.Duration
	for _, e := range r.events {
		switch e := e.(type) {
		case marketdata.Trade:
			got = append(got, e.Timestamp.Sub(start))
		case marketdata.Quote:
			got = append(got, e.Timestamp.Sub(start))
		}
	}
	want := []time.Duration{0, 333333333, 666666666, time.Second, time.Second + 333333333}
	if !slices.Equal(got, want) {
		t.Errorf("event times %v, want %v", got, want)
	}
}

func TestSimulatorPacing(t *testing.T) {
	t.Parallel()
	began := time.Now()
	r := simulate(t, marketdata.SimulatorConfig{Symbols: []string{"AAPL"}, Rate: 1000, Start: start, Paced: true, Limit: 100})
	// The 100th event is due 99ms after the first.
	if elapsed := time.Since(began); elapsed < 99*time.Millisecond {
		t.Errorf("100 events at 1000/s took %s, want at least 99ms", elapsed)
	}
	if len(r.events) != 100 {
		t.Errorf("got %d events, want 100", len(r.events))
	}
}

func TestSimulatorStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	for _, paced := range []bool{false, true} {
		sim, err := marketdata.NewSimulator(marketdata.SimulatorConfig{Symbols: []string{"AAPL"}, Rate: 1000, Paced: paced})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		err = sim.Run(ctx, discard{})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("paced %t: Run returned %v, want %v", paced, err, context.DeadlineExceeded)
		}
	}
}

func TestNewSimulatorRejectsBadConfig(t *testing.T) {
	t.Parallel()
	for _, cfg := range []marketdata.SimulatorConfig{
		{Rate: 10},
		{Symbols: []string{"AAPL", ""}, Rate: 10},
		{Symbols: []string{"AAPL", "AAPL"}, Rate: 10},
		{Symbols: []string{"AAPL"}},
		{Symbols: []string{"AAPL"}, Rate: -1},
		{Symbols: []string{"AAPL"}, Rate: 10, Limit: -1},
	} {
		if _, err := marketdata.NewSimulator(cfg); err == nil {
			t.Errorf("NewSimulator(%+v) succeeded, want an error", cfg)
		}
	}
}

func BenchmarkSimulator(b *testing.B) {
	sim, err := marketdata.NewSimulator(marketdata.SimulatorConfig{
		Symbols: []string{"AAPL", "AMZN", "GOOG", "META", "MSFT", "NVDA", "TSLA", "AMD", "INTC", "NFLX"},
		Seed:    1, Rate: 50000, Start: start, Limit: b.N,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if err = sim.Run(context.Background(), discard{}); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
}
