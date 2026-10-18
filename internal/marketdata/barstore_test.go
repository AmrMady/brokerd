package marketdata_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func barAt(symbol string, interval time.Duration, n int) marketdata.Bar {
	return marketdata.Bar{Symbol: symbol, Interval: interval, Start: start.Add(time.Duration(n) * interval), Trades: n}
}

func TestBarStore(t *testing.T) {
	t.Parallel()
	s := marketdata.NewBarStore(3)
	hs := marketdata.BarHandlers{s}
	for n := range 10 {
		hs.HandleBar(barAt("AAPL", time.Second, n))
	}
	hs.HandleBar(barAt("AAPL", time.Minute, 0))

	trades := func(bars []marketdata.Bar) []int {
		var out []int
		for _, b := range bars {
			out = append(out, b.Trades)
		}
		return out
	}
	for _, tt := range []struct {
		interval time.Duration
		limit    int
		want     []int
	}{
		{time.Second, 2, []int{8, 9}},
		{time.Second, 10, []int{7, 8, 9}}, // only 3 are kept
		{time.Minute, 10, []int{0}},
	} {
		if got := trades(s.Bars("AAPL", tt.interval, tt.limit)); !slices.Equal(got, tt.want) {
			t.Errorf("Bars(%s, %d) = %v, want %v", tt.interval, tt.limit, got, tt.want)
		}
	}
	if got := s.Bars("MSFT", time.Second, 10); len(got) != 0 {
		t.Errorf("bars of a symbol with none: %v", got)
	}
}

// Bars can be read while they're written.
func TestBarStoreConcurrentReads(t *testing.T) {
	t.Parallel()
	s := marketdata.NewBarStore(100)
	var wg sync.WaitGroup
	wg.Go(func() {
		for n := range 10000 {
			s.HandleBar(barAt("AAPL", time.Second, n))
		}
	})
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				bars := s.Bars("AAPL", time.Second, 50)
				for i := 1; i < len(bars); i++ {
					if bars[i].Trades != bars[i-1].Trades+1 {
						t.Errorf("bars out of order: %d then %d", bars[i-1].Trades, bars[i].Trades)
						return
					}
				}
			}
		})
	}
	wg.Wait()
}
