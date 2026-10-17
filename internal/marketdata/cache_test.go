package marketdata_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func TestQuoteCache(t *testing.T) {
	t.Parallel()
	var c marketdata.QuoteCache
	if q, ok := c.Quote("AAPL"); ok {
		t.Errorf("empty cache returned %+v", q)
	}

	first := marketdata.Quote{Symbol: "AAPL", Bid: dec("227.14"), Ask: dec("227.16"), Seq: 1}
	c.HandleQuote(first)
	c.HandleQuote(marketdata.Quote{Symbol: "MSFT", Bid: dec("415.20"), Ask: dec("415.25"), Seq: 1})
	c.HandleTrade(marketdata.Trade{Symbol: "AAPL", Price: dec("300"), Seq: 2})
	if q, ok := c.Quote("AAPL"); !ok || q.Seq != 1 || !q.Bid.Equal(first.Bid) {
		t.Errorf("Quote(AAPL) = %+v, %t, want %+v", q, ok, first)
	}

	c.HandleQuote(marketdata.Quote{Symbol: "AAPL", Bid: dec("227.15"), Ask: dec("227.17"), Seq: 3})
	if q, _ := c.Quote("AAPL"); q.Seq != 3 || !q.Bid.Equal(dec("227.15")) {
		t.Errorf("Quote(AAPL) after an update = %+v, want seq 3 bid 227.15", q)
	}
	if q, _ := c.Quote("MSFT"); q.Seq != 1 {
		t.Errorf("Quote(MSFT) = %+v, want seq 1", q)
	}
}

// Readers running alongside the writer always see a whole quote, and never
// an older one than they saw before.
func TestQuoteCacheConcurrentReads(t *testing.T) {
	t.Parallel()
	var c marketdata.QuoteCache
	symbols := []string{"AAPL", "MSFT", "NVDA"}
	const updates = 20000

	var (
		wg   sync.WaitGroup
		done atomic.Bool
	)
	wg.Go(func() {
		defer done.Store(true)
		for n := 1; n <= updates; n++ {
			bid := decimal.NewFromInt(int64(n))
			c.HandleQuote(marketdata.Quote{Symbol: symbols[n%len(symbols)], Bid: bid, Ask: bid.Add(decimal.NewFromInt(1)), Seq: uint64(n)})
		}
	})
	for range 4 {
		wg.Go(func() {
			seen := map[string]uint64{}
			for !done.Load() {
				for _, s := range symbols {
					q, ok := c.Quote(s)
					if !ok {
						continue
					}
					if q.Symbol != s || !q.Bid.Equal(decimal.NewFromUint64(q.Seq)) || !q.Ask.Sub(q.Bid).Equal(decimal.NewFromInt(1)) {
						t.Errorf("read a mixed-up quote %+v", q)
						return
					}
					if q.Seq < seen[s] {
						t.Errorf("%s went back from seq %d to %d", s, seen[s], q.Seq)
						return
					}
					seen[s] = q.Seq
				}
			}
		})
	}
	wg.Wait()
	for i, s := range symbols {
		want := uint64(updates - (updates-i)%len(symbols))
		if q, _ := c.Quote(s); q.Seq != want {
			t.Errorf("Quote(%s) ended at seq %d, want %d", s, q.Seq, want)
		}
	}
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func ExampleQuoteCache() {
	var c marketdata.QuoteCache
	c.HandleQuote(marketdata.Quote{Symbol: "AAPL", Bid: dec("227.14"), Ask: dec("227.16")})
	q, ok := c.Quote("AAPL")
	fmt.Println(q.Bid, q.Ask, ok)
	// Output: 227.14 227.16 true
}
