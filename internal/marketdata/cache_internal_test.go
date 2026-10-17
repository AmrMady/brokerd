package marketdata

import (
	"sync/atomic"
	"testing"
)

// A reader can find a symbol's pointer before its first quote is stored.
func TestQuoteCacheSymbolWithoutQuote(t *testing.T) {
	t.Parallel()
	var c QuoteCache
	c.quotes.Store("AAPL", new(atomic.Pointer[Quote]))
	if q, ok := c.Quote("AAPL"); ok {
		t.Errorf("Quote returned %+v before any quote was stored", q)
	}
}
