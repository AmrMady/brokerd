package marketdata

import (
	"sync"
	"sync/atomic"
)

// QuoteCache keeps the latest quote for each symbol. It's read far more
// often than it's written, so reads take no locks. Each symbol has an
// atomic pointer to its latest Quote, which is never changed once stored;
// an update stores a pointer to a new one. The pointers are kept in a
// sync.Map, which doesn't lock on reads either and is only written the
// first time a symbol is seen.
//
// The zero value is ready to use. Reads are safe from any goroutine.
// Concurrent updates are safe too, and the last one stored wins, so a
// feed's quotes should come through a Sequencer.
type QuoteCache struct {
	quotes sync.Map // symbol -> *atomic.Pointer[Quote]
}

// HandleQuote makes q its symbol's latest quote.
func (c *QuoteCache) HandleQuote(q Quote) {
	v, ok := c.quotes.Load(q.Symbol)
	if !ok {
		v, _ = c.quotes.LoadOrStore(q.Symbol, new(atomic.Pointer[Quote]))
	}
	latest(v).Store(&q)
}

// HandleTrade does nothing: trades don't change quotes.
func (*QuoteCache) HandleTrade(Trade) {}

// Quote returns symbol's latest quote, or false if it has none.
func (c *QuoteCache) Quote(symbol string) (Quote, bool) {
	v, ok := c.quotes.Load(symbol)
	if !ok {
		return Quote{}, false
	}
	// A symbol's pointer is in the map just before its first quote is.
	q := latest(v).Load()
	if q == nil {
		return Quote{}, false
	}
	return *q, true
}

func latest(v any) *atomic.Pointer[Quote] {
	return v.(*atomic.Pointer[Quote]) //nolint:errcheck // the map holds nothing else
}
