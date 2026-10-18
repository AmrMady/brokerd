// Package marketdata reads trades and quotes from a feed, checks each
// symbol's sequence numbers, keeps the latest quote for each symbol and
// builds bars.
package marketdata

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

// Trade is a trade printed for a symbol.
type Trade struct {
	Symbol    string
	Price     decimal.Decimal
	Size      decimal.Decimal
	Timestamp time.Time
	Seq       uint64
}

// Quote is a symbol's best bid and ask.
type Quote struct {
	Symbol    string
	Bid       decimal.Decimal
	BidSize   decimal.Decimal
	Ask       decimal.Decimal
	AskSize   decimal.Decimal
	Timestamp time.Time
	Seq       uint64
}

// Handler receives a feed's events one at a time, in feed order, on the
// feed's goroutine. It must not block.
type Handler interface {
	HandleTrade(Trade)
	HandleQuote(Quote)
}

// Feed is a source of trades and quotes. Each symbol's events, trades and
// quotes together, carry increasing sequence numbers.
type Feed interface {
	// Run sends events to h until the feed ends, and then returns nil, or
	// until ctx is done, and then returns ctx.Err().
	Run(ctx context.Context, h Handler) error
}

// Handlers passes each event to every handler in it, in order.
type Handlers []Handler

// HandleTrade implements Handler.
func (hs Handlers) HandleTrade(t Trade) {
	for _, h := range hs {
		h.HandleTrade(t)
	}
}

// HandleQuote implements Handler.
func (hs Handlers) HandleQuote(q Quote) {
	for _, h := range hs {
		h.HandleQuote(q)
	}
}
