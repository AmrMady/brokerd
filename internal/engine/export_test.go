package engine

import (
	"context"
	"time"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// MatchForTest matches symbol's book against its latest quote once, as a
// worker does when the symbol gets a quote.
func (e *Engine) MatchForTest(ctx context.Context, symbol string) {
	e.match(ctx, ledger.Asset(symbol))
}

// ExpireForTest expires order id as the expiry loop would at now.
func (e *Engine) ExpireForTest(ctx context.Context, id string, now time.Time) error {
	return e.expire(ctx, id, now)
}
