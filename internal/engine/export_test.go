package engine

import (
	"context"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// MatchForTest matches symbol's book against its latest quote once, as a
// worker does when the symbol gets a quote.
func (e *Engine) MatchForTest(ctx context.Context, symbol string) {
	e.match(ctx, ledger.Asset(symbol))
}
