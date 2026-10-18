package marketdata_test

import (
	"slices"
	"testing"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func TestHandlers(t *testing.T) {
	t.Parallel()
	var a, b recorder
	hs := marketdata.Handlers{&a, &b}
	hs.HandleTrade(trade("AAPL", 1))
	hs.HandleQuote(quote("AAPL", 2))
	for _, r := range []*recorder{&a, &b} {
		if got := r.lines(); len(got) != 2 || !slices.Equal(got, a.lines()) {
			t.Errorf("handler got %q, want the trade and the quote", got)
		}
	}
}
