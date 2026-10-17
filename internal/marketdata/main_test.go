package marketdata_test

import (
	"fmt"
	"testing"

	"go.uber.org/goleak"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// recorder keeps the events it gets, in order.
type recorder struct {
	events []any
}

func (r *recorder) HandleTrade(t marketdata.Trade) { r.events = append(r.events, t) }
func (r *recorder) HandleQuote(q marketdata.Quote) { r.events = append(r.events, q) }

// lines formats each event on one line.
func (r *recorder) lines() []string {
	lines := make([]string, len(r.events))
	for i, e := range r.events {
		switch e := e.(type) {
		case marketdata.Trade:
			lines[i] = fmt.Sprintf("trade %s #%d %s %s@%s",
				e.Symbol, e.Seq, e.Timestamp.Format("15:04:05.000"), e.Size, e.Price)
		case marketdata.Quote:
			lines[i] = fmt.Sprintf("quote %s #%d %s %sx%s %sx%s",
				e.Symbol, e.Seq, e.Timestamp.Format("15:04:05.000"), e.BidSize, e.Bid, e.AskSize, e.Ask)
		}
	}
	return lines
}

type discard struct{}

func (discard) HandleTrade(marketdata.Trade) {}
func (discard) HandleQuote(marketdata.Quote) {}
