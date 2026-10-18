package api

import (
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

type quoteJSON struct {
	Symbol    string          `json:"symbol"`
	Bid       decimal.Decimal `json:"bid"`
	BidSize   decimal.Decimal `json:"bid_size"`
	Ask       decimal.Decimal `json:"ask"`
	AskSize   decimal.Decimal `json:"ask_size"`
	Timestamp time.Time       `json:"timestamp"`
	Seq       uint64          `json:"seq"`
}

func (s *Server) quote(w http.ResponseWriter, r *http.Request) error {
	q, ok := s.cfg.Quotes.Quote(r.PathValue("symbol"))
	if !ok {
		return newError(http.StatusNotFound, "not_found", "no quote for %s", r.PathValue("symbol"))
	}
	writeJSON(w, http.StatusOK, quoteJSON{q.Symbol, q.Bid, q.BidSize, q.Ask, q.AskSize, q.Timestamp, q.Seq})
	return nil
}

type barJSON struct {
	Start  time.Time       `json:"start"`
	Open   decimal.Decimal `json:"open"`
	High   decimal.Decimal `json:"high"`
	Low    decimal.Decimal `json:"low"`
	Close  decimal.Decimal `json:"close"`
	Volume decimal.Decimal `json:"volume"`
	Trades int             `json:"trades"`
}

var timeframes = map[string]time.Duration{"1s": time.Second, "1m": time.Minute}

// bars returns a symbol's most recent bars, oldest first: 1m by default.
func (s *Server) bars(w http.ResponseWriter, r *http.Request) error {
	tf := r.URL.Query().Get("timeframe")
	if tf == "" {
		tf = "1m"
	}
	interval, ok := timeframes[tf]
	if !ok {
		return newError(http.StatusUnprocessableEntity, "invalid_request", "timeframe must be 1s or 1m, got %q", tf)
	}
	limit, err := pageLimit(r)
	if err != nil {
		return err
	}
	bars := s.cfg.Bars.Bars(r.PathValue("symbol"), interval, limit)
	out := make([]barJSON, len(bars))
	for i, b := range bars {
		out[i] = barJSON{b.Start, b.Open, b.High, b.Low, b.Close, b.Volume, b.Trades}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
