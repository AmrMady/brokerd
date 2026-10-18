package portfolio_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/portfolio"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

var now = time.Date(2026, 10, 16, 14, 0, 0, 0, time.UTC)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

type harness struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	quotes  *marketdata.QuoteCache
	engine  *engine.Engine
	account string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{ctx: t.Context(), pool: pgtest.NewPool(t), quotes: &marketdata.QuoteCache{}}
	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		for _, symbol := range []ledger.Asset{"AAPL", "MSFT"} {
			if err := ledger.AddAsset(h.ctx, tx, symbol, 9); err != nil {
				return err
			}
		}
		for _, r := range []ledger.FeeRate{
			{Fee: ledger.SECFee, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
			{Fee: ledger.TAF, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), MaxPerTrade: decimal.NewNullDecimal(decimal.Zero)},
		} {
			if err := ledger.AddFeeRate(h.ctx, tx, r); err != nil {
				return err
			}
		}
		a, err := ledger.OpenAccount(h.ctx, tx)
		if err != nil {
			return err
		}
		h.account = a.ID
		tr, err := ledger.RequestDeposit(h.ctx, tx, ledger.CashTransfer{IdempotencyKey: "seed", AccountID: a.ID, Amount: dec("10000")})
		if err != nil {
			return err
		}
		_, err = ledger.CompleteTransfer(h.ctx, tx, tr.ID)
		return err
	})
	noErr(t, err)
	h.engine, err = engine.New(h.ctx, engine.Config{
		Pool: h.pool, Quotes: h.quotes, Calendar: calendar.New(), Now: func() time.Time { return now },
	})
	noErr(t, err)
	return h
}

func (h *harness) quote(symbol, bid, ask string) {
	h.quotes.HandleQuote(marketdata.Quote{Symbol: symbol, Bid: dec(bid), Ask: dec(ask), Timestamp: now})
}

func (h *harness) order(t *testing.T, symbol ledger.Asset, side ledger.Side, qty string, limit string) {
	t.Helper()
	r := orders.Request{
		AccountID: h.account, Symbol: symbol, Side: side, Type: orders.Market, TimeInForce: orders.GTC, Quantity: dec(qty),
	}
	if limit != "" {
		r.Type, r.LimitPrice = orders.Limit, decimal.NewNullDecimal(dec(limit))
	}
	o, err := h.engine.Place(h.ctx, r)
	noErr(t, err)
	if o.Status == orders.Rejected {
		t.Fatalf("order rejected: %s", o.RejectReason)
	}
}

func (h *harness) positions(t *testing.T) []portfolio.Position {
	t.Helper()
	positions, err := portfolio.Positions(h.ctx, h.pool, h.quotes, h.account)
	noErr(t, err)
	return positions
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func null(d decimal.NullDecimal) string {
	if !d.Valid {
		return "null"
	}
	return d.Decimal.String()
}

func want(t *testing.T, p portfolio.Position, quantity, available, basis, average, price, value, unrealized string) {
	t.Helper()
	got := []string{p.Quantity.String(), p.Available.String(), null(p.CostBasis), null(p.AverageCost), null(p.Price), null(p.MarketValue), null(p.Unrealized)}
	exp := []string{quantity, available, basis, average, price, value, unrealized}
	for i := range got {
		if !decimalEqual(got[i], exp[i]) {
			t.Errorf("%s position: got %v, want %v", p.Symbol, got, exp)
			return
		}
	}
}

func decimalEqual(a, b string) bool {
	if a == "null" || b == "null" {
		return a == b
	}
	return dec(a).Equal(dec(b))
}

func TestPositions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if p := h.positions(t); len(p) != 0 {
		t.Errorf("cash only: %v, want no positions", p)
	}

	h.quote("AAPL", "100.00", "100.10")
	h.order(t, "AAPL", ledger.Buy, "10", "") // 1001.00
	h.quote("AAPL", "109.90", "110.00")
	h.order(t, "AAPL", ledger.Buy, "10", "") // 1100.00
	h.quote("AAPL", "120", "120.10")
	p := h.positions(t)
	if len(p) != 1 {
		t.Fatalf("positions %v, want AAPL", p)
	}
	want(t, p[0], "20", "20", "2101", "105.05", "120", "2400", "299")

	// Selling takes the older lot first, so half of its 1001 goes.
	h.order(t, "AAPL", ledger.Sell, "5", "")
	h.order(t, "AAPL", ledger.Sell, "5", "200") // rests, holding 5 shares
	want(t, h.positions(t)[0], "15", "10", "1600.5", "106.7", "120", "1800", "199.5")
}

func TestPositionsWithoutAQuote(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.quote("MSFT", "400", "400.10")
	h.order(t, "MSFT", ledger.Buy, "3", "")
	h.quotes.HandleQuote(marketdata.Quote{Symbol: "MSFT", Ask: dec("400.10"), Timestamp: now}) // the bid is gone
	want(t, h.positions(t)[0], "3", "3", "1200.3", "400.1", "null", "null", "null")
}

func TestPositionsOfUnknownCost(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.quote("AAPL", "100.00", "100.10")
	h.order(t, "AAPL", ledger.Buy, "10", "")
	_, err := h.pool.Exec(h.ctx, `UPDATE tax_lots SET cost = NULL, remaining_cost = NULL`)
	noErr(t, err)
	want(t, h.positions(t)[0], "10", "10", "null", "null", "100", "1000", "null")
}

func TestPositionsOfUnknownAccount(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	_, err := portfolio.Positions(h.ctx, h.pool, h.quotes, "00000000-0000-0000-0000-000000000099")
	if !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Errorf("got %v, want %v", err, ledger.ErrAccountNotFound)
	}
}
