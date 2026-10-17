package ledger_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/migrations"
)

// trade books an AAPL fill on day, with a hold for exactly what it needs.
func (l testLedger) trade(t *testing.T, key, account string, side ledger.Side, quantity, price, fee string, day time.Time) ledger.Entry {
	t.Helper()
	f := ledger.Fill{
		ID: key, AccountID: account, Symbol: "AAPL", Side: side,
		Quantity: dec(quantity), Price: dec(price), Fee: dec(fee),
		TradeDate: day, SettleDate: day.AddDate(0, 0, 1),
	}
	asset, amount := ledger.Asset("AAPL"), f.Quantity
	if side == ledger.Buy {
		asset, amount = ledger.USD, f.Notional().Add(f.Fee)
	}
	f.HoldID = l.mustHold(t, "hold-"+key, account, asset, amount.String()).ID
	e, err := l.settle(f)
	noErr(t, err)
	return e
}

func (l testLedger) openLots(t *testing.T, account string) []string {
	t.Helper()
	lots, err := ledger.OpenLots(l.ctx, l.pool, account, "AAPL")
	noErr(t, err)
	rows := make([]string, len(lots))
	for i, lot := range lots {
		rows[i] = fmt.Sprintf("%s: %s of %s shares, cost %s of %s", lot.AcquiredOn.Format(time.DateOnly),
			lot.Remaining, lot.Quantity, money(lot.RemainingCost), money(lot.Cost))
	}
	return rows
}

func (l testLedger) realizedGains(t *testing.T, account string) []string {
	t.Helper()
	gains, err := ledger.RealizedGains(l.ctx, l.pool, account)
	noErr(t, err)
	rows := make([]string, len(gains))
	for i, g := range gains {
		term := "short"
		if g.LongTerm {
			term = "long"
		}
		rows[i] = fmt.Sprintf("%s: %s shares, proceeds %s, cost %s, gain %s, %s-term", g.AcquiredOn.Format(time.DateOnly),
			g.Quantity, g.Proceeds.StringFixed(2), money(g.Cost), money(g.Gain), term)
	}
	return rows
}

func money(d decimal.NullDecimal) string {
	if !d.Valid {
		return "unknown"
	}
	return d.Decimal.StringFixed(2)
}

func wantRows(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

func TestSellsUseOldestLotsFirst(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.trade(t, "b1", alice, ledger.Buy, "10", "10", "1", time.Date(2025, 10, 15, 0, 0, 0, 0, time.UTC))
	l.trade(t, "b2", alice, ledger.Buy, "10", "20", "1", time.Date(2025, 10, 16, 0, 0, 0, 0, time.UTC))
	wantRows(t, "open lots", l.openLots(t, alice), []string{
		"2025-10-15: 10 of 10 shares, cost 101.00 of 101.00",
		"2025-10-16: 10 of 10 shares, cost 201.00 of 201.00",
	})

	// 447 after the fee, split 10:5 between the lots. The second lot was
	// bought exactly a year before the first sale, which is still short-term,
	// and a day more than a year before the second.
	l.trade(t, "s1", alice, ledger.Sell, "15", "30", "3", tradeDate)
	wantRows(t, "open lots", l.openLots(t, alice), []string{
		"2025-10-16: 5 of 10 shares, cost 100.50 of 201.00",
	})
	l.trade(t, "s2", alice, ledger.Sell, "5", "25", "0", tradeDate.AddDate(0, 0, 1))
	wantRows(t, "open lots", l.openLots(t, alice), []string{})
	wantRows(t, "realized gains", l.realizedGains(t, alice), []string{
		"2025-10-15: 10 shares, proceeds 298.00, cost 101.00, gain 197.00, long-term",
		"2025-10-16: 5 shares, proceeds 149.00, cost 100.50, gain 48.50, short-term",
		"2025-10-16: 5 shares, proceeds 125.00, cost 100.50, gain 24.50, long-term",
	})
	l.wantBalance(t, alice, ledger.USD, "1270", "0")
	l.requireReconciled(t)
}

// Cost and proceeds are split to the cent, and the last part takes what's
// left, so no cent is lost.
func TestLotRounding(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "100")
	l.trade(t, "b1", alice, ledger.Buy, "3", "10", "0.01", tradeDate)
	for i := range 3 {
		l.trade(t, fmt.Sprintf("s%d", i), alice, ledger.Sell, "1", "10", "0", tradeDate)
	}
	wantRows(t, "realized gains", l.realizedGains(t, alice), []string{
		"2026-10-16: 1 shares, proceeds 10.00, cost 10.00, gain 0.00, short-term",
		"2026-10-16: 1 shares, proceeds 10.00, cost 10.00, gain 0.00, short-term",
		"2026-10-16: 1 shares, proceeds 10.00, cost 10.01, gain -0.01, short-term",
	})

	bob := l.openAccount(t)
	l.mustDeposit(t, "d2", bob, "100")
	for i := range 3 {
		l.trade(t, fmt.Sprintf("b%d", i), bob, ledger.Buy, "1", "5", "0", tradeDate)
	}
	l.trade(t, "s", bob, ledger.Sell, "3", "3.333333333", "0", tradeDate) // 10.00
	wantRows(t, "realized gains", l.realizedGains(t, bob), []string{
		"2026-10-16: 1 shares, proceeds 3.33, cost 5.00, gain -1.67, short-term",
		"2026-10-16: 1 shares, proceeds 3.33, cost 5.00, gain -1.67, short-term",
		"2026-10-16: 1 shares, proceeds 3.34, cost 5.00, gain -1.66, short-term",
	})
	l.requireReconciled(t)
}

func TestReversingFillsRestoresLots(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.trade(t, "b1", alice, ledger.Buy, "10", "10", "0", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	second := l.trade(t, "b2", alice, ledger.Buy, "10", "20", "0", time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC))
	sale := l.trade(t, "s1", alice, ledger.Sell, "15", "30", "0", tradeDate)

	_, err := l.reverse(second.ID, "busted")
	wantErr(t, err, ledger.ErrInvalid)

	_, err = l.reverse(sale.ID, "busted")
	noErr(t, err)
	wantRows(t, "open lots", l.openLots(t, alice), []string{
		"2026-10-01: 10 of 10 shares, cost 100.00 of 100.00",
		"2026-10-02: 10 of 10 shares, cost 200.00 of 200.00",
	})
	wantRows(t, "realized gains", l.realizedGains(t, alice), []string{})

	_, err = l.reverse(second.ID, "busted")
	noErr(t, err)
	wantRows(t, "open lots", l.openLots(t, alice), []string{
		"2026-10-01: 10 of 10 shares, cost 100.00 of 100.00",
	})
	l.trade(t, "s2", alice, ledger.Sell, "4", "12", "0", tradeDate)
	wantRows(t, "realized gains", l.realizedGains(t, alice), []string{
		"2026-10-01: 4 shares, proceeds 48.00, cost 40.00, gain 8.00, short-term",
	})
	l.requireReconciled(t)
}

// Sells that run at the same time still take shares oldest lot first, and
// never take more from a lot than it has.
func TestConcurrentSellsShareLots(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	prices := []string{"10", "11", "12", "13"}
	for i, price := range prices {
		l.trade(t, fmt.Sprintf("b%d", i), alice, ledger.Buy, "5", price, "0", time.Date(2026, 10, 1+i, 0, 0, 0, 0, time.UTC))
	}

	holds := make([]int64, 20)
	for i := range holds {
		holds[i] = l.mustHold(t, fmt.Sprintf("h%d", i), alice, "AAPL", "1").ID
	}
	runConcurrently(len(holds), func(i int) {
		_, err := l.settle(ledger.Fill{
			ID: fmt.Sprintf("s%d", i), HoldID: holds[i], AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
			Quantity: dec("1"), Price: dec("20"), Fee: dec("0"),
		})
		if err != nil {
			t.Errorf("sell %d: %v", i, err)
		}
	})

	var want []string
	for i, price := range prices {
		row := fmt.Sprintf("2026-10-%02d: 1 shares, proceeds 20.00, cost %s.00, gain %s.00, short-term",
			1+i, price, dec("20").Sub(dec(price)))
		for range 5 {
			want = append(want, row)
		}
	}
	wantRows(t, "realized gains", l.realizedGains(t, alice), want)
	wantRows(t, "open lots", l.openLots(t, alice), []string{})
	l.requireReconciled(t)
}

// Positions from before lots were tracked get a lot of unknown cost when the
// migration runs. Selling from it realizes an unknown gain.
func TestMigrationAddsLotsForExistingPositions(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.trade(t, "b1", alice, ledger.Buy, "10", "10", "0", tradeDate)

	db := stdlib.OpenDBFromPool(l.pool)
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	noErr(t, err)
	_, err = provider.ApplyVersion(l.ctx, 8, false)
	noErr(t, err)
	_, err = provider.ApplyVersion(l.ctx, 8, true)
	noErr(t, err)

	lots, err := ledger.OpenLots(l.ctx, l.pool, alice, "AAPL")
	noErr(t, err)
	if len(lots) != 1 || !lots[0].Remaining.Equal(dec("10")) || lots[0].Cost.Valid || lots[0].RemainingCost.Valid {
		t.Fatalf("lots after the migration: %+v, want one lot of 10 shares with an unknown cost", lots)
	}

	now := time.Now()
	h := l.mustHold(t, "h1", alice, "AAPL", "4")
	_, err = l.settle(ledger.Fill{
		ID: "s1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("4"), Price: dec("15"), Fee: dec("0"), TradeDate: now, SettleDate: now,
	})
	noErr(t, err)
	gains, err := ledger.RealizedGains(l.ctx, l.pool, alice)
	noErr(t, err)
	if len(gains) != 1 || !gains[0].Proceeds.Equal(dec("60")) || gains[0].Cost.Valid || gains[0].Gain.Valid {
		t.Errorf("gains: %+v, want proceeds 60 with an unknown cost and gain", gains)
	}
	l.requireReconciled(t)
}

func TestReconcileReportsLotMismatch(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.trade(t, "b1", alice, ledger.Buy, "10", "10", "0", tradeDate)
	noErr(t, l.exec(t, `UPDATE tax_lots SET remaining = 9`))

	report, err := ledger.Reconcile(l.ctx, l.pool)
	noErr(t, err)
	want := []ledger.Mismatch{{Check: ledger.CheckLots, AccountID: alice, Asset: "AAPL", Want: dec("9"), Got: dec("10")}}
	if !sameMismatches(report.Mismatches, want) {
		t.Errorf("mismatches = %+v, want %+v", report.Mismatches, want)
	}
}
