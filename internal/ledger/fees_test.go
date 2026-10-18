package ledger_test

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestRegulatoryFeeAmounts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                     string
		quantity, price          string
		secRate, tafRate, tafMax string
		wantSEC, wantTAF         string
	}{
		{"rounded up to the cent", "10", "150.37", "0.0000278", "0.000166", "8.30", "0.05", "0.01"}, // 0.0418, 0.00166
		{"whole cents", "1000", "100", "0.0000278", "0.000166", "8.30", "2.78", "0.17"},
		{"TAF capped per trade", "60000", "1", "0.0000278", "0.000166", "8.30", "1.67", "8.30"}, // TAF 9.96
		{"fractional shares", "0.5", "100", "0.0000278", "0.000166", "8.30", "0.01", "0.01"},
		{"zero rates", "10", "10", "0", "0", "8.30", "0", "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := newTestLedger(t)
			noErr(t, l.addFeeRate(ledger.SECFee, "2026-01-01", tt.secRate, ""))
			noErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", tt.tafRate, tt.tafMax))
			alice := l.openAccount(t)
			l.mustDeposit(t, "d1", alice, "100000")
			l.trade(t, "b1", alice, ledger.Buy, tt.quantity, tt.price, "0", tradeDate)
			l.trade(t, "s1", alice, ledger.Sell, tt.quantity, tt.price, "0", tradeDate)

			l.wantBalance(t, ledger.SECFeePayable, ledger.USD, tt.wantSEC, "0")
			l.wantBalance(t, ledger.TAFPayable, ledger.USD, tt.wantTAF, "0")
			cash := dec("100000").Sub(dec(tt.wantSEC)).Sub(dec(tt.wantTAF))
			l.wantBalance(t, alice, ledger.USD, cash.String(), "0")
			l.requireReconciled(t)
		})
	}
}

func TestSellPaysRegulatoryFees(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-01-01", "0.0000278", ""))
	noErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.000166", "8.30"))
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "2000")
	l.trade(t, "b1", alice, ledger.Buy, "10", "100", "0", tradeDate)
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "0", "0")
	l.wantBalance(t, ledger.TAFPayable, ledger.USD, "0", "0")

	// 1503.70 less the $1 fee, 0.05 of SEC fee and 0.01 of TAF.
	sale := l.trade(t, "s1", alice, ledger.Sell, "10", "150.37", "1", tradeDate)
	if sale.Metadata["sec_fee_rate"] != "0.0000278" || sale.Metadata["taf_rate"] != "0.000166" {
		t.Errorf("metadata = %v, want the rates charged", sale.Metadata)
	}
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "0.05", "0")
	l.wantBalance(t, ledger.TAFPayable, ledger.USD, "0.01", "0")
	if b := l.balance(t, alice, ledger.USD); !b.Balance.Equal(dec("2502.64")) || !b.Unsettled.Equal(dec("1502.64")) {
		t.Errorf("cash = %+v, want balance 2502.64 with 1502.64 unsettled", b)
	}
	wantRows(t, "realized gains", l.realizedGains(t, alice), []string{
		"2026-10-16: 10 shares, proceeds 1502.64, cost 1000.00, gain 502.64, short-term",
	})

	_, err := l.reverse(sale.ID, "busted")
	noErr(t, err)
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "0", "0")
	l.wantBalance(t, ledger.TAFPayable, ledger.USD, "0", "0")
	l.wantBalance(t, alice, ledger.USD, "1000", "0")
	l.requireReconciled(t)
}

func TestFeeRatesFollowTradeDate(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-01-01", "0.0000278", ""))
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-10-16", "0.00001", ""))
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "200000")
	l.trade(t, "b1", alice, ledger.Buy, "2000", "100", "0", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))

	// newTestLedger's rates start in 2000.
	h := l.mustHold(t, "h1", alice, "AAPL", "1")
	day := time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)
	_, err := l.settle(ledger.Fill{
		ID: "s0", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("1"), Price: dec("100"), Fee: dec("0"), TradeDate: day, SettleDate: day,
	})
	wantErr(t, err, ledger.ErrNoFeeRate)
	_, err = l.releaseHold(h.ID)
	noErr(t, err)

	l.trade(t, "s1", alice, ledger.Sell, "1000", "100", "0", time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "2.78", "0")
	l.trade(t, "s2", alice, ledger.Sell, "1000", "100", "0", tradeDate)
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "3.78", "0")
	l.requireReconciled(t)
}

// A retried fill returns what was booked, even if the rate schedule changed
// in between.
func TestFillRetryAfterRateChange(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-01-01", "0.0000278", ""))
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "100000")
	l.trade(t, "b1", alice, ledger.Buy, "1000", "100", "0", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))

	h := l.mustHold(t, "h1", alice, "AAPL", "1000")
	f := ledger.Fill{
		ID: "s1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("1000"), Price: dec("100"), Fee: dec("0"),
	}
	first, err := l.settle(f)
	noErr(t, err)
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-10-10", "0.00002", ""))
	again, err := l.settle(f)
	noErr(t, err)
	if again.ID != first.ID {
		t.Errorf("retry booked entry %d, want the original %d", again.ID, first.ID)
	}
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "2.78", "0")
	l.requireReconciled(t)
}

func TestAddFeeRate(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	capped := decimal.NewNullDecimal(dec("8.30"))
	for _, r := range []ledger.FeeRate{
		{Fee: "finra", EffectiveOn: day, Rate: dec("0.0001"), MaxPerTrade: capped},
		{Fee: ledger.SECFee, Rate: dec("0.0000278")},
		{Fee: ledger.SECFee, EffectiveOn: day, Rate: dec("-0.0000278")},
		{Fee: ledger.SECFee, EffectiveOn: day, Rate: dec("0.0000000001")},
		{Fee: ledger.SECFee, EffectiveOn: day, Rate: dec("0.0000278"), MaxPerTrade: capped},
		{Fee: ledger.TAF, EffectiveOn: day, Rate: dec("0.000166")},
		{Fee: ledger.TAF, EffectiveOn: day, Rate: dec("0.000166"), MaxPerTrade: decimal.NewNullDecimal(dec("-1"))},
		{Fee: ledger.TAF, EffectiveOn: day, Rate: dec("0.000166"), MaxPerTrade: decimal.NewNullDecimal(dec("8.305"))},
	} {
		if err := l.addRate(r); err == nil {
			t.Errorf("AddFeeRate(%+v) succeeded, want an error", r)
		} else {
			wantErr(t, err, ledger.ErrInvalid)
		}
	}

	// Adding the same rate again is fine; a different one for the same date
	// isn't.
	noErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.000166", "8.30"))
	noErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.000166", "8.3"))
	wantErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.000166", "9.00"), ledger.ErrInvalid)
	wantErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.0002", "8.30"), ledger.ErrInvalid)
}

// A trade that happened is always booked. A sale worth less than its fees,
// by an account that can't pay the difference, takes what cash the account
// has; the deficits account covers the rest, and the account is restricted.
func TestSaleWorthLessThanItsFees(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1")
	l.buy(t, "f1", alice, "1", "0.50")
	h := l.mustHold(t, "h1", alice, "AAPL", "1")
	_, err := l.settle(ledger.Fill{
		ID: "f2", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("1"), Price: dec("0.40"), Fee: dec("1"),
	})
	noErr(t, err)

	// The sale costs 0.60 more than it brings in, and the account has 0.50.
	l.wantBalance(t, alice, ledger.USD, "0", "0")
	l.wantBalance(t, alice, "AAPL", "0", "0")
	l.wantBalance(t, ledger.Fees, ledger.USD, "1", "0")
	l.wantBalance(t, ledger.Deficits, ledger.USD, "-0.10", "0")
	l.wantRestricted(t, alice, "fees more than the sale: f2")
	l.requireReconciled(t)
}
