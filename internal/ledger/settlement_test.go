package ledger_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// sell sells quantity shares at price with a $1 fee, trading on tradeDate.
func (l testLedger) sell(t *testing.T, key, account, quantity, price string) ledger.Entry {
	t.Helper()
	h := l.mustHold(t, "hold-"+key, account, "AAPL", quantity)
	e, err := l.settle(ledger.Fill{
		ID: key, HoldID: h.ID, AccountID: account, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec(quantity), Price: dec(price), Fee: dec("1"),
	})
	noErr(t, err)
	return e
}

func TestSaleProceedsSettle(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.buy(t, "f1", alice, "10", "50")
	l.sell(t, "f2", alice, "10", "60") // 599 of proceeds

	// The proceeds can be traded with but not withdrawn yet.
	b := l.balance(t, alice, ledger.USD)
	if !b.Balance.Equal(dec("1099")) || !b.Unsettled.Equal(dec("599")) || !b.Withdrawable.Equal(dec("500")) {
		t.Errorf("before settlement: %+v, want balance 1099, unsettled 599, withdrawable 500", b)
	}
	_, err := l.withdraw("w1", alice, "600")
	wantErr(t, err, ledger.ErrUnsettledFunds)
	_, err = l.withdraw("w2", alice, "1100")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	n, err := l.settleDue(tradeDate)
	noErr(t, err)
	if n != 0 {
		t.Errorf("settled %d trades on the trade date, want 0", n)
	}
	n, err = l.settleDue(settleDate)
	noErr(t, err)
	if n != 2 {
		t.Errorf("settled %d trades on the settlement date, want 2", n)
	}
	n, err = l.settleDue(settleDate)
	noErr(t, err)
	if n != 0 {
		t.Errorf("settled %d trades on a second run, want 0", n)
	}

	b = l.balance(t, alice, ledger.USD)
	if !b.Unsettled.IsZero() || !b.Withdrawable.Equal(dec("1099")) {
		t.Errorf("after settlement: %+v, want nothing unsettled and 1099 withdrawable", b)
	}
	_, err = l.withdraw("w1", alice, "600")
	noErr(t, err)

	_, err = inTx(l, func(tx pgx.Tx) (int, error) { return ledger.SettleDue(l.ctx, tx, settleDate, 0) })
	wantErr(t, err, ledger.ErrInvalid)
	l.requireReconciled(t)
}

func TestBuyWithUnsettledProceeds(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "500")
	l.buy(t, "f1", alice, "10", "50")
	l.sell(t, "f2", alice, "10", "60")

	// Everything left is unsettled proceeds, and they can still buy shares.
	l.buy(t, "f3", alice, "5", "100")
	b := l.balance(t, alice, ledger.USD)
	if !b.Balance.Equal(dec("99")) || !b.Unsettled.Equal(dec("599")) || !b.Withdrawable.IsZero() {
		t.Errorf("%+v, want balance 99, unsettled 599, nothing withdrawable", b)
	}
	_, err := l.withdraw("w1", alice, "1")
	wantErr(t, err, ledger.ErrUnsettledFunds)

	_, err = l.settleDue(settleDate)
	noErr(t, err)
	_, err = l.withdraw("w1", alice, "99")
	noErr(t, err)
	l.requireReconciled(t)
}

func TestReversingUnsettledSale(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.buy(t, "f1", alice, "10", "50")
	sale := l.sell(t, "f2", alice, "10", "60")

	_, err := l.reverse(sale.ID, "busted by the exchange")
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "500", "0")
	l.wantBalance(t, alice, "AAPL", "10", "0")
	if b := l.balance(t, alice, ledger.USD); !b.Unsettled.IsZero() {
		t.Errorf("unsettled = %s after the sale was reversed, want 0", b.Unsettled)
	}

	// Only the buy is left to settle.
	n, err := l.settleDue(settleDate)
	noErr(t, err)
	if n != 1 {
		t.Errorf("settled %d trades, want 1", n)
	}
	l.requireReconciled(t)
}

// Four settlement runs race over the same due trades. Each trade settles
// exactly once.
func TestConcurrentSettlementRuns(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	trades := 0
	for i := range 4 {
		account := l.openAccount(t)
		l.mustDeposit(t, fmt.Sprintf("d%d", i), account, "1000")
		l.buy(t, fmt.Sprintf("b%d", i), account, "5", "100")
		trades++
		for j := range 5 {
			l.sell(t, fmt.Sprintf("s%d-%d", i, j), account, "1", "100")
			trades++
		}
	}

	var settled atomic.Int64
	runConcurrently(4, func(i int) {
		for {
			n, err := inTx(l, func(tx pgx.Tx) (int, error) { return ledger.SettleDue(l.ctx, tx, settleDate, 5) })
			if err != nil {
				t.Errorf("run %d: %v", i, err)
				return
			}
			settled.Add(int64(n))
			if n == 0 {
				return
			}
		}
	})

	// A run that loses every race can stop early, so finish whatever is left.
	n, err := l.settleDue(settleDate)
	noErr(t, err)
	settled.Add(int64(n))
	if got := settled.Load(); got != int64(trades) {
		t.Errorf("settled %d trades, want %d", got, trades)
	}
	l.requireReconciled(t)
}
