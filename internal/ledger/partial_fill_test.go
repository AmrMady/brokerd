package ledger_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestPartialFills(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	fill := func(id string, holdID int64, side ledger.Side, quantity, price, fee string) (ledger.Entry, error) {
		return l.settle(ledger.Fill{
			ID: id, HoldID: holdID, AccountID: alice, Symbol: "AAPL", Side: side,
			Quantity: dec(quantity), Price: dec(price), Fee: dec(fee),
		})
	}

	// A buy order for 10 shares at up to $50 plus $5 fees, filled in two parts.
	buy := l.mustHold(t, "h1", alice, ledger.USD, "505")
	_, err := fill("f1", buy.ID, ledger.Buy, "4", "49", "2") // 198
	noErr(t, err)
	_, err = fill("f2", buy.ID, ledger.Buy, "6", "50", "3") // 303
	noErr(t, err)
	h, err := ledger.HoldByID(l.ctx, l.pool, buy.ID)
	noErr(t, err)
	if h.Status != ledger.HoldActive || !h.Remaining.Equal(dec("4")) {
		t.Errorf("buy hold = %+v, want active with 4 remaining", h)
	}
	_, err = fill("f3", buy.ID, ledger.Buy, "1", "10", "0")
	wantErr(t, err, ledger.ErrHoldExceeded)
	_, err = l.releaseHold(buy.ID)
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "499", "0")
	l.wantBalance(t, alice, "AAPL", "10", "0")

	// A sell order for all 10 shares, filled in two parts. The second fill
	// uses up the hold, so it settles and there is nothing left to release.
	sell := l.mustHold(t, "h2", alice, "AAPL", "10")
	_, err = fill("f4", sell.ID, ledger.Sell, "3", "60", "1") // +179
	noErr(t, err)
	_, err = fill("f5", sell.ID, ledger.Sell, "7", "61", "1") // +426
	noErr(t, err)
	h, err = ledger.HoldByID(l.ctx, l.pool, sell.ID)
	noErr(t, err)
	if h.Status != ledger.HoldSettled || !h.Remaining.IsZero() {
		t.Errorf("sell hold = %+v, want settled with nothing remaining", h)
	}
	_, err = l.releaseHold(sell.ID)
	wantErr(t, err, ledger.ErrHoldClosed)

	l.wantBalance(t, alice, ledger.USD, "1104", "0")
	l.wantBalance(t, alice, "AAPL", "0", "0")
	l.wantBalance(t, ledger.Fees, ledger.USD, "7", "0")
	l.requireReconciled(t)
}

// 50 one-share fills race for a hold that covers 30 of them. Exactly 30 must
// go through.
func TestConcurrentPartialFills(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "300")

	var filled atomic.Int64
	runConcurrently(50, func(i int) {
		_, err := l.settle(ledger.Fill{
			ID: fmt.Sprintf("f%d", i), HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
			Quantity: dec("1"), Price: dec("10"), Fee: dec("0"),
		})
		switch {
		case err == nil:
			filled.Add(1)
		case !errors.Is(err, ledger.ErrHoldExceeded) && !errors.Is(err, ledger.ErrHoldClosed):
			t.Errorf("fill %d: %v", i, err)
		}
	})

	if n := filled.Load(); n != 30 {
		t.Errorf("%d fills went through, want 30", n)
	}
	got, err := ledger.HoldByID(l.ctx, l.pool, h.ID)
	noErr(t, err)
	if got.Status != ledger.HoldSettled {
		t.Errorf("hold status = %s, want settled", got.Status)
	}
	l.wantBalance(t, alice, ledger.USD, "700", "0")
	l.wantBalance(t, alice, "AAPL", "30", "0")
	l.requireReconciled(t)
}

// Cancels race with fills on one hold. Every cancel succeeds, since
// releasing twice is a no-op, and fills either land before the release or
// find the hold closed.
func TestCancelRacesWithFills(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "500")

	var filled atomic.Int64
	runConcurrently(50, func(i int) {
		if i%5 == 0 {
			if _, err := l.releaseHold(h.ID); err != nil {
				t.Errorf("release %d: %v", i, err)
			}
			return
		}
		_, err := l.settle(ledger.Fill{
			ID: fmt.Sprintf("f%d", i), HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
			Quantity: dec("1"), Price: dec("10"), Fee: dec("0"),
		})
		switch {
		case err == nil:
			filled.Add(1)
		case !errors.Is(err, ledger.ErrHoldClosed):
			t.Errorf("fill %d: %v", i, err)
		}
	})

	n := filled.Load()
	got, err := ledger.HoldByID(l.ctx, l.pool, h.ID)
	noErr(t, err)
	if got.Status != ledger.HoldReleased || !got.Remaining.Equal(dec("500").Sub(dec(fmt.Sprint(10*n)))) {
		t.Errorf("hold = %+v after %d fills, want released with the rest remaining", got, n)
	}
	l.wantBalance(t, alice, ledger.USD, fmt.Sprint(1000-10*n), "0")
	l.wantBalance(t, alice, "AAPL", fmt.Sprint(n), "0")
	l.requireReconciled(t)
}
