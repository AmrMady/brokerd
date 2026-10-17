package ledger_test

import (
	"testing"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func (l testLedger) countEntries(t *testing.T, kind ledger.Kind) int {
	t.Helper()
	var n int
	err := l.pool.QueryRow(l.ctx, `SELECT count(*) FROM journal_entries WHERE kind = $1`, kind).Scan(&n)
	noErr(t, err)
	return n
}

func requireSameIDs(t *testing.T, ids []int64) {
	t.Helper()
	for i, id := range ids {
		if id == 0 || id != ids[0] {
			t.Fatalf("request %d returned id %d, request 0 returned %d", i, id, ids[0])
		}
	}
}

func TestConcurrentDuplicateDeposit(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	ids := make([]int64, 50)
	runConcurrently(len(ids), func(i int) {
		e, err := l.deposit("dep-1", alice, "100")
		if err != nil {
			t.Errorf("deposit %d: %v", i, err)
			return
		}
		ids[i] = e.ID
	})

	requireSameIDs(t, ids)
	if n := l.countEntries(t, ledger.KindDeposit); n != 1 {
		t.Errorf("%d deposit entries, want 1", n)
	}
	l.wantBalance(t, alice, ledger.USD, "100", "0")
	l.requireReconciled(t)
}

func TestConcurrentDuplicateHold(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "100")

	ids := make([]int64, 50)
	runConcurrently(len(ids), func(i int) {
		h, err := l.placeHold("hold-1", alice, ledger.USD, "60")
		if err != nil {
			t.Errorf("hold %d: %v", i, err)
			return
		}
		ids[i] = h.ID
	})

	requireSameIDs(t, ids)
	l.wantBalance(t, alice, ledger.USD, "100", "60")
	l.requireReconciled(t)
}

func TestConcurrentDuplicateFill(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "505")
	fill := ledger.Fill{
		ID: "fill-1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("10"), Price: dec("50"), Fee: dec("5"),
	}

	ids := make([]int64, 50)
	runConcurrently(len(ids), func(i int) {
		e, err := l.settle(fill)
		if err != nil {
			t.Errorf("settle %d: %v", i, err)
			return
		}
		ids[i] = e.ID
	})

	requireSameIDs(t, ids)
	if n := l.countEntries(t, ledger.KindTradeFill); n != 1 {
		t.Errorf("%d fill entries, want 1", n)
	}
	l.wantBalance(t, alice, ledger.USD, "495", "0")
	l.wantBalance(t, alice, "AAPL", "10", "0")
	l.requireReconciled(t)
}

func TestIdempotencyKeyReusedForDifferentRequest(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	bob := l.openAccount(t)

	first, err := l.deposit("k1", alice, "100")
	noErr(t, err)
	again, err := l.deposit("k1", alice, "100.00")
	noErr(t, err)
	if again.ID != first.ID {
		t.Errorf("retry returned entry %d, want %d", again.ID, first.ID)
	}
	_, err = l.deposit("k1", alice, "101")
	wantErr(t, err, ledger.ErrIdempotencyConflict)
	// Keys belong to an account, so another account can use the same one.
	_, err = l.deposit("k1", bob, "100")
	noErr(t, err)
	_, err = l.withdraw("k1", alice, "100")
	wantErr(t, err, ledger.ErrIdempotencyConflict)

	h, err := l.placeHold("k2", alice, ledger.USD, "10")
	noErr(t, err)
	_, err = l.placeHold("k2", alice, ledger.USD, "11")
	wantErr(t, err, ledger.ErrIdempotencyConflict)

	fill := ledger.Fill{
		ID: "k3", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("1"), Price: dec("9"), Fee: dec("1"),
	}
	_, err = l.settle(fill)
	noErr(t, err)
	fill.Price = dec("8")
	_, err = l.settle(fill)
	wantErr(t, err, ledger.ErrIdempotencyConflict)

	l.wantBalance(t, alice, ledger.USD, "90", "0")
	l.requireReconciled(t)
}

func TestFailedRequestCanBeRetried(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.withdraw("w1", alice, "50")
	wantErr(t, err, ledger.ErrInsufficientFunds)
	_, err = l.deposit("d1", alice, "100")
	noErr(t, err)
	_, err = l.withdraw("w1", alice, "50")
	noErr(t, err)

	l.wantBalance(t, alice, ledger.USD, "50", "0")
	l.requireReconciled(t)
}

func TestRetriedHoldIsNotPlacedAgain(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	_, err := l.deposit("d1", alice, "100")
	noErr(t, err)

	h, err := l.placeHold("h1", alice, ledger.USD, "60")
	noErr(t, err)
	_, err = l.releaseHold(h.ID)
	noErr(t, err)

	retried, err := l.placeHold("h1", alice, ledger.USD, "60")
	noErr(t, err)
	if retried.ID != h.ID || retried.Status != ledger.HoldReleased {
		t.Errorf("retried hold = %+v, want hold %d, released", retried, h.ID)
	}
	l.wantBalance(t, alice, ledger.USD, "100", "0")
	l.requireReconciled(t)
}
