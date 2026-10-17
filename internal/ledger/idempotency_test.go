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

	l.wantBalance(t, alice, ledger.USD, "100", "0")
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
