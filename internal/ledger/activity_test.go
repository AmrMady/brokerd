package ledger_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestActivities(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice, bob := l.openAccount(t), l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.mustDeposit(t, "d2", bob, "50")
	l.buy(t, "f1", alice, "2", "100")
	_, err := l.withdraw("w1", alice, "300")
	noErr(t, err)

	page1, err := ledger.Activities(l.ctx, l.pool, alice, 0, 2)
	noErr(t, err)
	page2, err := ledger.Activities(l.ctx, l.pool, alice, page1[len(page1)-1].EntryID, 2)
	noErr(t, err)
	var got []string
	for _, a := range append(page1, page2...) {
		changes := ""
		for _, c := range a.Changes {
			changes += fmt.Sprintf(" %s%s", c.Amount, c.Asset)
		}
		got = append(got, string(a.Kind)+changes)
	}
	want := []string{"withdrawal -300USD", "trade_fill 2AAPL -200USD", "deposit 1000USD"}
	if !slices.Equal(got, want) {
		t.Errorf("activities %q, want %q", got, want)
	}

	_, err = ledger.Activities(l.ctx, l.pool, alice, 0, ledger.MaxPageSize+1)
	wantErr(t, err, ledger.ErrInvalid)
}
