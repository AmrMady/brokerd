package ledger_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"

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

// Every posting writes an entry_posted event in its own transaction, keyed
// by its account, in the order the postings happened.
func TestPostingsWriteEvents(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	l.buy(t, "f1", alice, "2", "100")
	_, err := l.withdraw("w1", alice, "5000") // fails, so writes nothing
	wantErr(t, err, ledger.ErrInsufficientFunds)

	rows, err := l.pool.Query(l.ctx, `SELECT type, key, payload FROM outbox ORDER BY seq`)
	noErr(t, err)
	type event struct {
		Type, Key string
		Payload   struct {
			EntryID int64
			Kind    string
			Lines   []struct {
				AccountID string `json:"account_id"`
				Amount    string
				Version   int64
			}
		}
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[event])
	noErr(t, err)
	var got []string
	for _, e := range events {
		got = append(got, fmt.Sprintf("%s %t %s", e.Type, e.Key == alice, e.Payload.Kind))
		for _, line := range e.Payload.Lines {
			if line.AccountID == alice && line.Version == 0 {
				t.Errorf("%s line has no version", e.Payload.Kind)
			}
		}
	}
	want := []string{"entry_posted true deposit", "entry_posted true trade_fill"}
	if !slices.Equal(got, want) {
		t.Errorf("events %q, want %q", got, want)
	}
}
