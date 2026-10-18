package ledger_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

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
	page2, err := ledger.Activities(l.ctx, l.pool, alice, page1[len(page1)-1].Seq, 2)
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

// An entry that commits after a page of activity was read is newer than
// everything on that page, so a client that pages back from the newest
// activity, and then asks for anything newer, never misses one. Here a fill
// is held up partway, after its entry was created, while a deposit after it
// commits.
func TestActivitiesFollowCommitOrder(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "100")

	blocker, err := l.pool.Begin(l.ctx)
	noErr(t, err)
	defer func() { _ = blocker.Rollback(l.ctx) }() //nolint:errcheck // rolled back below already
	_, err = blocker.Exec(l.ctx, `SELECT id FROM holds WHERE id = $1 FOR UPDATE`, h.ID)
	noErr(t, err)
	filled := make(chan error, 1)
	go func() {
		_, settleErr := l.settle(ledger.Fill{
			ID: "f1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
			Quantity: dec("1"), Price: dec("100"), Fee: dec("0"),
		})
		filled <- settleErr
	}()
	l.waitForLockWaits(t, 1)
	deposited := make(chan error, 1)
	go func() {
		_, depositErr := l.deposit("d2", alice, "5")
		deposited <- depositErr
	}()
	select {
	case err = <-deposited:
		noErr(t, err)
	case <-time.After(200 * time.Millisecond): // it waits for the fill
	}

	seen, err := ledger.Activities(l.ctx, l.pool, alice, 0, 10)
	noErr(t, err)
	noErr(t, blocker.Rollback(l.ctx))
	noErr(t, <-filled)
	if len(deposited) > 0 {
		noErr(t, <-deposited)
	}
	all, err := ledger.Activities(l.ctx, l.pool, alice, 0, 10)
	noErr(t, err)
	newest := cursor(seen[0])
	for _, a := range all {
		if cursor(a) <= newest && !slices.ContainsFunc(seen, func(s ledger.Activity) bool { return s.EntryID == a.EntryID }) {
			t.Errorf("%s entry %d committed after the first read, but is older than what it showed", a.Kind, a.EntryID)
		}
	}
}

func cursor(a ledger.Activity) int64 { return a.Seq }

// waitForLockWaits waits until n of the test database's connections are
// waiting for a lock.
func (l testLedger) waitForLockWaits(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		noErr(t, l.pool.QueryRow(l.ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting))
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections waiting for a lock, want %d", waiting, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
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
