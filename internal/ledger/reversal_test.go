package ledger_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func (l testLedger) reverse(entryID int64, reason string) (ledger.Entry, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Entry, error) {
		return ledger.ReverseEntry(l.ctx, tx, entryID, reason)
	})
}

func (l testLedger) buy(t *testing.T, key, account string, quantity, price string) ledger.Entry {
	t.Helper()
	cost := dec(quantity).Mul(dec(price))
	h := l.mustHold(t, "hold-"+key, account, ledger.USD, cost.String())
	e, err := l.settle(ledger.Fill{
		ID: key, HoldID: h.ID, AccountID: account, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec(quantity), Price: dec(price), Fee: dec("0"),
	})
	noErr(t, err)
	return e
}

func TestReverseFill(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "505")
	fill, err := l.settle(ledger.Fill{
		ID: "fill-1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("10"), Price: dec("49.50"), Fee: dec("5"),
	})
	noErr(t, err)

	rev, err := l.reverse(fill.ID, "busted by the exchange")
	noErr(t, err)
	if rev.Kind != ledger.KindReversal || rev.ReversesEntryID != fill.ID || rev.Metadata["reason"] != "busted by the exchange" {
		t.Errorf("reversal = %+v, want a reversal of entry %d with its reason", rev, fill.ID)
	}

	// Everything the fill moved is back. The hold is untouched: what's left
	// of it is still reserved for the order.
	l.wantBalance(t, alice, ledger.USD, "1000", "5")
	l.wantBalance(t, alice, "AAPL", "0", "0")
	l.wantBalance(t, ledger.Clearing, ledger.USD, "0", "0")
	l.wantBalance(t, ledger.Clearing, "AAPL", "0", "0")
	l.wantBalance(t, ledger.Fees, ledger.USD, "0", "0")

	again, err := l.reverse(fill.ID, "busted by the exchange")
	noErr(t, err)
	if again.ID != rev.ID {
		t.Errorf("second reversal returned entry %d, want %d", again.ID, rev.ID)
	}
	l.wantBalance(t, alice, ledger.USD, "1000", "5")
	l.requireReconciled(t)
}

func TestReverseRules(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	bought := l.buy(t, "f1", alice, "1", "100")

	_, err := l.reverse(bought.ID, "")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.reverse(123456, "typo")
	wantErr(t, err, ledger.ErrEntryNotFound)

	var deposit int64
	noErr(t, l.pool.QueryRow(l.ctx, `SELECT id FROM journal_entries WHERE kind = 'deposit'`).Scan(&deposit))
	_, err = l.reverse(deposit, "typo")
	wantErr(t, err, ledger.ErrInvalid)

	// The shares bought are sold again, so the buy can't be undone.
	h := l.mustHold(t, "h2", alice, "AAPL", "1")
	_, err = l.settle(ledger.Fill{
		ID: "f2", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("1"), Price: dec("100"), Fee: dec("0"),
	})
	noErr(t, err)
	_, err = l.reverse(bought.ID, "busted")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	other := l.buy(t, "f3", alice, "2", "50")
	rev, err := l.reverse(other.ID, "busted")
	noErr(t, err)
	_, err = l.reverse(rev.ID, "undo the bust")
	wantErr(t, err, ledger.ErrInvalid)
	l.requireReconciled(t)
}

func TestConcurrentReversals(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	fill := l.buy(t, "f1", alice, "3", "100")

	ids := make([]int64, 20)
	runConcurrently(len(ids), func(i int) {
		e, err := l.reverse(fill.ID, "busted")
		if err != nil {
			t.Errorf("reverse %d: %v", i, err)
			return
		}
		ids[i] = e.ID
	})

	requireSameIDs(t, ids)
	l.wantBalance(t, alice, ledger.USD, "1000", "0")
	l.wantBalance(t, alice, "AAPL", "0", "0")
	l.requireReconciled(t)
}

// Postgres rejects a reversal that isn't the exact mirror of its entry, even
// one that balances.
func TestDatabaseRejectsBadReversal(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "100")

	var deposit int64
	noErr(t, l.pool.QueryRow(l.ctx, `SELECT id FROM journal_entries WHERE kind = 'deposit'`).Scan(&deposit))
	err := l.exec(t, `
		WITH e AS (
			INSERT INTO journal_entries (kind, reverses_entry_id) VALUES ('reversal', $1) RETURNING id
		)
		INSERT INTO journal_lines (entry_id, ledger_account_id, asset, amount, account_version)
		SELECT e.id, l.account, 'USD', l.amount, 1000
		FROM e, (VALUES ($2::bigint, -50::numeric), ($3::bigint, 50::numeric)) AS l(account, amount)`,
		deposit, l.accountID(t, alice, ledger.USD), l.accountID(t, ledger.ExternalBank, ledger.USD))

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Errorf("got %v, want a check violation", err)
	}
	l.requireReconciled(t)
}
