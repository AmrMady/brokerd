package ledger_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestDepositAndWithdraw(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	e, err := l.deposit("d1", alice, "100.00")
	noErr(t, err)
	if e.Kind != ledger.KindDeposit || len(e.Lines) != 2 {
		t.Errorf("deposit entry = %+v, want a deposit with 2 lines", e)
	}
	_, err = l.withdraw("w1", alice, "30")
	noErr(t, err)
	_, err = l.withdraw("w2", alice, "70.01")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	l.wantBalance(t, alice, ledger.USD, "70", "0")
	l.wantBalance(t, ledger.ExternalBank, ledger.USD, "-70", "0")
	if v := l.balance(t, alice, ledger.USD).Version; v != 2 {
		t.Errorf("alice/USD version = %d, want 2", v)
	}
	l.requireReconciled(t)
}

func TestRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "0")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d2", alice, "1.001")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d3", ledger.Clearing, "100")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d4", "alice", "100")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d5", "00000000-0000-4000-8000-000000000000", "100")
	wantErr(t, err, ledger.ErrAccountNotFound)
	_, err = l.deposit("", alice, "100")
	wantErr(t, err, ledger.ErrInvalid)
}

func (l testLedger) exec(t *testing.T, sql string, args ...any) error {
	t.Helper()
	return pgx.BeginFunc(l.ctx, l.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(l.ctx, sql, args...)
		return err
	})
}

func (l testLedger) accountID(t *testing.T, owner string, asset ledger.Asset) int64 {
	t.Helper()
	var id int64
	err := l.pool.QueryRow(l.ctx, `SELECT id FROM ledger_accounts WHERE account_id = $1 AND asset = $2`, owner, asset).Scan(&id)
	noErr(t, err)
	return id
}

// forgeLine inserts a one-line entry adding $1 to account $2.
const forgeLine = `
	WITH e AS (
		INSERT INTO journal_entries (kind) VALUES ('deposit') RETURNING id
	)
	INSERT INTO journal_lines (entry_id, ledger_account_id, asset, amount, account_version)
	SELECT id, $2, 'USD', $1, 1000 FROM e`

func TestReconcileReportsTampering(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		tamper func(t *testing.T, l testLedger, cash int64)
		want   func(owner string) []ledger.Mismatch
	}{
		{
			name: "balance edited",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `UPDATE ledger_accounts SET balance = balance + 5 WHERE id = $1`, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckBalance, AccountID: owner, Asset: ledger.USD, Want: dec("100"), Got: dec("105")},
				}
			},
		},
		{
			name: "one-sided line",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `ALTER TABLE journal_lines DISABLE TRIGGER journal_lines_balanced`))
				noErr(t, l.exec(t, forgeLine, 1, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckAssetTotal, Asset: ledger.USD, Want: dec("0"), Got: dec("1")},
					{Check: ledger.CheckBalance, AccountID: owner, Asset: ledger.USD, Want: dec("101"), Got: dec("100")},
				}
			},
		},
		{
			name: "negative available",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `ALTER TABLE ledger_accounts DROP CONSTRAINT ledger_accounts_available_non_negative`))
				noErr(t, l.exec(t, `UPDATE ledger_accounts SET held = 150 WHERE id = $1`, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckAvailable, AccountID: owner, Asset: ledger.USD, Want: dec("0"), Got: dec("-50")},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := newTestLedger(t)
			owner := l.openAccount(t)
			l.mustDeposit(t, "d1", owner, "100")
			l.requireReconciled(t)

			tt.tamper(t, l, l.accountID(t, owner, ledger.USD))

			report, err := ledger.Reconcile(l.ctx, l.pool)
			noErr(t, err)
			if want := tt.want(owner); !sameMismatches(report.Mismatches, want) {
				t.Errorf("mismatches = %+v\nwant %+v", report.Mismatches, want)
			}
		})
	}
}

func sameMismatches(got, want []ledger.Mismatch) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.Check != w.Check || g.AccountID != w.AccountID || g.Asset != w.Asset || !g.Want.Equal(w.Want) || !g.Got.Equal(w.Got) {
			return false
		}
	}
	return true
}

func TestDatabaseBackstops(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	owner := l.openAccount(t)
	l.mustDeposit(t, "d1", owner, "100")
	cash := l.accountID(t, owner, ledger.USD)

	wantSQLState := func(t *testing.T, err error, code string) {
		t.Helper()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Errorf("got %v, want SQLSTATE %s", err, code)
		}
	}
	const checkViolation, raiseException = "23514", "P0001"

	t.Run("unbalanced entry fails at commit", func(t *testing.T) {
		wantSQLState(t, l.exec(t, forgeLine, 1, cash), checkViolation)
	})
	t.Run("entry without lines fails at commit", func(t *testing.T) {
		err := l.exec(t, `INSERT INTO journal_entries (kind) VALUES ('deposit')`)
		wantSQLState(t, err, checkViolation)
	})
	t.Run("journal is append-only", func(t *testing.T) {
		for _, sql := range []string{
			`UPDATE journal_lines SET amount = amount * 2`,
			`DELETE FROM journal_lines`,
			`DELETE FROM journal_entries`,
			`TRUNCATE journal_lines`,
		} {
			wantSQLState(t, l.exec(t, sql), raiseException)
		}
	})
	t.Run("customer balance cannot go negative", func(t *testing.T) {
		err := l.exec(t, `UPDATE ledger_accounts SET balance = -1 WHERE id = $1`, cash)
		wantSQLState(t, err, checkViolation)
	})
	l.requireReconciled(t)
}
