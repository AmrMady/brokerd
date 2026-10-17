package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// Checks run by Reconcile.
const (
	CheckAssetTotal = "asset_total" // lines of each asset sum to zero
	CheckBalance    = "balance"     // balance equals the sum of the account's lines
	CheckAvailable  = "available"   // customer accounts have no negative available balance
)

// Mismatch is a failed check.
type Mismatch struct {
	Check     string
	AccountID string
	Asset     Asset
	Want      decimal.Decimal
	Got       decimal.Decimal
}

// Report is the result of Reconcile.
type Report struct {
	LedgerAccounts int
	Lines          int
	Mismatches     []Mismatch
}

// OK reports whether all checks passed.
func (r Report) OK() bool {
	return len(r.Mismatches) == 0
}

// Each query returns (account, asset, want, got) for the rows that fail the check.
var reconcileQueries = []struct {
	check string
	sql   string
}{
	{CheckAssetTotal, `
		SELECT '', asset, 0::numeric, SUM(amount)
		FROM journal_lines
		GROUP BY asset
		HAVING SUM(amount) <> 0`},
	{CheckBalance, `
		SELECT la.account_id::text, la.asset, COALESCE(l.total, 0), la.balance
		FROM ledger_accounts la
		LEFT JOIN (
			SELECT ledger_account_id, SUM(amount) AS total FROM journal_lines GROUP BY ledger_account_id
		) l ON l.ledger_account_id = la.id
		WHERE la.balance <> COALESCE(l.total, 0)`},
	{CheckAvailable, `
		SELECT account_id::text, asset, 0::numeric, balance - held
		FROM ledger_accounts
		WHERE kind = 'customer' AND balance < held`},
}

// Reconcile recomputes balances from the journal and reports anything that
// doesn't match. All checks read the same
// snapshot, so concurrent writes can't cause false mismatches.
func Reconcile(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	var report Report
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err := pgx.BeginTxFunc(ctx, pool, opts, func(tx pgx.Tx) error {
		return report.check(ctx, tx)
	})
	return report, err
}

func (r *Report) check(ctx context.Context, tx pgx.Tx) error {
	if err := tx.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM ledger_accounts), (SELECT count(*) FROM journal_lines)`,
	).Scan(&r.LedgerAccounts, &r.Lines); err != nil {
		return fmt.Errorf("count rows: %w", err)
	}
	for _, q := range reconcileQueries {
		rows, err := tx.Query(ctx, q.sql)
		if err != nil {
			return fmt.Errorf("check %s: %w", q.check, err)
		}
		found, err := pgx.CollectRows(rows, scanMismatch(q.check))
		if err != nil {
			return fmt.Errorf("check %s: %w", q.check, err)
		}
		r.Mismatches = append(r.Mismatches, found...)
	}
	return nil
}

func scanMismatch(check string) pgx.RowToFunc[Mismatch] {
	return func(row pgx.CollectableRow) (Mismatch, error) {
		m := Mismatch{Check: check}
		err := row.Scan(&m.AccountID, &m.Asset, &m.Want, &m.Got)
		return m, err
	}
}
