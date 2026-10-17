package ledger

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// SettleDue settles up to limit trades whose settlement date is on or before
// asOf and returns how many it settled. Settling makes a sell's proceeds
// withdrawable. It's safe to run again, or several times at once: each
// settlement is a compare-and-set on its status.
func SettleDue(ctx context.Context, tx pgx.Tx, asOf time.Time, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("%w: limit must be positive, got %d", ErrInvalid, limit)
	}
	rows, err := tx.Query(ctx, `
		SELECT s.id, la.account_id::text
		FROM settlements s
		JOIN ledger_accounts la ON la.id = s.ledger_account_id
		WHERE s.status = 'pending' AND s.settle_date <= $1
		ORDER BY s.id
		LIMIT $2`,
		dateOnly(asOf), limit)
	if err != nil {
		return 0, fmt.Errorf("find due settlements: %w", err)
	}
	var (
		ids      []int64
		accounts []string
		id       int64
		account  string
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &account}, func() error {
		ids = append(ids, id)
		accounts = append(accounts, account)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("find due settlements: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	// Same lock order as every other write: accounts, then the settlement
	// rows, then the ledger rows.
	slices.Sort(accounts)
	for _, a := range slices.Compact(accounts) {
		if _, err = lockAccount(ctx, tx, a); err != nil {
			return 0, err
		}
	}
	rows, err = tx.Query(ctx, `
		UPDATE settlements SET status = 'settled', settled_at = now()
		WHERE id = ANY($1) AND status = 'pending'
		RETURNING ledger_account_id, unsettled_cash`, ids)
	if err != nil {
		return 0, fmt.Errorf("settle: %w", err)
	}
	var (
		settled   int
		changes   = map[int64]change{}
		ledgerID  int64
		unsettled decimal.Decimal
	)
	_, err = pgx.ForEachRow(rows, []any{&ledgerID, &unsettled}, func() error {
		settled++
		if unsettled.IsPositive() {
			c := changes[ledgerID]
			c.unsettled = c.unsettled.Sub(unsettled)
			changes[ledgerID] = c
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("settle: %w", err)
	}
	if len(changes) > 0 {
		if _, err = applyChanges(ctx, tx, changes); err != nil {
			return 0, err
		}
	}
	return settled, nil
}
