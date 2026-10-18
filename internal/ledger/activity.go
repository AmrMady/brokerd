package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// Activity is a journal entry as one account sees it: the entry and its
// lines on that account.
type Activity struct {
	EntryID   int64
	Kind      Kind
	Metadata  map[string]string
	CreatedAt time.Time
	Changes   []Line // AccountID is the account's own
}

// Activities returns the entries that changed an account, newest first.
// before is the EntryID of the last activity of the previous page, or 0 for
// the first page.
func Activities(ctx context.Context, q Querier, accountID string, before int64, limit int) ([]Activity, error) {
	if err := validateAccountID(accountID); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxPageSize {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d, got %d", ErrInvalid, MaxPageSize, limit)
	}
	rows, err := q.Query(ctx, `
		WITH page AS (
			SELECT DISTINCT l.entry_id
			FROM journal_lines l
			JOIN ledger_accounts la ON la.id = l.ledger_account_id
			WHERE la.account_id = $1 AND ($2 = 0 OR l.entry_id < $2)
			ORDER BY l.entry_id DESC
			LIMIT $3
		)
		SELECT e.id, e.kind, e.metadata, e.created_at, la.asset, l.amount
		FROM page
		JOIN journal_entries e ON e.id = page.entry_id
		JOIN journal_lines l ON l.entry_id = e.id
		JOIN ledger_accounts la ON la.id = l.ledger_account_id AND la.account_id = $1
		ORDER BY e.id DESC, la.asset`, accountID, before, limit)
	if err != nil {
		return nil, fmt.Errorf("load activity of %s: %w", accountID, err)
	}
	var (
		out      []Activity
		id       int64
		kind     Kind
		metadata map[string]string
		created  time.Time
		asset    Asset
		amount   decimal.Decimal
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &kind, &metadata, &created, &asset, &amount}, func() error {
		if len(out) == 0 || out[len(out)-1].EntryID != id {
			out = append(out, Activity{EntryID: id, Kind: kind, Metadata: metadata, CreatedAt: created})
		}
		a := &out[len(out)-1]
		a.Changes = append(a.Changes, Line{AccountID: accountID, Asset: asset, Amount: amount})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load activity of %s: %w", accountID, err)
	}
	return out, nil
}
