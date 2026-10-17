package ledger

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// MaxPageSize is the largest page History returns.
const MaxPageSize = 500

// HistoryItem is one journal line of an account.
type HistoryItem struct {
	EntryID   int64
	Kind      Kind
	Amount    decimal.Decimal
	Version   int64
	Metadata  map[string]string
	CreatedAt time.Time
}

// HistoryPage is a page of history, newest first. Next is the cursor for the
// following page, or 0 if there are no more.
type HistoryPage struct {
	Items []HistoryItem
	Next  int64
}

// History returns the journal lines of one asset in an account, newest
// first. Pass 0 as before for the first page and the previous page's Next
// after that.
func History(ctx context.Context, q Querier, accountID string, asset Asset, before int64, limit int) (HistoryPage, error) {
	if err := validateAccountID(accountID); err != nil {
		return HistoryPage{}, err
	}
	if limit < 1 || limit > MaxPageSize {
		return HistoryPage{}, fmt.Errorf("%w: page size must be between 1 and %d, got %d", ErrInvalid, MaxPageSize, limit)
	}
	if before < 0 {
		return HistoryPage{}, fmt.Errorf("%w: invalid cursor %d", ErrInvalid, before)
	}
	if before == 0 {
		before = math.MaxInt64
	}

	// Versions are assigned under the row lock, so they follow commit order
	// and paging by version can't skip a line committed in between pages.
	rows, err := q.Query(ctx, `
		SELECT l.entry_id, e.kind, l.amount, l.account_version, e.metadata, e.created_at
		FROM journal_lines l
		JOIN ledger_accounts la ON la.id = l.ledger_account_id
		JOIN journal_entries e ON e.id = l.entry_id
		WHERE la.account_id = $1 AND la.asset = $2 AND l.account_version < $3
		ORDER BY l.account_version DESC
		LIMIT $4`,
		accountID, asset, before, limit+1)
	if err != nil {
		return HistoryPage{}, fmt.Errorf("load history of %s/%s: %w", accountID, asset, err)
	}
	items, err := pgx.CollectRows(rows, scanHistoryItem)
	if err != nil {
		return HistoryPage{}, fmt.Errorf("load history of %s/%s: %w", accountID, asset, err)
	}

	page := HistoryPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.Next = page.Items[limit-1].Version
	}
	return page, nil
}

func scanHistoryItem(row pgx.CollectableRow) (HistoryItem, error) {
	var it HistoryItem
	err := row.Scan(&it.EntryID, &it.Kind, &it.Amount, &it.Version, &it.Metadata, &it.CreatedAt)
	return it, err
}
