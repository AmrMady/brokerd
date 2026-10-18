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
	Seq       int64 // the account's number for it, in commit order
	EntryID   int64
	Kind      Kind
	Metadata  map[string]string
	CreatedAt time.Time
	Changes   []Line // AccountID is the account's own
}

// recordActivity gives the entry the next number in the activity of each
// customer it changes. The number comes from the account's row, which the
// caller holds with lockAccount until it commits, so an account's numbers
// follow commit order: an entry that commits after a page was read is
// newer than anything on it.
func recordActivity(ctx context.Context, tx pgx.Tx, entryID int64, lines []Line) error {
	for _, account := range customerAccounts(lines) {
		_, err := tx.Exec(ctx, `
			WITH a AS (
				UPDATE accounts SET last_activity = last_activity + 1 WHERE id = $1 RETURNING last_activity
			)
			INSERT INTO activities (account_id, seq, entry_id) SELECT $1, last_activity, $2 FROM a`,
			account, entryID)
		if err != nil {
			return fmt.Errorf("record activity of %s: %w", account, err)
		}
	}
	return nil
}

// Activities returns the entries that changed an account, newest first.
// before is the Seq of the last activity of the previous page, or 0 for the
// first page.
func Activities(ctx context.Context, q Querier, accountID string, before int64, limit int) ([]Activity, error) {
	if err := validateAccountID(accountID); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxPageSize {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d, got %d", ErrInvalid, MaxPageSize, limit)
	}
	rows, err := q.Query(ctx, `
		WITH page AS (
			SELECT seq, entry_id FROM activities
			WHERE account_id = $1 AND ($2 = 0 OR seq < $2)
			ORDER BY seq DESC
			LIMIT $3
		)
		SELECT page.seq, e.id, e.kind, e.metadata, e.created_at, la.asset, l.amount
		FROM page
		JOIN journal_entries e ON e.id = page.entry_id
		JOIN journal_lines l ON l.entry_id = e.id
		JOIN ledger_accounts la ON la.id = l.ledger_account_id AND la.account_id = $1
		ORDER BY page.seq DESC, la.asset`, accountID, before, limit)
	if err != nil {
		return nil, fmt.Errorf("load activity of %s: %w", accountID, err)
	}
	var (
		out      []Activity
		seq, id  int64
		kind     Kind
		metadata map[string]string
		created  time.Time
		asset    Asset
		amount   decimal.Decimal
	)
	_, err = pgx.ForEachRow(rows, []any{&seq, &id, &kind, &metadata, &created, &asset, &amount}, func() error {
		if len(out) == 0 || out[len(out)-1].Seq != seq {
			out = append(out, Activity{Seq: seq, EntryID: id, Kind: kind, Metadata: metadata, CreatedAt: created})
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
