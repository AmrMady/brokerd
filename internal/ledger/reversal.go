package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// ReverseEntry posts a new entry that undoes entryID line by line, to correct
// a mistake such as a busted trade. Only trade fills can be reversed:
// transfers are undone with ReturnTransfer, which keeps the transfer's status
// in step. Reversing a fill doesn't touch its hold, cancels its settlement if
// it hasn't settled yet, and undoes what it did to tax lots; a buy can't be
// reversed once any of its shares were sold. Reversing an entry twice returns
// the first reversal.
func ReverseEntry(ctx context.Context, tx pgx.Tx, entryID int64, reason string) (Entry, error) {
	if err := validateReason(reason); err != nil {
		return Entry{}, err
	}
	original, err := loadEntry(ctx, tx, entryID)
	if err != nil {
		return Entry{}, err
	}
	if original.Kind != KindTradeFill {
		return Entry{}, fmt.Errorf("%w: %s entries can't be reversed", ErrInvalid, original.Kind)
	}
	for _, account := range customerAccounts(original.Lines) {
		if _, err = lockAccount(ctx, tx, account); err != nil {
			return Entry{}, err
		}
	}

	lines := make([]Line, len(original.Lines))
	for i, l := range original.Lines {
		lines[i] = Line{AccountID: l.AccountID, Asset: l.Asset, Amount: l.Amount.Neg()}
	}
	pending, err := prepareEntry(ctx, tx, posting{
		kind:     KindReversal,
		reverses: entryID,
		metadata: map[string]string{"reason": reason},
		lines:    lines,
	})
	if err != nil {
		return Entry{}, err
	}

	// reverses_entry_id is unique, so this insert is the point where two
	// reversals of the same entry meet: the second waits for the first and
	// then finds it.
	err = tx.QueryRow(ctx, `
		INSERT INTO journal_entries (kind, reverses_entry_id, metadata)
		VALUES ($1, $2, $3)
		ON CONFLICT (reverses_entry_id) DO NOTHING
		RETURNING id, created_at`,
		KindReversal, entryID, pending.metadata,
	).Scan(&pending.id, &pending.createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existing int64
		if err = tx.QueryRow(ctx, `SELECT id FROM journal_entries WHERE reverses_entry_id = $1`, entryID).Scan(&existing); err != nil {
			return Entry{}, fmt.Errorf("load reversal of entry %d: %w", entryID, err)
		}
		return loadEntry(ctx, tx, existing)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("insert reversal of entry %d: %w", entryID, err)
	}

	var (
		cash      int64
		unsettled decimal.Decimal
		extra     map[int64]change
	)
	err = tx.QueryRow(ctx, `
		UPDATE settlements SET status = 'canceled'
		WHERE entry_id = $1 AND status = 'pending'
		RETURNING ledger_account_id, unsettled_cash`, entryID).Scan(&cash, &unsettled)
	switch {
	case err == nil:
		if unsettled.IsPositive() {
			extra = map[int64]change{cash: {unsettled: unsettled.Neg()}}
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return Entry{}, fmt.Errorf("cancel settlement of entry %d: %w", entryID, err)
	}
	if err = reverseLots(ctx, tx, entryID); err != nil {
		return Entry{}, err
	}
	return pending.finish(ctx, tx, extra)
}

// customerAccounts returns the customer accounts in lines, sorted so that
// accounts are always locked in the same order.
func customerAccounts(lines []Line) []string {
	var accounts []string
	for _, l := range lines {
		if !slices.Contains(systemAccounts, l.AccountID) {
			accounts = append(accounts, l.AccountID)
		}
	}
	slices.Sort(accounts)
	return slices.Compact(accounts)
}
