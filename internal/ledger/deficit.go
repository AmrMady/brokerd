package ledger

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// coverShortfall is for entries that have to be booked even when the
// customer can't pay, like a trade that has already happened or a deposit
// the bank took back. If lines take more out of a's cash than it has
// available, it posts a deficit entry that moves the difference in from the
// deficits account, and restricts a for reason. Call it after the entry's
// idempotency key or compare-and-set, so a retry can't cover twice, and
// before applying the entry.
//
// The caller must hold a with lockAccountForUpdate, so what's available
// can't change before the entry is applied. The deficit entry locks the
// deficits account before the entry locks any other system account; every
// transaction that covers a shortfall does the same, so they can't
// deadlock.
func coverShortfall(ctx context.Context, tx pgx.Tx, lines []Line, a Account, reason string) error {
	i := slices.IndexFunc(lines, func(l Line) bool { return l.AccountID == a.ID && l.Asset == USD })
	if i < 0 || !lines[i].Amount.IsNegative() {
		return nil
	}
	var available decimal.Decimal
	err := tx.QueryRow(ctx, `
		SELECT COALESCE((SELECT balance - held FROM ledger_accounts WHERE account_id = $1 AND asset = 'USD'), 0)`,
		a.ID).Scan(&available)
	if err != nil {
		return fmt.Errorf("load available cash of %s: %w", a.ID, err)
	}
	shortfall := lines[i].Amount.Neg().Sub(decimal.Max(available, decimal.Zero))
	if !shortfall.IsPositive() {
		return nil
	}
	pending, err := prepareEntry(ctx, tx, posting{
		kind:     KindDeficit,
		metadata: map[string]string{"reason": reason},
		lines: []Line{
			{AccountID: Deficits, Asset: USD, Amount: shortfall.Neg()},
			{AccountID: a.ID, Asset: USD, Amount: shortfall},
		},
	})
	if err != nil {
		return err
	}
	if err = pending.insert(ctx, tx); err != nil {
		return err
	}
	if _, err = pending.finish(ctx, tx, nil); err != nil {
		return err
	}
	if a.Status != AccountActive {
		return nil
	}
	_, err = changeStatus(ctx, tx, a, AccountRestricted, reason)
	return err
}
