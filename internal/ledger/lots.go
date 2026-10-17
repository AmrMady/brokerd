package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// Lot is shares bought in one fill. Cost includes the buy's fee. A position
// that existed before lots were tracked has one lot whose cost is unknown.
type Lot struct {
	ID            int64
	AccountID     string
	Symbol        Asset
	Quantity      decimal.Decimal
	Remaining     decimal.Decimal
	Cost          decimal.NullDecimal
	RemainingCost decimal.NullDecimal
	AcquiredOn    time.Time
}

// RealizedGain is the gain or loss from selling some of a lot. Proceeds are
// net of the sell's fee. Cost and Gain are null when the lot's cost is
// unknown. A sale is long-term if the lot was held for more than a year.
type RealizedGain struct {
	LotID      int64
	EntryID    int64
	Symbol     Asset
	Quantity   decimal.Decimal
	Proceeds   decimal.Decimal
	Cost       decimal.NullDecimal
	Gain       decimal.NullDecimal
	AcquiredOn time.Time
	SoldOn     time.Time
	LongTerm   bool
}

// OpenLots returns the lots of a position that still have shares, oldest
// first: the order sells take them in.
func OpenLots(ctx context.Context, q Querier, accountID string, symbol Asset) ([]Lot, error) {
	if err := validateAccountID(accountID); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, `
		SELECT t.id, la.account_id::text, la.asset, t.quantity, t.remaining, t.cost, t.remaining_cost, t.acquired_on
		FROM tax_lots t
		JOIN ledger_accounts la ON la.id = t.ledger_account_id
		WHERE la.account_id = $1 AND la.asset = $2 AND t.remaining > 0
		ORDER BY t.acquired_on, t.id`, accountID, symbol)
	if err != nil {
		return nil, fmt.Errorf("load lots of %s/%s: %w", accountID, symbol, err)
	}
	lots, err := pgx.CollectRows(rows, scanLot)
	if err != nil {
		return nil, fmt.Errorf("load lots of %s/%s: %w", accountID, symbol, err)
	}
	return lots, nil
}

func scanLot(row pgx.CollectableRow) (Lot, error) {
	var l Lot
	err := row.Scan(&l.ID, &l.AccountID, &l.Symbol, &l.Quantity, &l.Remaining, &l.Cost, &l.RemainingCost, &l.AcquiredOn)
	return l, err
}

// RealizedGains returns the gains and losses an account has realized, in the
// order the sales happened. Sales that were reversed are left out.
func RealizedGains(ctx context.Context, q Querier, accountID string) ([]RealizedGain, error) {
	if err := validateAccountID(accountID); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, `
		SELECT s.lot_id, s.entry_id, la.asset, s.quantity, s.proceeds, s.cost, s.gain,
		       t.acquired_on, s.sold_on, s.long_term
		FROM lot_sales s
		JOIN tax_lots t ON t.id = s.lot_id
		JOIN ledger_accounts la ON la.id = t.ledger_account_id
		WHERE la.account_id = $1 AND s.reversed_at IS NULL
		ORDER BY s.sold_on, s.id`, accountID)
	if err != nil {
		return nil, fmt.Errorf("load realized gains of %s: %w", accountID, err)
	}
	gains, err := pgx.CollectRows(rows, scanRealizedGain)
	if err != nil {
		return nil, fmt.Errorf("load realized gains of %s: %w", accountID, err)
	}
	return gains, nil
}

func scanRealizedGain(row pgx.CollectableRow) (RealizedGain, error) {
	var g RealizedGain
	err := row.Scan(&g.LotID, &g.EntryID, &g.Symbol, &g.Quantity, &g.Proceeds, &g.Cost, &g.Gain,
		&g.AcquiredOn, &g.SoldOn, &g.LongTerm)
	return g, err
}

// openLot records the shares a buy added to a position.
func openLot(ctx context.Context, tx pgx.Tx, position, entryID int64, f Fill) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tax_lots (ledger_account_id, entry_id, quantity, remaining, cost, remaining_cost, acquired_on)
		VALUES ($1, $2, $3, $3, $4, $4, $5)`,
		position, entryID, f.Quantity, f.Notional().Add(f.Fee), dateOnly(f.TradeDate))
	if err != nil {
		return fmt.Errorf("open lot: %w", err)
	}
	return nil
}

type openLotRow struct {
	id            int64
	remaining     decimal.Decimal
	remainingCost decimal.NullDecimal
	acquiredOn    time.Time
}

// sellLots takes a sell's shares out of the position's open lots, oldest
// first, and records the gain or loss on each lot. proceeds are net of all
// fees. A lot's cost goes with its
// shares in proportion to what's left of it, so the last share takes the
// last of the cost. The proceeds are split the same way across the lots
// used. Both are rounded to the cent, and nothing is lost to rounding.
func sellLots(ctx context.Context, tx pgx.Tx, position, entryID int64, f Fill, proceeds decimal.Decimal) error {
	// Locked in the order they're used, the same order for every sell.
	rows, err := tx.Query(ctx, `
		SELECT id, remaining, remaining_cost, acquired_on
		FROM tax_lots
		WHERE ledger_account_id = $1 AND remaining > 0
		ORDER BY acquired_on, id
		FOR UPDATE`, position)
	if err != nil {
		return fmt.Errorf("lock lots: %w", err)
	}
	lots, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (openLotRow, error) {
		var l openLotRow
		scanErr := row.Scan(&l.id, &l.remaining, &l.remainingCost, &l.acquiredOn)
		return l, scanErr
	})
	if err != nil {
		return fmt.Errorf("lock lots: %w", err)
	}

	soldOn := dateOnly(f.TradeDate)
	left, allocated := f.Quantity, decimal.Zero
	for _, lot := range lots {
		if left.IsZero() {
			break
		}
		take := decimal.Min(lot.remaining, left)
		left = left.Sub(take)

		part := proceeds.Sub(allocated)
		if left.IsPositive() {
			part = proceeds.Mul(take).Div(f.Quantity).RoundBank(usdScale)
		}
		allocated = allocated.Add(part)

		var cost, gain decimal.NullDecimal
		if lot.remainingCost.Valid {
			c := lot.remainingCost.Decimal.Mul(take).Div(lot.remaining).RoundBank(usdScale)
			cost = decimal.NewNullDecimal(c)
			gain = decimal.NewNullDecimal(part.Sub(c))
		}
		longTerm := soldOn.After(lot.acquiredOn.AddDate(1, 0, 0))

		_, err = tx.Exec(ctx, `
			UPDATE tax_lots SET remaining = remaining - $2, remaining_cost = remaining_cost - $3
			WHERE id = $1`, lot.id, take, cost)
		if err != nil {
			return fmt.Errorf("update lot %d: %w", lot.id, err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO lot_sales (lot_id, entry_id, quantity, proceeds, cost, gain, sold_on, long_term)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			lot.id, entryID, take, part, cost, gain, soldOn, longTerm)
		if err != nil {
			return fmt.Errorf("record sale of lot %d: %w", lot.id, err)
		}
	}
	if left.IsPositive() {
		return fmt.Errorf("position %d has no open lots for %s of the shares sold", position, left)
	}
	return nil
}

// reverseLots undoes what a fill did to the lots: a buy's lot is closed, and
// a sell's shares go back to the lots it took them from.
func reverseLots(ctx context.Context, tx pgx.Tx, entryID int64) error {
	var (
		lotID               int64
		quantity, remaining decimal.Decimal
	)
	err := tx.QueryRow(ctx, `
		SELECT id, quantity, remaining FROM tax_lots WHERE entry_id = $1 FOR UPDATE`,
		entryID).Scan(&lotID, &quantity, &remaining)
	switch {
	case err == nil:
		if !remaining.Equal(quantity) {
			return fmt.Errorf("%w: shares from entry %d were already sold; reverse those sales first", ErrInvalid, entryID)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE tax_lots SET remaining = 0, remaining_cost = 0, reversed_at = now()
			WHERE id = $1`, lotID); err != nil {
			return fmt.Errorf("close lot %d: %w", lotID, err)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("load lot of entry %d: %w", entryID, err)
	}

	// Lock the lots in the same order sells do before changing them.
	if _, err = tx.Exec(ctx, `
		SELECT t.id FROM tax_lots t
		JOIN lot_sales s ON s.lot_id = t.id
		WHERE s.entry_id = $1 AND s.reversed_at IS NULL
		ORDER BY t.acquired_on, t.id
		FOR UPDATE OF t`, entryID); err != nil {
		return fmt.Errorf("lock lots of entry %d: %w", entryID, err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE tax_lots t
		SET remaining = t.remaining + s.quantity, remaining_cost = t.remaining_cost + s.cost
		FROM lot_sales s
		WHERE s.entry_id = $1 AND s.reversed_at IS NULL AND t.id = s.lot_id`, entryID); err != nil {
		return fmt.Errorf("restore lots of entry %d: %w", entryID, err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE lot_sales SET reversed_at = now()
		WHERE entry_id = $1 AND reversed_at IS NULL`, entryID); err != nil {
		return fmt.Errorf("reverse sales of entry %d: %w", entryID, err)
	}
	return nil
}
