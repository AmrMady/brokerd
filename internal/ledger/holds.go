package ledger

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// HoldStatus is the state of a hold.
type HoldStatus string

// Hold statuses. A hold starts active and is closed exactly once: settled
// when fills have used all of it, or released.
const (
	HoldActive   HoldStatus = "active"
	HoldReleased HoldStatus = "released"
	HoldSettled  HoldStatus = "settled"
)

// HoldRequest reserves Amount of Asset in an account, e.g. cash for a buy
// order or shares for a sell order.
type HoldRequest struct {
	IdempotencyKey string
	AccountID      string
	Asset          Asset
	Amount         decimal.Decimal
}

func (r HoldRequest) validate() error {
	if err := validateKey(r.IdempotencyKey); err != nil {
		return err
	}
	if err := validateCustomer(r.AccountID); err != nil {
		return err
	}
	if err := r.Asset.validate(); err != nil {
		return err
	}
	return checkPositive("amount", r.Amount, maxScale)
}

func (r HoldRequest) hash() []byte {
	sum := sha256.Sum256(fmt.Appendf(nil, "hold %q %s", r.Asset, r.Amount))
	return sum[:]
}

// Hold is an amount reserved in an account. Fills use it up a part at a
// time; while it's active, Remaining counts toward the account's held
// balance.
type Hold struct {
	ID        int64
	AccountID string
	Asset     Asset
	Amount    decimal.Decimal
	Remaining decimal.Decimal
	Status    HoldStatus
	CreatedAt time.Time
	ClosedAt  time.Time
}

// PlaceHold reserves r.Amount. It fails with ErrInsufficientFunds if the
// available balance is too low, and only active accounts can place holds.
// Retrying with the same key returns the original hold.
func PlaceHold(ctx context.Context, tx pgx.Tx, r HoldRequest) (Hold, error) {
	if err := r.validate(); err != nil {
		return Hold{}, err
	}
	status, err := lockAccount(ctx, tx, r.AccountID)
	if err != nil {
		return Hold{}, err
	}
	if err = checkScales(ctx, tx, []Line{{AccountID: r.AccountID, Asset: r.Asset, Amount: r.Amount}}); err != nil {
		return Hold{}, err
	}
	ref := accountRef{account: r.AccountID, asset: r.Asset}
	ids, err := resolveAccounts(ctx, tx, []accountRef{ref})
	if err != nil {
		return Hold{}, err
	}

	hash := r.hash()
	claimed, err := claimKey(ctx, tx, r.AccountID, r.IdempotencyKey, hash)
	if err != nil {
		return Hold{}, err
	}
	if !claimed {
		return replayHold(ctx, tx, r.AccountID, r.IdempotencyKey, hash)
	}
	if err = requireStatus(r.AccountID, status, AccountActive); err != nil {
		return Hold{}, err
	}

	h := Hold{AccountID: r.AccountID, Asset: r.Asset, Amount: r.Amount, Remaining: r.Amount, Status: HoldActive}
	err = tx.QueryRow(ctx, `
		WITH h AS (
			INSERT INTO holds (ledger_account_id, amount, remaining)
			VALUES ($3, $4, $4)
			RETURNING id, created_at
		)
		UPDATE idempotency_keys k SET hold_id = h.id
		FROM h
		WHERE k.account_id = $1 AND k.idempotency_key = $2
		RETURNING h.id, h.created_at`,
		r.AccountID, r.IdempotencyKey, ids[ref], r.Amount,
	).Scan(&h.ID, &h.CreatedAt)
	if err != nil {
		return Hold{}, fmt.Errorf("insert hold: %w", err)
	}

	if _, err = applyChanges(ctx, tx, map[int64]change{ids[ref]: {held: r.Amount}}); err != nil {
		return Hold{}, err
	}
	return h, nil
}

func replayHold(ctx context.Context, tx pgx.Tx, account, key string, hash []byte) (Hold, error) {
	_, holdID, err := usedKey(ctx, tx, account, key, hash)
	if err != nil {
		return Hold{}, err
	}
	if holdID == 0 {
		return Hold{}, fmt.Errorf("%w: %q", ErrIdempotencyConflict, key)
	}
	return HoldByID(ctx, tx, holdID)
}

// ReleaseHold closes an active hold and makes what's left of it available
// again. Releasing a hold twice is a no-op; releasing a hold that fills have
// used up returns ErrHoldClosed.
func ReleaseHold(ctx context.Context, tx pgx.Tx, holdID int64) (Hold, error) {
	current, err := loadHold(ctx, tx, holdID)
	if err != nil {
		return Hold{}, err
	}
	if _, err = lockAccount(ctx, tx, current.AccountID); err != nil {
		return Hold{}, err
	}
	h, err := scanHold(tx.QueryRow(ctx, `
		UPDATE holds h
		SET status = 'released', closed_at = now()
		FROM ledger_accounts la
		WHERE h.id = $1 AND h.status = 'active' AND la.id = h.ledger_account_id
		RETURNING `+holdColumns,
		holdID))
	if errors.Is(err, pgx.ErrNoRows) {
		// No longer active; read it again to see what closed it.
		if current, err = loadHold(ctx, tx, holdID); err != nil {
			return Hold{}, err
		}
		if current.Status == HoldReleased {
			return current.Hold, nil
		}
		return Hold{}, fmt.Errorf("%w: hold %d is %s", ErrHoldClosed, holdID, current.Status)
	}
	if err != nil {
		return Hold{}, fmt.Errorf("release hold %d: %w", holdID, err)
	}

	if _, err = applyChanges(ctx, tx, map[int64]change{h.ledgerAccountID: {held: h.Remaining.Neg()}}); err != nil {
		return Hold{}, err
	}
	return h.Hold, nil
}

// HoldByID returns the hold with the given id.
func HoldByID(ctx context.Context, q Querier, id int64) (Hold, error) {
	h, err := loadHold(ctx, q, id)
	return h.Hold, err
}

type holdRow struct {
	Hold
	ledgerAccountID int64
}

const holdColumns = `h.id, la.account_id::text, la.asset, h.amount, h.remaining, h.status,
	h.created_at, h.closed_at, h.ledger_account_id`

func scanHold(row pgx.Row) (holdRow, error) {
	var (
		h        holdRow
		closedAt *time.Time
	)
	err := row.Scan(&h.ID, &h.AccountID, &h.Asset, &h.Amount, &h.Remaining, &h.Status,
		&h.CreatedAt, &closedAt, &h.ledgerAccountID)
	if closedAt != nil {
		h.ClosedAt = *closedAt
	}
	return h, err
}

func loadHold(ctx context.Context, q Querier, id int64) (holdRow, error) {
	h, err := scanHold(q.QueryRow(ctx, `
		SELECT `+holdColumns+`
		FROM holds h
		JOIN ledger_accounts la ON la.id = h.ledger_account_id
		WHERE h.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return holdRow{}, fmt.Errorf("%w: %d", ErrHoldNotFound, id)
	}
	if err != nil {
		return holdRow{}, fmt.Errorf("load hold %d: %w", id, err)
	}
	return h, nil
}

// consumeHold takes amount out of an active hold on ledgerAccountID, and
// settles the hold if that uses it up. The conditional UPDATE makes this
// safe under concurrency: fills against the same hold queue on its row, and
// each one checks what the previous one left.
func consumeHold(ctx context.Context, tx pgx.Tx, id, ledgerAccountID int64, amount decimal.Decimal) (holdRow, error) {
	h, err := scanHold(tx.QueryRow(ctx, `
		UPDATE holds h
		SET remaining = h.remaining - $3,
		    status = CASE WHEN h.remaining = $3 THEN 'settled' ELSE h.status END,
		    closed_at = CASE WHEN h.remaining = $3 THEN now() END
		FROM ledger_accounts la
		WHERE h.id = $1 AND h.ledger_account_id = $2 AND h.status = 'active' AND h.remaining >= $3
		  AND la.id = h.ledger_account_id
		RETURNING `+holdColumns,
		id, ledgerAccountID, amount))
	if err == nil {
		return h, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return holdRow{}, fmt.Errorf("use hold %d: %w", id, err)
	}

	if h, err = loadHold(ctx, tx, id); err != nil {
		return holdRow{}, err
	}
	switch {
	case h.Status != HoldActive:
		return holdRow{}, fmt.Errorf("%w: hold %d is %s", ErrHoldClosed, id, h.Status)
	case h.ledgerAccountID != ledgerAccountID:
		return holdRow{}, fmt.Errorf("%w: hold %d is on %s/%s", ErrInvalid, id, h.AccountID, h.Asset)
	default:
		return holdRow{}, fmt.Errorf("%w: hold %d has %s left, the fill needs %s", ErrHoldExceeded, id, h.Remaining, amount)
	}
}
