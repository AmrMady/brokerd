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

// Hold statuses. A hold starts active and is closed exactly once.
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

// Hold is an amount reserved in an account. While active, it counts toward
// the account's held balance.
type Hold struct {
	ID        int64
	AccountID string
	Asset     Asset
	Amount    decimal.Decimal
	Status    HoldStatus
	CreatedAt time.Time
	ClosedAt  time.Time
}

// PlaceHold reserves r.Amount. It fails with ErrInsufficientFunds if the
// available balance is too low. Retrying with the same key returns the
// original hold.
func PlaceHold(ctx context.Context, tx pgx.Tx, r HoldRequest) (Hold, error) {
	if err := r.validate(); err != nil {
		return Hold{}, err
	}
	if err := checkScales(ctx, tx, []Line{{AccountID: r.AccountID, Asset: r.Asset, Amount: r.Amount}}); err != nil {
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

	h := Hold{AccountID: r.AccountID, Asset: r.Asset, Amount: r.Amount, Status: HoldActive}
	err = tx.QueryRow(ctx, `
		WITH h AS (
			INSERT INTO holds (ledger_account_id, amount)
			VALUES ($3, $4)
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

// ReleaseHold makes an active hold's amount available again. Releasing a
// hold twice is a no-op; releasing a settled hold returns ErrHoldClosed.
func ReleaseHold(ctx context.Context, tx pgx.Tx, holdID int64) (Hold, error) {
	h, closed, err := closeHold(ctx, tx, holdID, HoldReleased)
	if err != nil {
		return Hold{}, err
	}
	if !closed {
		if h.Status == HoldReleased {
			return h.Hold, nil
		}
		return Hold{}, fmt.Errorf("%w: hold %d is %s", ErrHoldClosed, holdID, h.Status)
	}

	if _, err = applyChanges(ctx, tx, map[int64]change{h.ledgerAccountID: {held: h.Amount.Neg()}}); err != nil {
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

const holdColumns = `h.id, la.account_id::text, la.asset, h.amount, h.status,
	h.created_at, h.closed_at, h.ledger_account_id`

func scanHold(row pgx.Row) (holdRow, error) {
	var (
		h        holdRow
		closedAt *time.Time
	)
	err := row.Scan(&h.ID, &h.AccountID, &h.Asset, &h.Amount, &h.Status,
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

// closeHold moves an active hold to status. The WHERE status = 'active'
// makes it a compare-and-set, so a release and a fill racing for the same
// hold can't both succeed. closed is false if the hold wasn't active.
func closeHold(ctx context.Context, tx pgx.Tx, id int64, status HoldStatus) (h holdRow, closed bool, err error) {
	h, err = scanHold(tx.QueryRow(ctx, `
		UPDATE holds h
		SET status = $2, closed_at = now()
		FROM ledger_accounts la
		WHERE h.id = $1 AND h.status = 'active' AND la.id = h.ledger_account_id
		RETURNING `+holdColumns,
		id, status))
	if err == nil {
		return h, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return holdRow{}, false, fmt.Errorf("close hold %d: %w", id, err)
	}
	h, err = loadHold(ctx, tx, id)
	return h, false, err
}
