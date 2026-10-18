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

// Direction says whether a transfer moves cash into or out of an account.
type Direction string

// Transfer directions.
const (
	DirectionDeposit    Direction = "deposit"
	DirectionWithdrawal Direction = "withdrawal"
)

// TransferStatus is the state of a transfer. A transfer starts pending and is
// completed or canceled when the bank answers. A completed transfer can later
// be returned by the bank.
type TransferStatus string

// Transfer statuses.
const (
	TransferPending   TransferStatus = "pending"
	TransferCompleted TransferStatus = "completed"
	TransferReturned  TransferStatus = "returned"
	TransferCanceled  TransferStatus = "canceled"
)

// Transfer moves cash between an account and the customer's bank.
type Transfer struct {
	ID           int64
	AccountID    string
	Direction    Direction
	Amount       decimal.Decimal
	Status       TransferStatus
	ReturnReason string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type transferRow struct {
	Transfer
	holdID int64
}

const transferColumns = `id, account_id::text, direction, amount, status, coalesce(return_reason, ''),
	hold_id, created_at, updated_at`

func scanTransfer(row pgx.Row) (transferRow, error) {
	var (
		t      transferRow
		holdID *int64
	)
	err := row.Scan(&t.ID, &t.AccountID, &t.Direction, &t.Amount, &t.Status, &t.ReturnReason,
		&holdID, &t.CreatedAt, &t.UpdatedAt)
	t.holdID = deref(holdID)
	return t, err
}

func loadTransfer(ctx context.Context, q Querier, id int64) (transferRow, error) {
	t, err := scanTransfer(q.QueryRow(ctx, `SELECT `+transferColumns+` FROM transfers WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return transferRow{}, fmt.Errorf("%w: %d", ErrTransferNotFound, id)
	}
	if err != nil {
		return transferRow{}, fmt.Errorf("load transfer %d: %w", id, err)
	}
	return t, nil
}

// TransferByID returns the transfer with the given id.
func TransferByID(ctx context.Context, q Querier, id int64) (Transfer, error) {
	t, err := loadTransfer(ctx, q, id)
	return t.Transfer, err
}

// RequestDeposit records a deposit the bank has yet to confirm. Nothing is
// credited until CompleteTransfer. Closed accounts can't receive deposits.
// Retrying with the same key returns the original transfer.
func RequestDeposit(ctx context.Context, tx pgx.Tx, c CashTransfer) (Transfer, error) {
	return requestTransfer(ctx, tx, c, DirectionDeposit)
}

// RequestWithdrawal holds the amount and records a withdrawal for the bank to
// send. It fails with ErrInsufficientFunds if the amount is more than the
// available balance, and only active accounts can withdraw. Retrying with the
// same key returns the original transfer.
func RequestWithdrawal(ctx context.Context, tx pgx.Tx, c CashTransfer) (Transfer, error) {
	return requestTransfer(ctx, tx, c, DirectionWithdrawal)
}

func requestTransfer(ctx context.Context, tx pgx.Tx, c CashTransfer, d Direction) (Transfer, error) {
	if err := c.validate(); err != nil {
		return Transfer{}, err
	}
	status, err := lockAccount(ctx, tx, c.AccountID)
	if err != nil {
		return Transfer{}, err
	}
	cash := accountRef{account: c.AccountID, asset: USD}
	var ids map[accountRef]int64
	if d == DirectionWithdrawal {
		if ids, err = resolveAccounts(ctx, tx, []accountRef{cash}); err != nil {
			return Transfer{}, err
		}
	}

	sum := sha256.Sum256(fmt.Appendf(nil, "transfer %q %s", d, c.Amount))
	claimed, err := claimKey(ctx, tx, opTransfer, c.AccountID, c.IdempotencyKey, sum[:])
	if err != nil {
		return Transfer{}, err
	}
	if !claimed {
		return replayTransfer(ctx, tx, c.AccountID, c.IdempotencyKey, sum[:])
	}

	var holdID int64
	if d == DirectionDeposit {
		err = requireStatus(c.AccountID, status, AccountActive, AccountRestricted)
	} else {
		if err = requireStatus(c.AccountID, status, AccountActive); err == nil {
			// A withdrawal can only use settled cash.
			holdID, _, err = reserve(ctx, tx, ids[cash], change{held: c.Amount, settledOnly: true})
		}
	}
	if err != nil {
		return Transfer{}, err
	}

	t, err := scanTransfer(tx.QueryRow(ctx, `
		INSERT INTO transfers (account_id, direction, amount, hold_id)
		VALUES ($1, $2, $3, $4)
		RETURNING `+transferColumns,
		c.AccountID, d, c.Amount, nullable(holdID)))
	if err != nil {
		return Transfer{}, fmt.Errorf("insert transfer: %w", err)
	}
	if err = recordKey(ctx, tx, opTransfer, c.AccountID, c.IdempotencyKey, keyResult{transferID: t.ID}); err != nil {
		return Transfer{}, err
	}
	return t.Transfer, nil
}

func replayTransfer(ctx context.Context, tx pgx.Tx, account, key string, hash []byte) (Transfer, error) {
	used, err := usedKey(ctx, tx, opTransfer, account, key, hash)
	if err != nil {
		return Transfer{}, err
	}
	if used.transferID == 0 {
		return Transfer{}, fmt.Errorf("%w: %q", ErrIdempotencyConflict, key)
	}
	return TransferByID(ctx, tx, used.transferID)
}

// CompleteTransfer books a transfer the bank has confirmed: a deposit is
// credited, and a withdrawal is taken out of its hold. Completing a completed
// transfer is a no-op, so repeated notifications from the bank are harmless.
func CompleteTransfer(ctx context.Context, tx pgx.Tx, id int64) (Transfer, error) {
	current, err := loadTransfer(ctx, tx, id)
	if err != nil {
		return Transfer{}, err
	}
	if _, err = lockAccount(ctx, tx, current.AccountID); err != nil {
		return Transfer{}, err
	}
	p := posting{account: current.AccountID, kind: KindDeposit, transferID: id, lines: []Line{
		{AccountID: ExternalBank, Asset: USD, Amount: current.Amount.Neg()},
		{AccountID: current.AccountID, Asset: USD, Amount: current.Amount},
	}}
	if current.Direction == DirectionWithdrawal {
		p.kind, p.holdID = KindWithdrawal, current.holdID
		p.lines = []Line{
			{AccountID: current.AccountID, Asset: USD, Amount: current.Amount.Neg()},
			{AccountID: ExternalBank, Asset: USD, Amount: current.Amount},
		}
	}
	pending, err := prepareEntry(ctx, tx, p)
	if err != nil {
		return Transfer{}, err
	}

	t, changed, err := moveTransfer(ctx, tx, id, TransferPending, TransferCompleted, "")
	if err != nil || !changed {
		return t.Transfer, err
	}
	if err = pending.insert(ctx, tx); err != nil {
		return Transfer{}, err
	}
	var held map[int64]change
	if t.Direction == DirectionWithdrawal {
		cash := pending.accounts[accountRef{account: t.AccountID, asset: USD}]
		if err = consumeHold(ctx, tx, t.holdID, cash, t.Amount); err != nil {
			return Transfer{}, err
		}
		held = map[int64]change{cash: {held: t.Amount.Neg()}}
	}
	if _, err = pending.finish(ctx, tx, held); err != nil {
		return Transfer{}, err
	}
	return t.Transfer, nil
}

// CancelTransfer cancels a pending transfer and releases a withdrawal's hold.
// Canceling a canceled transfer is a no-op.
func CancelTransfer(ctx context.Context, tx pgx.Tx, id int64) (Transfer, error) {
	current, err := loadTransfer(ctx, tx, id)
	if err != nil {
		return Transfer{}, err
	}
	if _, err = lockAccount(ctx, tx, current.AccountID); err != nil {
		return Transfer{}, err
	}
	t, changed, err := moveTransfer(ctx, tx, id, TransferPending, TransferCanceled, "")
	if err != nil || !changed {
		return t.Transfer, err
	}
	if t.Direction == DirectionWithdrawal {
		if _, err = release(ctx, tx, t.holdID); err != nil {
			return Transfer{}, err
		}
	}
	return t.Transfer, nil
}

// ReturnTransfer reverses a completed transfer the bank sent back, such as an
// ACH return. A returned withdrawal is credited back. A returned deposit is
// taken back out of the account; if the customer has already spent some of
// it, the deficits account covers the rest with a deficit entry, and the
// account is restricted.
// Returning a returned transfer is a no-op.
func ReturnTransfer(ctx context.Context, tx pgx.Tx, id int64, reason string) (Transfer, error) {
	if err := validateReason(reason); err != nil {
		return Transfer{}, err
	}
	current, err := loadTransfer(ctx, tx, id)
	if err != nil {
		return Transfer{}, err
	}
	// The exclusive lock keeps every other write off this account, so what's
	// available can't change between reading it and taking it.
	account, err := lockAccountForUpdate(ctx, tx, current.AccountID)
	if err != nil {
		return Transfer{}, err
	}

	p := posting{account: current.AccountID, kind: KindWithdrawalReturn, transferID: id, lines: []Line{
		{AccountID: ExternalBank, Asset: USD, Amount: current.Amount.Neg()},
		{AccountID: current.AccountID, Asset: USD, Amount: current.Amount},
	}}
	if current.Direction == DirectionDeposit {
		p.kind = KindDepositReturn
		p.lines = []Line{
			{AccountID: ExternalBank, Asset: USD, Amount: current.Amount},
			{AccountID: current.AccountID, Asset: USD, Amount: current.Amount.Neg()},
		}
	}
	pending, err := prepareEntry(ctx, tx, p)
	if err != nil {
		return Transfer{}, err
	}

	t, changed, err := moveTransfer(ctx, tx, id, TransferCompleted, TransferReturned, reason)
	if err != nil || !changed {
		return t.Transfer, err
	}
	if err = coverShortfall(ctx, tx, p.lines, account, "deposit returned: "+reason); err != nil {
		return Transfer{}, err
	}
	if err = pending.insert(ctx, tx); err != nil {
		return Transfer{}, err
	}
	if _, err = pending.finish(ctx, tx, nil); err != nil {
		return Transfer{}, err
	}
	return t.Transfer, nil
}

// moveTransfer changes a transfer's status from one value to another. The
// WHERE status = $2 makes it a compare-and-set, so of two racing bank events
// only one takes effect. If the transfer is already in the target status,
// changed is false and there is no error.
func moveTransfer(ctx context.Context, tx pgx.Tx, id int64, from, to TransferStatus, reason string) (t transferRow, changed bool, err error) {
	t, err = scanTransfer(tx.QueryRow(ctx, `
		UPDATE transfers
		SET status = $3, return_reason = NULLIF($4, ''), updated_at = now()
		WHERE id = $1 AND status = $2
		RETURNING `+transferColumns,
		id, from, to, reason))
	if err == nil {
		return t, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return transferRow{}, false, fmt.Errorf("update transfer %d: %w", id, err)
	}
	if t, err = loadTransfer(ctx, tx, id); err != nil {
		return transferRow{}, false, err
	}
	if t.Status == to {
		return t, false, nil
	}
	return transferRow{}, false, fmt.Errorf("%w: transfer %d is %s", ErrTransferStatus, id, t.Status)
}
