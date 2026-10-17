// Package ledger is a double-entry ledger for cash and share positions,
// stored in Postgres.
//
// Writes run inside a transaction owned by the caller. The transaction must
// use READ COMMITTED and must be rolled back if a write returns an error.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// Errors returned by ledger operations.
var (
	ErrInvalid             = errors.New("invalid ledger request")
	ErrUnbalanced          = errors.New("entry does not balance")
	ErrInsufficientFunds   = errors.New("insufficient available balance")
	ErrIdempotencyConflict = errors.New("idempotency key already used for a different request")
	ErrHoldNotFound        = errors.New("hold not found")
	ErrHoldClosed          = errors.New("hold is no longer active")
	ErrHoldExceeded        = errors.New("fill exceeds its hold")
	ErrAccountNotFound     = errors.New("account not found")
	ErrAccountRestricted   = errors.New("account is restricted")
	ErrAccountClosed       = errors.New("account is closed")
	ErrAccountNotEmpty     = errors.New("account still has balances, holds or pending transfers")
	ErrTransferNotFound    = errors.New("transfer not found")
	ErrTransferStatus      = errors.New("transfer status doesn't allow this")
)

// Querier is implemented by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Kind is the type of a journal entry.
type Kind string

// Entry kinds.
const (
	KindDeposit          Kind = "deposit"
	KindWithdrawal       Kind = "withdrawal"
	KindDepositReturn    Kind = "deposit_return"
	KindWithdrawalReturn Kind = "withdrawal_return"
	KindTradeFill        Kind = "trade_fill"
)

// Line adds Amount of Asset to an account. Negative amounts take it away.
type Line struct {
	AccountID string
	Asset     Asset
	Amount    decimal.Decimal
}

// Entry is a set of lines, summing to zero per asset, posted atomically.
type Entry struct {
	ID         int64
	Kind       Kind
	HoldID     int64 // hold the entry took from, if any
	TransferID int64 // transfer the entry booked, if any
	Metadata   map[string]string
	Lines      []Line
	CreatedAt  time.Time
}

// CashTransfer is a request to deposit or withdraw cash.
type CashTransfer struct {
	IdempotencyKey string
	AccountID      string
	Amount         decimal.Decimal
}

func (c CashTransfer) validate() error {
	if err := validateKey(c.IdempotencyKey); err != nil {
		return err
	}
	if err := validateCustomer(c.AccountID); err != nil {
		return err
	}
	return checkPositive("amount", c.Amount, usdScale)
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: idempotency key is required", ErrInvalid)
	}
	if len(key) > 255 {
		return fmt.Errorf("%w: idempotency key is longer than 255 bytes", ErrInvalid)
	}
	return nil
}
