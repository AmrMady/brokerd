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
	ErrAccountNotEmpty     = errors.New("account still has balances or holds")
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
	KindDeposit    Kind = "deposit"
	KindWithdrawal Kind = "withdrawal"
	KindTradeFill  Kind = "trade_fill"
)

// Line adds Amount of Asset to an account. Negative amounts take it away.
type Line struct {
	AccountID string
	Asset     Asset
	Amount    decimal.Decimal
}

// Entry is a set of lines, summing to zero per asset, posted atomically.
type Entry struct {
	ID        int64
	Kind      Kind
	HoldID    int64 // hold the entry settled, if any
	Metadata  map[string]string
	Lines     []Line
	CreatedAt time.Time
}

// CashTransfer is a deposit or withdrawal request.
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

// Deposit moves cash from the external bank into an account. Closed accounts
// can't receive deposits.
func Deposit(ctx context.Context, tx pgx.Tx, c CashTransfer) (Entry, error) {
	if err := c.validate(); err != nil {
		return Entry{}, err
	}
	return postEntry(ctx, tx, posting{
		account: c.AccountID,
		key:     c.IdempotencyKey,
		kind:    KindDeposit,
		lines: []Line{
			{AccountID: ExternalBank, Asset: USD, Amount: c.Amount.Neg()},
			{AccountID: c.AccountID, Asset: USD, Amount: c.Amount},
		},
	}, AccountActive, AccountRestricted)
}

// Withdraw moves cash from an account to the external bank. It fails with
// ErrInsufficientFunds if the amount is more than the available balance, and
// only active accounts can withdraw.
func Withdraw(ctx context.Context, tx pgx.Tx, c CashTransfer) (Entry, error) {
	if err := c.validate(); err != nil {
		return Entry{}, err
	}
	return postEntry(ctx, tx, posting{
		account: c.AccountID,
		key:     c.IdempotencyKey,
		kind:    KindWithdrawal,
		lines: []Line{
			{AccountID: c.AccountID, Asset: USD, Amount: c.Amount.Neg()},
			{AccountID: ExternalBank, Asset: USD, Amount: c.Amount},
		},
	}, AccountActive)
}

func postEntry(ctx context.Context, tx pgx.Tx, p posting, allowed ...AccountStatus) (Entry, error) {
	status, err := lockAccount(ctx, tx, p.account)
	if err != nil {
		return Entry{}, err
	}
	pending, replayed, err := startEntry(ctx, tx, p)
	if err != nil || pending == nil {
		return replayed, err
	}
	if err = requireStatus(p.account, status, allowed...); err != nil {
		return Entry{}, err
	}
	return pending.finish(ctx, tx, nil)
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
