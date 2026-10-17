package ledger

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// Side is buy or sell.
type Side string

// Trade sides.
const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

// Fill is an executed trade to be settled against the hold of its order.
type Fill struct {
	ID        string // also the idempotency key
	HoldID    int64
	AccountID string
	Symbol    Asset
	Side      Side
	Quantity  decimal.Decimal
	Price     decimal.Decimal
	Fee       decimal.Decimal // USD
}

// Notional returns quantity * price, rounded half-to-even to the cent.
func (f Fill) Notional() decimal.Decimal {
	return f.Quantity.Mul(f.Price).RoundBank(usdScale)
}

func (f Fill) validate() error {
	if err := validateKey(f.ID); err != nil {
		return err
	}
	if f.HoldID <= 0 {
		return fmt.Errorf("%w: hold id is required", ErrInvalid)
	}
	if err := validateCustomer(f.AccountID); err != nil {
		return err
	}
	if err := validateSymbol(f.Symbol); err != nil {
		return err
	}
	if f.Side != Buy && f.Side != Sell {
		return fmt.Errorf("%w: side must be buy or sell, got %q", ErrInvalid, f.Side)
	}
	if err := checkPositive("quantity", f.Quantity, maxScale); err != nil {
		return err
	}
	if err := checkPositive("price", f.Price, maxScale); err != nil {
		return err
	}
	if f.Fee.IsNegative() {
		return fmt.Errorf("%w: fee must not be negative, got %s", ErrInvalid, f.Fee)
	}
	if err := checkDecimal("fee", f.Fee, usdScale); err != nil {
		return err
	}
	if f.Notional().IsZero() {
		return fmt.Errorf("%w: %s shares at %s is worth less than half a cent", ErrInvalid, f.Quantity, f.Price)
	}
	return nil
}

func (f Fill) lines() []Line {
	n := f.Notional()
	var lines []Line
	switch f.Side {
	case Buy:
		lines = []Line{
			{AccountID: f.AccountID, Asset: USD, Amount: n.Add(f.Fee).Neg()},
			{AccountID: Clearing, Asset: USD, Amount: n},
			{AccountID: Clearing, Asset: f.Symbol, Amount: f.Quantity.Neg()},
			{AccountID: f.AccountID, Asset: f.Symbol, Amount: f.Quantity},
		}
	case Sell:
		lines = []Line{
			{AccountID: f.AccountID, Asset: f.Symbol, Amount: f.Quantity.Neg()},
			{AccountID: Clearing, Asset: f.Symbol, Amount: f.Quantity},
			{AccountID: Clearing, Asset: USD, Amount: n.Neg()},
			{AccountID: f.AccountID, Asset: USD, Amount: n.Sub(f.Fee)},
		}
	}
	if f.Fee.IsPositive() {
		lines = append(lines, Line{AccountID: Fees, Asset: USD, Amount: f.Fee})
	}
	// A sell whose fee equals its proceeds has a zero cash line.
	return slices.DeleteFunc(lines, func(l Line) bool { return l.Amount.IsZero() })
}

// holdNeeds returns the ledger account the hold must be on and how much of
// it the fill uses.
func (f Fill) holdNeeds() (accountRef, decimal.Decimal) {
	if f.Side == Buy {
		return accountRef{account: f.AccountID, asset: USD}, f.Notional().Add(f.Fee)
	}
	return accountRef{account: f.AccountID, asset: f.Symbol}, f.Quantity
}

func (f Fill) metadata() map[string]string {
	return map[string]string{
		"fill_id":  f.ID,
		"side":     string(f.Side),
		"symbol":   string(f.Symbol),
		"quantity": f.Quantity.String(),
		"price":    f.Price.String(),
		"fee":      f.Fee.String(),
		"notional": f.Notional().String(),
	}
}

// SettleFill posts f and closes its hold. The part of the hold the fill
// didn't use becomes available again. A fill that needs more than its hold
// fails with ErrHoldExceeded. Settling the same fill twice returns the
// original entry.
func SettleFill(ctx context.Context, tx pgx.Tx, f Fill) (Entry, error) {
	if err := f.validate(); err != nil {
		return Entry{}, err
	}
	if _, err := loadHold(ctx, tx, f.HoldID); err != nil {
		return Entry{}, err
	}
	pending, replayed, err := startEntry(ctx, tx, posting{
		account:  f.AccountID,
		key:      f.ID,
		kind:     KindTradeFill,
		holdID:   f.HoldID,
		metadata: f.metadata(),
		lines:    f.lines(),
	})
	if err != nil || pending == nil {
		return replayed, err
	}

	hold, closed, err := closeHold(ctx, tx, f.HoldID, HoldSettled)
	if err != nil {
		return Entry{}, err
	}
	if !closed {
		return Entry{}, fmt.Errorf("%w: hold %d is %s", ErrHoldClosed, f.HoldID, hold.Status)
	}
	ref, need := f.holdNeeds()
	if hold.ledgerAccountID != pending.accounts[ref] {
		return Entry{}, fmt.Errorf("%w: fill %q needs a hold on %s, hold %d is on %s/%s",
			ErrInvalid, f.ID, ref, f.HoldID, hold.AccountID, hold.Asset)
	}
	if need.GreaterThan(hold.Amount) {
		return Entry{}, fmt.Errorf("%w: fill %q needs %s %s, hold %d reserved %s",
			ErrHoldExceeded, f.ID, need, ref.asset, f.HoldID, hold.Amount)
	}
	return pending.finish(ctx, tx, map[int64]decimal.Decimal{hold.ledgerAccountID: hold.Amount.Neg()})
}
