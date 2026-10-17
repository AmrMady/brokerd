package ledger

import (
	"context"
	"fmt"
	"slices"
	"time"

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

// Fill is an executed trade to be booked against the hold of its order.
// TradeDate and SettleDate come from whoever executed the trade; only their
// dates are used.
type Fill struct {
	ID         string // also the idempotency key
	HoldID     int64
	AccountID  string
	Symbol     Asset
	Side       Side
	Quantity   decimal.Decimal
	Price      decimal.Decimal
	Fee        decimal.Decimal // USD
	TradeDate  time.Time
	SettleDate time.Time
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
	if f.TradeDate.IsZero() || f.SettleDate.IsZero() {
		return fmt.Errorf("%w: trade and settlement dates are required", ErrInvalid)
	}
	if dateOnly(f.SettleDate).Before(dateOnly(f.TradeDate)) {
		return fmt.Errorf("%w: settlement date %s is before trade date %s",
			ErrInvalid, f.SettleDate.Format(time.DateOnly), f.TradeDate.Format(time.DateOnly))
	}
	return nil
}

// proceeds is the cash a sell adds to the account, which stays unsettled
// until the settlement date.
func (f Fill) proceeds() decimal.Decimal {
	if f.Side == Buy {
		return decimal.Zero
	}
	return decimal.Max(f.Notional().Sub(f.Fee), decimal.Zero)
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
		"fill_id":     f.ID,
		"side":        string(f.Side),
		"symbol":      string(f.Symbol),
		"quantity":    f.Quantity.String(),
		"price":       f.Price.String(),
		"fee":         f.Fee.String(),
		"notional":    f.Notional().String(),
		"trade_date":  dateOnly(f.TradeDate).Format(time.DateOnly),
		"settle_date": dateOnly(f.SettleDate).Format(time.DateOnly),
	}
}

// SettleFill posts f and takes what it costs out of its hold. An order can
// fill in several parts, so the hold stays active until fills use all of it
// or ReleaseHold returns the rest. A fill that needs more than what's left of
// the hold fails with ErrHoldExceeded. Booking the same fill twice returns
// the original entry.
//
// Positions change on the trade date. A sell's proceeds can be traded with
// right away but stay unsettled, and can't be withdrawn, until SettleDue
// settles them.
func SettleFill(ctx context.Context, tx pgx.Tx, f Fill) (Entry, error) {
	if err := f.validate(); err != nil {
		return Entry{}, err
	}
	if _, err := loadHold(ctx, tx, f.HoldID); err != nil {
		return Entry{}, err
	}
	status, err := lockAccount(ctx, tx, f.AccountID)
	if err != nil {
		return Entry{}, err
	}
	cashRef := accountRef{account: f.AccountID, asset: USD}
	cash, err := resolveAccounts(ctx, tx, []accountRef{cashRef})
	if err != nil {
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
	if err = requireStatus(f.AccountID, status, AccountActive, AccountRestricted); err != nil {
		return Entry{}, err
	}

	ref, need := f.holdNeeds()
	held := pending.accounts[ref]
	if err = consumeHold(ctx, tx, f.HoldID, held, need); err != nil {
		return Entry{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO settlements (entry_id, ledger_account_id, unsettled_cash, trade_date, settle_date)
		VALUES ($1, $2, $3, $4, $5)`,
		pending.id, cash[cashRef], f.proceeds(), dateOnly(f.TradeDate), dateOnly(f.SettleDate))
	if err != nil {
		return Entry{}, fmt.Errorf("insert settlement: %w", err)
	}

	extra := map[int64]change{held: {held: need.Neg()}}
	if p := f.proceeds(); p.IsPositive() {
		c := extra[cash[cashRef]]
		c.unsettled = p
		extra[cash[cashRef]] = c
	}
	return pending.finish(ctx, tx, extra)
}

func dateOnly(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
