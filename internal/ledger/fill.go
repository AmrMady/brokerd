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
	// CoverShortfall is for a fill the customer can't be refused anymore,
	// like one of an order accepted earlier. A sale whose fees are more than
	// it brings in is then booked even when the account can't pay the
	// difference. Without it, such a fill fails with ErrInsufficientFunds.
	CoverShortfall bool
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

// netProceeds is what a sell adds to the account's cash after its fees. It's
// negative if the fees are more than the sale.
func (f Fill) netProceeds(r regulatoryFees) decimal.Decimal {
	return f.Notional().Sub(f.Fee).Sub(r.total())
}

// unsettledCash is the cash a fill adds that stays unsettled until the
// settlement date.
func (f Fill) unsettledCash(r regulatoryFees) decimal.Decimal {
	if f.Side == Buy {
		return decimal.Zero
	}
	return decimal.Max(f.netProceeds(r), decimal.Zero)
}

func (f Fill) lines(r regulatoryFees) []Line {
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
			{AccountID: f.AccountID, Asset: USD, Amount: f.netProceeds(r)},
		}
	}
	lines = append(lines,
		Line{AccountID: Fees, Asset: USD, Amount: f.Fee},
		Line{AccountID: SECFeePayable, Asset: USD, Amount: r.sec},
		Line{AccountID: TAFPayable, Asset: USD, Amount: r.taf},
	)
	// Fees can be zero, and a sell whose fees equal its proceeds has a zero
	// cash line.
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
// settles them. Sells also pay the SEC fee and TAF at the rates in effect on
// the trade date, and fail with ErrNoFeeRate if there is none.
//
// A sale whose fees are more than it brings in takes the difference from
// the account's cash, and fails with ErrInsufficientFunds if there isn't
// enough. With CoverShortfall it's booked anyway: it takes what cash the
// account has, the deficits account covers the rest, and the account is
// restricted.
func SettleFill(ctx context.Context, tx pgx.Tx, f Fill) (Entry, error) {
	if err := f.validate(); err != nil {
		return Entry{}, err
	}
	if _, err := loadHold(ctx, tx, f.HoldID); err != nil {
		return Entry{}, err
	}
	fees, err := feesFor(ctx, tx, f)
	if err != nil {
		return Entry{}, err
	}
	// Covering a shortfall reads what cash the account has, which needs the
	// account to itself.
	owes := f.CoverShortfall && f.Side == Sell && f.netProceeds(fees).IsNegative()
	var account Account
	if owes {
		account, err = lockAccountForUpdate(ctx, tx, f.AccountID)
	} else {
		account.Status, err = lockAccount(ctx, tx, f.AccountID)
	}
	if err != nil {
		return Entry{}, err
	}
	cashRef := accountRef{account: f.AccountID, asset: USD}
	cash, err := resolveAccounts(ctx, tx, []accountRef{cashRef})
	if err != nil {
		return Entry{}, err
	}
	p := posting{
		account:  f.AccountID,
		op:       opFill,
		key:      f.ID,
		kind:     KindTradeFill,
		holdID:   f.HoldID,
		metadata: f.metadata(),
		lines:    f.lines(regulatoryFees{}),
	}
	// The idempotency key covers the fill as it was requested. Its regulatory
	// fees depend on the rate schedule, which can change before a retry.
	p.request = p.hash()
	p.lines = f.lines(fees)
	if f.Side == Sell {
		p.metadata["sec_fee_rate"] = fees.secRate.String()
		p.metadata["taf_rate"] = fees.tafRate.String()
	}
	pending, replayed, err := startEntry(ctx, tx, p)
	if err != nil || pending == nil {
		return replayed, err
	}
	if err = requireStatus(f.AccountID, account.Status, AccountActive, AccountRestricted); err != nil {
		return Entry{}, err
	}
	if owes {
		if err = coverShortfall(ctx, tx, p.lines, account, "fees more than the sale: "+f.ID); err != nil {
			return Entry{}, err
		}
	}

	ref, need := f.holdNeeds()
	held := pending.accounts[ref]
	if err = consumeHold(ctx, tx, f.HoldID, held, need); err != nil {
		return Entry{}, err
	}
	position := pending.accounts[accountRef{account: f.AccountID, asset: f.Symbol}]
	if f.Side == Buy {
		err = openLot(ctx, tx, position, pending.id, f)
	} else {
		err = sellLots(ctx, tx, position, pending.id, f, f.netProceeds(fees))
	}
	if err != nil {
		return Entry{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO settlements (entry_id, ledger_account_id, unsettled_cash, trade_date, settle_date)
		VALUES ($1, $2, $3, $4, $5)`,
		pending.id, cash[cashRef], f.unsettledCash(fees), dateOnly(f.TradeDate), dateOnly(f.SettleDate))
	if err != nil {
		return Entry{}, fmt.Errorf("insert settlement: %w", err)
	}

	extra := map[int64]change{held: {held: need.Neg()}}
	if u := f.unsettledCash(fees); u.IsPositive() {
		c := extra[cash[cashRef]]
		c.unsettled = u
		extra[cash[cashRef]] = c
	}
	return pending.finish(ctx, tx, extra)
}

func dateOnly(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
