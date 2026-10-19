package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// RegulatoryFee is a fee charged on sells that the firm owes a regulator.
type RegulatoryFee string

// Regulatory fees.
const (
	SECFee RegulatoryFee = "sec" // SEC Section 31 fee, per dollar sold
	TAF    RegulatoryFee = "taf" // FINRA trading activity fee, per share sold, capped per trade
)

// FeeRate is the rate of a regulatory fee from EffectiveOn until the next
// rate for the same fee takes over. For the SEC fee, $27.80 per million is a
// Rate of 0.0000278.
type FeeRate struct {
	Fee         RegulatoryFee
	EffectiveOn time.Time // only the date is used
	Rate        decimal.Decimal
	MaxPerTrade decimal.NullDecimal // TAF only
}

func (r FeeRate) validate() error {
	switch r.Fee {
	case SECFee:
		if r.MaxPerTrade.Valid {
			return fmt.Errorf("%w: the SEC fee has no maximum per trade", ErrInvalid)
		}
	case TAF:
		if !r.MaxPerTrade.Valid {
			return fmt.Errorf("%w: TAF needs a maximum per trade", ErrInvalid)
		}
		if r.MaxPerTrade.Decimal.IsNegative() {
			return fmt.Errorf("%w: maximum per trade must not be negative, got %s", ErrInvalid, r.MaxPerTrade.Decimal)
		}
		if err := checkDecimal("maximum per trade", r.MaxPerTrade.Decimal, usdScale); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown regulatory fee %q", ErrInvalid, r.Fee)
	}
	if r.EffectiveOn.IsZero() {
		return fmt.Errorf("%w: effective date is required", ErrInvalid)
	}
	if r.Rate.IsNegative() {
		return fmt.Errorf("%w: rate must not be negative, got %s", ErrInvalid, r.Rate)
	}
	return checkDecimal("rate", r.Rate, maxScale)
}

// AddFeeRate adds a rate to a fee's schedule. Adding the same rate again
// changes nothing; a different rate for the same fee and date is an error.
// Entries record the rates they were charged, so a new rate never changes
// what was already booked.
func AddFeeRate(ctx context.Context, tx pgx.Tx, r FeeRate) error {
	if err := r.validate(); err != nil {
		return err
	}
	day := dateOnly(r.EffectiveOn)
	tag, err := tx.Exec(ctx, `
		INSERT INTO fee_rates (fee, effective_on, rate, max_per_trade)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (fee, effective_on) DO NOTHING`,
		r.Fee, day, r.Rate, r.MaxPerTrade)
	if err != nil {
		return fmt.Errorf("add %s rate: %w", r.Fee, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var existing FeeRate
	err = tx.QueryRow(ctx, `
		SELECT rate, max_per_trade FROM fee_rates WHERE fee = $1 AND effective_on = $2`,
		r.Fee, day).Scan(&existing.Rate, &existing.MaxPerTrade)
	if err != nil {
		return fmt.Errorf("load %s rate: %w", r.Fee, err)
	}
	if !existing.Rate.Equal(r.Rate) || existing.MaxPerTrade.Valid != r.MaxPerTrade.Valid ||
		!existing.MaxPerTrade.Decimal.Equal(r.MaxPerTrade.Decimal) {
		return fmt.Errorf("%w: a different %s rate is already set from %s", ErrInvalid, r.Fee, day.Format(time.DateOnly))
	}
	return nil
}

// regulatoryFees is what a sell owes the SEC and FINRA, and the rates used.
type regulatoryFees struct {
	sec, taf         decimal.Decimal
	secRate, tafRate decimal.Decimal
}

func (r regulatoryFees) total() decimal.Decimal {
	return r.sec.Add(r.taf)
}

// SaleFees returns the SEC fee and TAF that selling quantity at price on
// tradeDate pays, at the rates in effect then, or ErrNoFeeRate.
func SaleFees(ctx context.Context, q Querier, quantity, price decimal.Decimal, tradeDate time.Time) (decimal.Decimal, error) {
	r, err := feesFor(ctx, q, Fill{Side: Sell, Quantity: quantity, Price: price, TradeDate: tradeDate})
	return r.total(), err
}

// feesFor returns the regulatory fees on f at the rates in effect on its
// trade date. Only sells pay them. Each is rounded up to the cent, and TAF
// is capped per trade.
func feesFor(ctx context.Context, q Querier, f Fill) (regulatoryFees, error) {
	var r regulatoryFees
	if f.Side == Buy {
		return r, nil
	}
	day := dateOnly(f.TradeDate)
	rows, err := q.Query(ctx, `
		SELECT DISTINCT ON (fee) fee, rate, max_per_trade
		FROM fee_rates
		WHERE effective_on <= $1
		ORDER BY fee, effective_on DESC`, day)
	if err != nil {
		return r, fmt.Errorf("load fee rates: %w", err)
	}
	var (
		fee         RegulatoryFee
		rate        decimal.Decimal
		maxPerTrade decimal.NullDecimal
		found       = map[RegulatoryFee]bool{}
	)
	_, err = pgx.ForEachRow(rows, []any{&fee, &rate, &maxPerTrade}, func() error {
		switch fee {
		case SECFee:
			r.secRate, r.sec = rate, f.Notional().Mul(rate).RoundCeil(usdScale)
		case TAF:
			r.tafRate, r.taf = rate, decimal.Min(f.Quantity.Mul(rate).RoundCeil(usdScale), maxPerTrade.Decimal)
		}
		found[fee] = true
		return nil
	})
	if err != nil {
		return r, fmt.Errorf("load fee rates: %w", err)
	}
	for _, want := range []RegulatoryFee{SECFee, TAF} {
		if !found[want] {
			return r, fmt.Errorf("%w: %s on %s", ErrNoFeeRate, want, day.Format(time.DateOnly))
		}
	}
	return r, nil
}
