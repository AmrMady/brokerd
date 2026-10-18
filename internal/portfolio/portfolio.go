// Package portfolio reports an account's positions: what it holds, what it
// cost, and what it's worth at the latest quotes.
package portfolio

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
)

// Quotes gives the latest quote for a symbol.
type Quotes interface {
	Quote(symbol string) (marketdata.Quote, bool)
}

// Position is the shares of one symbol an account holds. The cost fields
// are null when part of the position's cost is unknown, and the market
// fields when there's no bid to value it at.
type Position struct {
	Symbol      ledger.Asset
	Quantity    decimal.Decimal
	Available   decimal.Decimal // not held by open sell orders
	CostBasis   decimal.NullDecimal
	AverageCost decimal.NullDecimal // per share, to four decimal places
	Price       decimal.NullDecimal // the latest bid
	MarketValue decimal.NullDecimal
	Unrealized  decimal.NullDecimal // gain or loss: market value less cost basis
}

// Positions returns an account's positions, by symbol. Balances and lots
// are read from one snapshot, so they always agree.
func Positions(ctx context.Context, pool *pgxpool.Pool, quotes Quotes, accountID string) ([]Position, error) {
	var positions []Position
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
			if _, err := ledger.AccountByID(ctx, tx, accountID); err != nil {
				return err
			}
			balances, err := ledger.Balances(ctx, tx, accountID)
			if err != nil {
				return err
			}
			for _, b := range balances {
				if b.Asset == ledger.USD || !b.Balance.IsPositive() {
					continue
				}
				var p Position
				if p, err = position(ctx, tx, b); err != nil {
					return err
				}
				positions = append(positions, value(p, quotes))
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("positions of %s: %w", accountID, err)
	}
	return positions, nil
}

func position(ctx context.Context, tx pgx.Tx, b ledger.Balance) (Position, error) {
	p := Position{Symbol: b.Asset, Quantity: b.Balance, Available: b.Balance.Sub(b.Held)}
	lots, err := ledger.OpenLots(ctx, tx, b.AccountID, b.Asset)
	if err != nil {
		return p, err
	}
	basis := decimal.Zero
	for _, l := range lots {
		if !l.RemainingCost.Valid {
			return p, nil
		}
		basis = basis.Add(l.RemainingCost.Decimal)
	}
	p.CostBasis = decimal.NewNullDecimal(basis)
	p.AverageCost = decimal.NewNullDecimal(basis.DivRound(p.Quantity, 4))
	return p, nil
}

func value(p Position, quotes Quotes) Position {
	q, ok := quotes.Quote(string(p.Symbol))
	if !ok || !q.Bid.IsPositive() {
		return p
	}
	p.Price = decimal.NewNullDecimal(q.Bid)
	worth := p.Quantity.Mul(q.Bid).RoundBank(2)
	p.MarketValue = decimal.NewNullDecimal(worth)
	if p.CostBasis.Valid {
		p.Unrealized = decimal.NewNullDecimal(worth.Sub(p.CostBasis.Decimal))
	}
	return p
}
