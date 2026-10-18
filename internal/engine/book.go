package engine

import (
	"slices"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
)

// book is a symbol's resting limit orders, each side best price first and,
// at the same price, oldest first.
type book struct {
	buys, sells []resting
}

type resting struct {
	id      string
	limit   decimal.Decimal
	created time.Time
}

// candidate is a resting order the quote crosses, and the price it fills at.
type candidate struct {
	id    string
	price decimal.Decimal
}

func (b *book) side(s ledger.Side) *[]resting {
	if s == ledger.Buy {
		return &b.buys
	}
	return &b.sells
}

// add puts o in the book, unless it's there already.
func (b *book) add(o orders.Order) {
	side := b.side(o.Side)
	if slices.ContainsFunc(*side, func(r resting) bool { return r.id == o.ID }) {
		return
	}
	r := resting{id: o.ID, limit: o.LimitPrice.Decimal, created: o.CreatedAt}
	i, _ := slices.BinarySearchFunc(*side, r, func(a, b resting) int {
		if c := a.limit.Cmp(b.limit); c != 0 {
			if o.Side == ledger.Buy {
				return -c
			}
			return c
		}
		return a.created.Compare(b.created)
	})
	*side = slices.Insert(*side, i, r)
}

func (b *book) remove(id string) {
	for _, side := range []*[]resting{&b.buys, &b.sells} {
		*side = slices.DeleteFunc(*side, func(r resting) bool { return r.id == id })
	}
}

func (b *book) empty() bool {
	return len(b.buys) == 0 && len(b.sells) == 0
}

// crossing returns the orders q crosses: buys limited at or above the ask,
// which fill at the ask, and sells limited at or below the bid, which fill
// at the bid.
func (b *book) crossing(q marketdata.Quote) []candidate {
	var out []candidate
	if q.Ask.IsPositive() {
		for _, r := range b.buys {
			if r.limit.LessThan(q.Ask) {
				break
			}
			out = append(out, candidate{r.id, q.Ask})
		}
	}
	if q.Bid.IsPositive() {
		for _, r := range b.sells {
			if r.limit.GreaterThan(q.Bid) {
				break
			}
			out = append(out, candidate{r.id, q.Bid})
		}
	}
	return out
}
