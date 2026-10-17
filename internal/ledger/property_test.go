package ledger_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// Random sequences of operations, checked against an in-memory model.
// Rerun a failing seed with -run 'TestRandomOperationsKeepLedgerConsistent/seed=N'.
func TestRandomOperationsKeepLedgerConsistent(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			l := newTestLedger(t)
			s := &simulation{
				t:      t,
				l:      l,
				rng:    rand.New(rand.NewPCG(seed, 0)),
				owners: []string{l.openAccount(t), l.openAccount(t), l.openAccount(t)},
				model:  map[modelKey]*modelBalance{},
			}
			for s.step = range 200 {
				s.randomOperation()
			}
			s.compareBalances()
			s.l.requireReconciled(t)
		})
	}
}

var simSymbols = []ledger.Asset{"AAPL", "MSFT"}

type modelKey struct {
	owner string
	asset ledger.Asset
}

type modelBalance struct {
	balance, held decimal.Decimal
}

func (m *modelBalance) available() decimal.Decimal {
	return m.balance.Sub(m.held)
}

type simulation struct {
	t      *testing.T
	l      testLedger
	rng    *rand.Rand
	step   int
	seq    int // makes idempotency keys unique
	owners []string
	model  map[modelKey]*modelBalance
	holds  []ledger.Hold // active holds
	done   []doneRequest
}

// doneRequest is a successful request, kept to test replays and key reuse.
type doneRequest struct {
	name     string
	id       int64
	replay   func() (int64, error)
	conflict func() error // nil for requests without an idempotency key
}

func (s *simulation) randomOperation() {
	switch n := s.rng.IntN(100); {
	case n < 20:
		s.deposit()
	case n < 35:
		s.withdraw()
	case n < 50:
		s.holdCash()
	case n < 60:
		s.holdShares()
	case n < 70:
		s.release()
	case n < 90:
		s.settle()
	case n < 97:
		s.replay()
	default:
		s.reuseKey()
	}
}

func (s *simulation) account(owner string, asset ledger.Asset) *modelBalance {
	k := modelKey{owner, asset}
	if s.model[k] == nil {
		s.model[k] = &modelBalance{}
	}
	return s.model[k]
}

func (s *simulation) key(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

func (s *simulation) cents(lo, hi int64) decimal.Decimal {
	return decimal.New(lo+s.rng.Int64N(hi-lo+1), -2)
}

func (s *simulation) portion(limit decimal.Decimal) decimal.Decimal {
	units := limit.Shift(9).IntPart()
	if units < 1 {
		return decimal.New(1, -9)
	}
	return decimal.New(1+s.rng.Int64N(units), -9)
}

func pick[T any](rng *rand.Rand, xs []T) T {
	return xs[rng.IntN(len(xs))]
}

func entryID(e ledger.Entry, err error) (int64, error)       { return e.ID, err }
func transferID(t ledger.Transfer, err error) (int64, error) { return t.ID, err }
func holdID(h ledger.Hold, err error) (int64, error)         { return h.ID, err }
func errOf[T any](_ T, err error) error                      { return err }

// expect checks err against want (nil means success) and reports whether the operation succeeded.
func (s *simulation) expect(op string, err, want error) bool {
	s.t.Helper()
	if want == nil && err != nil {
		s.t.Fatalf("step %d: %s: unexpected error: %v", s.step, op, err)
	}
	if want != nil && !errors.Is(err, want) {
		s.t.Fatalf("step %d: %s: got %v, want %v", s.step, op, err, want)
	}
	return err == nil
}

func (s *simulation) remember(r doneRequest) {
	s.done = append(s.done, r)
}

func (s *simulation) deposit() {
	owner, amount, key := pick(s.rng, s.owners), s.cents(1, 500_000), s.key("deposit")
	op := fmt.Sprintf("deposit %s to %s", amount, owner)
	e, err := s.l.deposit(key, owner, amount.String())
	if !s.expect(op, err, nil) {
		return
	}
	cash := s.account(owner, ledger.USD)
	cash.balance = cash.balance.Add(amount)
	s.remember(doneRequest{
		name: op,
		id:   e.ID,
		replay: func() (int64, error) {
			return transferID(s.l.deposit(key, owner, amount.String()))
		},
		conflict: func() error {
			return errOf(s.l.deposit(key, owner, amount.Mul(decimal.NewFromInt(2)).String()))
		},
	})
}

func (s *simulation) withdraw() {
	owner, amount, key := pick(s.rng, s.owners), s.cents(1, 300_000), s.key("withdraw")
	op := fmt.Sprintf("withdraw %s from %s", amount, owner)
	cash := s.account(owner, ledger.USD)
	var want error
	if cash.available().LessThan(amount) {
		want = ledger.ErrInsufficientFunds
	}
	e, err := s.l.withdraw(key, owner, amount.String())
	if !s.expect(op, err, want) {
		return
	}
	cash.balance = cash.balance.Sub(amount)
	s.remember(doneRequest{
		name: op,
		id:   e.ID,
		replay: func() (int64, error) {
			return transferID(s.l.withdraw(key, owner, amount.String()))
		},
		conflict: func() error {
			return errOf(s.l.withdraw(key, owner, amount.Mul(decimal.NewFromInt(2)).String()))
		},
	})
}

func (s *simulation) holdCash() {
	s.hold(pick(s.rng, s.owners), ledger.USD, s.cents(100, 200_000))
}

func (s *simulation) holdShares() {
	s.hold(pick(s.rng, s.owners), pick(s.rng, simSymbols), s.portion(decimal.NewFromInt(3)))
}

func (s *simulation) hold(owner string, asset ledger.Asset, amount decimal.Decimal) {
	key := s.key("hold")
	op := fmt.Sprintf("hold %s %s for %s", amount, asset, owner)
	acct := s.account(owner, asset)
	var want error
	if acct.available().LessThan(amount) {
		want = ledger.ErrInsufficientFunds
	}
	h, err := s.l.placeHold(key, owner, asset, amount.String())
	if !s.expect(op, err, want) {
		return
	}
	acct.held = acct.held.Add(amount)
	s.holds = append(s.holds, h)
	s.remember(doneRequest{
		name: op,
		id:   h.ID,
		replay: func() (int64, error) {
			return holdID(s.l.placeHold(key, owner, asset, amount.String()))
		},
		conflict: func() error {
			return errOf(s.l.placeHold(key, owner, asset, amount.Mul(decimal.NewFromInt(2)).String()))
		},
	})
}

func (s *simulation) release() {
	if len(s.holds) == 0 {
		return
	}
	i := s.rng.IntN(len(s.holds))
	h := s.holds[i]
	op := fmt.Sprintf("release hold %d", h.ID)
	_, err := s.l.releaseHold(h.ID)
	if !s.expect(op, err, nil) {
		return
	}
	acct := s.account(h.AccountID, h.Asset)
	acct.held = acct.held.Sub(h.Remaining)
	s.holds = append(s.holds[:i], s.holds[i+1:]...)
	s.remember(doneRequest{
		name: op,
		id:   h.ID,
		replay: func() (int64, error) {
			return holdID(s.l.releaseHold(h.ID))
		},
	})
}

func (s *simulation) settle() {
	if len(s.holds) == 0 {
		return
	}
	i := s.rng.IntN(len(s.holds))
	h := s.holds[i]
	f := ledger.Fill{ID: s.key("fill"), HoldID: h.ID, AccountID: h.AccountID, Price: s.cents(100, 50_000)}
	exceed := s.rng.IntN(10) == 0
	if h.Asset == ledger.USD {
		f.Side, f.Symbol = ledger.Buy, pick(s.rng, simSymbols)
		f.Fee = decimal.Min(s.cents(0, 500), h.Remaining)
		maxQty := h.Remaining.Sub(f.Fee).Div(f.Price).Truncate(9)
		f.Quantity = s.portion(maxQty)
		if exceed {
			f.Quantity = maxQty.Add(decimal.NewFromInt(1))
		}
	} else {
		f.Side, f.Symbol = ledger.Sell, h.Asset
		f.Fee = s.cents(0, 200)
		f.Quantity = s.portion(h.Remaining)
		if exceed {
			f.Quantity = h.Remaining.Add(decimal.New(1, -9))
		}
	}
	op := fmt.Sprintf("%s %s %s at %s, fee %s, against hold %d", f.Side, f.Quantity, f.Symbol, f.Price, f.Fee, h.ID)

	notional := f.Notional()
	cash := s.account(f.AccountID, ledger.USD)
	shares := s.account(f.AccountID, f.Symbol)
	need := f.Quantity
	if f.Side == ledger.Buy {
		need = notional.Add(f.Fee)
	}
	var want error
	switch {
	case notional.IsZero():
		want = ledger.ErrInvalid
	case need.GreaterThan(h.Remaining):
		want = ledger.ErrHoldExceeded
	case f.Side == ledger.Sell && cash.available().Add(notional).Sub(f.Fee).IsNegative():
		want = ledger.ErrInsufficientFunds
	}

	e, err := s.l.settle(f)
	if !s.expect(op, err, want) {
		return
	}
	if f.Side == ledger.Buy {
		cash.balance = cash.balance.Sub(need)
		cash.held = cash.held.Sub(need)
		shares.balance = shares.balance.Add(f.Quantity)
	} else {
		shares.balance = shares.balance.Sub(f.Quantity)
		shares.held = shares.held.Sub(need)
		cash.balance = cash.balance.Add(notional).Sub(f.Fee)
	}
	// A fill that uses up the hold settles it; otherwise more fills can follow.
	if s.holds[i].Remaining = h.Remaining.Sub(need); s.holds[i].Remaining.IsZero() {
		s.holds = append(s.holds[:i], s.holds[i+1:]...)
	}
	s.remember(doneRequest{
		name: op,
		id:   e.ID,
		replay: func() (int64, error) {
			return entryID(s.l.settle(f))
		},
		conflict: func() error {
			g := f
			g.Price = f.Price.Mul(decimal.NewFromInt(2))
			return errOf(s.l.settle(g))
		},
	})
}

func (s *simulation) replay() {
	if len(s.done) == 0 {
		return
	}
	r := pick(s.rng, s.done)
	id, err := r.replay()
	if s.expect("replay "+r.name, err, nil) && id != r.id {
		s.t.Fatalf("step %d: replay %s returned id %d, want %d", s.step, r.name, id, r.id)
	}
}

func (s *simulation) reuseKey() {
	var keyed []doneRequest
	for _, r := range s.done {
		if r.conflict != nil {
			keyed = append(keyed, r)
		}
	}
	if len(keyed) == 0 {
		return
	}
	r := pick(s.rng, keyed)
	s.expect("reuse the key of "+r.name, r.conflict(), ledger.ErrIdempotencyConflict)
}

func (s *simulation) compareBalances() {
	s.t.Helper()
	for _, owner := range s.owners {
		for _, asset := range append([]ledger.Asset{ledger.USD}, simSymbols...) {
			got := s.l.balance(s.t, owner, asset)
			want := s.account(owner, asset)
			if !got.Balance.Equal(want.balance) || !got.Held.Equal(want.held) {
				s.t.Errorf("%s/%s: balance %s held %s, model says balance %s held %s",
					owner, asset, got.Balance, got.Held, want.balance, want.held)
			}
		}
	}
}
