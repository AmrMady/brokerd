package orders_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

type store struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	account string
	hold    int64 // a hold to accept orders with
	entry   int64 // a journal entry to fill orders with
}

func newStore(t *testing.T) store {
	t.Helper()
	s := store{ctx: t.Context(), pool: pgtest.NewPool(t)}
	err := pgx.BeginFunc(s.ctx, s.pool, func(tx pgx.Tx) error {
		if err := ledger.AddAsset(s.ctx, tx, "AAPL", 9); err != nil {
			return err
		}
		a, err := ledger.OpenAccount(s.ctx, tx)
		if err != nil {
			return err
		}
		s.account = a.ID
		tr, err := ledger.RequestDeposit(s.ctx, tx, ledger.CashTransfer{IdempotencyKey: "d1", AccountID: a.ID, Amount: dec("1000")})
		if err != nil {
			return err
		}
		if _, err = ledger.CompleteTransfer(s.ctx, tx, tr.ID); err != nil {
			return err
		}
		h, err := ledger.PlaceHold(s.ctx, tx, ledger.HoldRequest{IdempotencyKey: "h1", AccountID: a.ID, Asset: ledger.USD, Amount: dec("100")})
		if err != nil {
			return err
		}
		s.hold = h.ID
		return tx.QueryRow(s.ctx, `SELECT max(id) FROM journal_entries`).Scan(&s.entry)
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func inTx[T any](s store, fn func(pgx.Tx) (T, error)) (T, error) {
	var out T
	err := pgx.BeginFunc(s.ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		out, err = fn(tx)
		return err
	})
	return out, err
}

func (s store) request() orders.Request {
	return orders.Request{
		AccountID: s.account, Symbol: "AAPL", Side: ledger.Buy, Type: orders.Limit, TimeInForce: orders.GTC,
		Quantity: dec("2"), LimitPrice: limit("10"),
	}
}

func (s store) create(t *testing.T, r orders.Request) orders.Order {
	t.Helper()
	o, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		o, _, err := orders.Create(s.ctx, tx, r)
		return o, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// accepted returns a new accepted order.
func (s store) accepted(t *testing.T) orders.Order {
	t.Helper()
	o := s.create(t, s.request())
	o, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Accept(s.ctx, tx, o.ID, s.hold, time.Time{}, decimal.Zero)
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	r := s.request()
	r.TimeInForce = orders.Day
	o := s.create(t, r)
	if o.Status != orders.New || o.Open() || o.HoldID != 0 {
		t.Fatalf("created %+v, want a new order", o)
	}

	expires := time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)
	o, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Accept(s.ctx, tx, o.ID, s.hold, expires, decimal.Zero)
	})
	noErr(t, err)
	if o.Status != orders.Accepted || !o.Open() || o.HoldID != s.hold || !o.ExpiresAt.Equal(expires) || !o.ClosedAt.IsZero() {
		t.Fatalf("accepted %+v", o)
	}

	o, err = inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Fill(s.ctx, tx, o.ID, dec("9.99"), s.entry)
	})
	noErr(t, err)
	if o.Status != orders.Filled || !o.FillPrice.Decimal.Equal(dec("9.99")) || o.FillEntryID != s.entry || o.ClosedAt.IsZero() {
		t.Fatalf("filled %+v", o)
	}
	got, err := orders.Get(s.ctx, s.pool, o.ID)
	noErr(t, err)
	if got.Status != orders.Filled || got.ID != o.ID {
		t.Errorf("Get = %+v, want the filled order", got)
	}
	for _, id := range []string{"00000000-0000-0000-0000-000000000099", "not-an-id"} {
		if _, err = orders.Get(s.ctx, s.pool, id); !errors.Is(err, orders.ErrNotFound) {
			t.Errorf("Get(%q): %v, want %v", id, err, orders.ErrNotFound)
		}
	}
}

// Every transition the Go API allows is from one state; from any other it
// fails and changes nothing.
func TestIllegalTransitions(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	moves := map[string]func(tx pgx.Tx, id string) (orders.Order, error){
		"accept": func(tx pgx.Tx, id string) (orders.Order, error) {
			return orders.Accept(s.ctx, tx, id, s.hold, time.Time{}, decimal.Zero)
		},
		"reject": func(tx pgx.Tx, id string) (orders.Order, error) { return orders.Reject(s.ctx, tx, id, "no") },
		"fill":   func(tx pgx.Tx, id string) (orders.Order, error) { return orders.Fill(s.ctx, tx, id, dec("1"), s.entry) },
		"cancel": func(tx pgx.Tx, id string) (orders.Order, error) { return orders.Cancel(s.ctx, tx, id, "") },
		"expire": func(tx pgx.Tx, id string) (orders.Order, error) { return orders.Expire(s.ctx, tx, id) },
	}
	legal := map[orders.Status][]string{
		orders.New:      {"accept", "reject"},
		orders.Accepted: {"fill", "cancel", "expire"},
	}
	// An order in each state, reached by legal moves.
	reach := map[orders.Status][]string{
		orders.New: nil, orders.Accepted: {"accept"}, orders.Rejected: {"reject"},
		orders.Filled: {"accept", "fill"}, orders.Canceled: {"accept", "cancel"}, orders.Expired: {"accept", "expire"},
	}
	for status, path := range reach {
		for name, move := range moves {
			o := s.create(t, s.request())
			for _, step := range path {
				_, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) { return moves[step](tx, o.ID) })
				noErr(t, err)
			}
			_, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) { return move(tx, o.ID) })
			allowed := false
			for _, l := range legal[status] {
				allowed = allowed || l == name
			}
			if allowed != (err == nil) {
				t.Errorf("%s from %s: error %v, want allowed %t", name, status, err, allowed)
			}
			if !allowed {
				got, getErr := orders.Get(s.ctx, s.pool, o.ID)
				noErr(t, getErr)
				if got.Status != status {
					t.Errorf("%s from %s left it %s", name, status, got.Status)
				}
			}
		}
	}
}

// Postgres refuses the same changes, made directly.
func TestDatabaseRejectsIllegalChanges(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	accepted := s.accepted(t)
	filled := s.accepted(t)
	_, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) { return orders.Fill(s.ctx, tx, filled.ID, dec("1"), s.entry) })
	noErr(t, err)
	canceled := s.accepted(t)
	_, err = inTx(s, func(tx pgx.Tx) (orders.Order, error) { return orders.Cancel(s.ctx, tx, canceled.ID, "") })
	noErr(t, err)
	exec := func(sql string, args ...any) error {
		return pgx.BeginFunc(s.ctx, s.pool, func(tx pgx.Tx) error {
			_, execErr := tx.Exec(s.ctx, sql, args...)
			return execErr
		})
	}
	for _, tt := range []struct {
		name, sql string
		args      []any
	}{
		{"insert as accepted", `INSERT INTO orders (account_id, symbol, side, type, time_in_force, quantity, status, hold_id)
			VALUES ($1, 'AAPL', 'buy', 'market', 'gtc', 1, 'accepted', $2)`, []any{s.account, s.hold}},
		{"reopen a filled order", `UPDATE orders SET status = 'accepted', fill_price = NULL, fill_entry_id = NULL, closed_at = NULL WHERE id = $1`, []any{filled.ID}},
		{"skip from accepted to rejected", `UPDATE orders SET status = 'rejected', reject_reason = 'x', hold_id = NULL, closed_at = now() WHERE id = $1`, []any{accepted.ID}},
		{"expire a canceled order", `UPDATE orders SET status = 'expired' WHERE id = $1`, []any{canceled.ID}},
		{"change the quantity", `UPDATE orders SET quantity = 5 WHERE id = $1`, []any{accepted.ID}},
		{"change the hold", `UPDATE orders SET hold_id = NULL WHERE id = $1`, []any{accepted.ID}},
		{"change the commission", `UPDATE orders SET commission = 5 WHERE id = $1`, []any{accepted.ID}},
		{"a cancel reason on an open order", `UPDATE orders SET cancel_reason = 'x' WHERE id = $1`, []any{accepted.ID}},
		{"delete", `DELETE FROM orders WHERE id = $1`, []any{accepted.ID}},
		{"filled without a price", `UPDATE orders SET status = 'filled', closed_at = now() WHERE id = $1`, []any{accepted.ID}},
	} {
		if err = exec(tt.sql, tt.args...); err == nil {
			t.Errorf("%s: Postgres allowed it", tt.name)
		}
	}
}

func TestClientOrderID(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	r := s.request()
	r.ClientOrderID = "my-order-1"
	first := s.create(t, r)

	again, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		o, created, createErr := orders.Create(s.ctx, tx, r)
		if created {
			t.Error("a repeated request created a second order")
		}
		return o, createErr
	})
	noErr(t, err)
	if again.ID != first.ID {
		t.Errorf("repeat returned %s, want %s", again.ID, first.ID)
	}

	different := r
	different.Quantity = dec("3")
	_, err = inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		o, _, createErr := orders.Create(s.ctx, tx, different)
		return o, createErr
	})
	if !errors.Is(err, orders.ErrClientOrderIDConflict) {
		t.Errorf("a different request: %v, want %v", err, orders.ErrClientOrderIDConflict)
	}
}

func TestConcurrentClientOrderID(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	r := s.request()
	r.ClientOrderID = "dup"
	ids := make([]string, 20)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	for i := range ids {
		wg.Go(func() {
			o, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
				o, c, createErr := orders.Create(s.ctx, tx, r)
				if c {
					mu.Lock()
					created++
					mu.Unlock()
				}
				return o, createErr
			})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = o.ID
		})
	}
	wg.Wait()
	if created != 1 {
		t.Errorf("%d orders created, want 1", created)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("got different orders %v", ids)
		}
	}
}

func TestCancelRacesWithFill(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	for range 10 {
		o := s.accepted(t)
		var (
			wg   sync.WaitGroup
			errs [2]error
		)
		wg.Go(func() {
			_, errs[0] = inTx(s, func(tx pgx.Tx) (orders.Order, error) { return orders.Fill(s.ctx, tx, o.ID, dec("1"), s.entry) })
		})
		wg.Go(func() {
			_, errs[1] = inTx(s, func(tx pgx.Tx) (orders.Order, error) { return orders.Cancel(s.ctx, tx, o.ID, "") })
		})
		wg.Wait()
		if (errs[0] == nil) == (errs[1] == nil) {
			t.Fatalf("fill: %v, cancel: %v; want exactly one to win", errs[0], errs[1])
		}
		for _, err := range errs {
			if err != nil && !errors.Is(err, orders.ErrNotOpen) {
				t.Errorf("loser got %v, want %v", err, orders.ErrNotOpen)
			}
		}
	}
}

func TestListAndOpenOrders(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	var all []orders.Order
	for i := range 5 {
		if i%2 == 0 {
			all = append(all, s.accepted(t))
		} else {
			all = append(all, s.create(t, s.request()))
		}
	}

	page1, err := orders.List(s.ctx, s.pool, s.account, false, "", 3)
	noErr(t, err)
	page2, err := orders.List(s.ctx, s.pool, s.account, false, page1[len(page1)-1].ID, 3)
	noErr(t, err)
	var got []string
	for _, o := range append(page1, page2...) {
		got = append(got, o.ID)
	}
	want := []string{all[4].ID, all[3].ID, all[2].ID, all[1].ID, all[0].ID}
	if len(got) != len(want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listed %v, want newest first %v", got, want)
		}
	}

	open, err := orders.List(s.ctx, s.pool, s.account, true, "", 10)
	noErr(t, err)
	everyOpen, err := orders.OpenOrders(s.ctx, s.pool)
	noErr(t, err)
	if len(open) != 3 || len(everyOpen) != 3 || everyOpen[0].ID != all[0].ID {
		t.Errorf("open: %d listed, %d in OpenOrders, want 3 oldest first", len(open), len(everyOpen))
	}
	if _, err = orders.List(s.ctx, s.pool, s.account, false, "", orders.MaxPageSize+1); !errors.Is(err, orders.ErrInvalid) {
		t.Errorf("oversized page: %v", err)
	}
}

func TestExpiring(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	closeAt := time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)
	r := s.request()
	r.TimeInForce = orders.Day
	day := s.create(t, r)
	_, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Accept(s.ctx, tx, day.ID, s.hold, closeAt, decimal.Zero)
	})
	noErr(t, err)
	s.accepted(t) // GTC never expires

	for _, tt := range []struct {
		now  time.Time
		want int
	}{{closeAt.Add(-time.Second), 0}, {closeAt, 1}, {closeAt.Add(time.Hour), 1}} {
		list, listErr := orders.Expiring(s.ctx, s.pool, tt.now, orders.Order{}, 10)
		noErr(t, listErr)
		if len(list) != tt.want {
			t.Errorf("Expiring(%s) = %d orders, want %d", tt.now, len(list), tt.want)
		}
	}
	accepted, err := orders.Get(s.ctx, s.pool, day.ID)
	noErr(t, err)
	if list, listErr := orders.Expiring(s.ctx, s.pool, closeAt, accepted, 10); listErr != nil || len(list) != 0 {
		t.Errorf("Expiring after the last one = %d orders, %v; want none", len(list), listErr)
	}
}

func TestDayOrdersExpire(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	r := s.request()
	r.TimeInForce = orders.Day
	day := s.create(t, r)
	if _, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Accept(s.ctx, tx, day.ID, s.hold, time.Time{}, decimal.Zero)
	}); err == nil {
		t.Error("accepted a day order with no expiry")
	}
	gtc := s.create(t, s.request())
	if _, err := inTx(s, func(tx pgx.Tx) (orders.Order, error) {
		return orders.Accept(s.ctx, tx, gtc.ID, s.hold, time.Now(), decimal.Zero)
	}); err == nil {
		t.Error("accepted a GTC order with an expiry")
	}
}
