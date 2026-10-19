package engine_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
)

// run runs e until the test ends.
func run(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v", err)
		}
	})
}

// tick sends a quote through the pipeline: the cache first, then the engine.
func (h *harness) tick(symbol, bid, ask string, engines ...*engine.Engine) {
	h.quote(symbol, bid, ask)
	if len(engines) == 0 {
		engines = []*engine.Engine{h.engine}
	}
	for _, e := range engines {
		e.HandleQuote(marketdata.Quote{Symbol: symbol})
	}
}

func (h *harness) waitFor(t *testing.T, account, id string, status orders.Status) orders.Order {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		o, err := h.engine.Order(h.ctx, account, id)
		noErr(t, err)
		if o.Status == status {
			return o
		}
		if time.Now().After(deadline) {
			t.Fatalf("order is %s, want %s", o.Status, status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRestingOrdersFillWhenCrossed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	run(t, h.engine)
	alice, bob := h.account(t, "10000"), h.account(t, "10000")
	h.tick("AAPL", "100.00", "100.10")
	h.place(t, market(bob, ledger.Buy, "5"))

	buy := h.place(t, limit(alice, ledger.Buy, "10", "99.50"))
	sell := h.place(t, limit(bob, ledger.Sell, "5", "101"))
	wantStatus(t, buy, orders.Accepted, "")
	wantStatus(t, sell, orders.Accepted, "")

	h.tick("AAPL", "99.00", "99.40")
	buy = h.waitFor(t, alice, buy.ID, orders.Filled)
	if !buy.FillPrice.Decimal.Equal(dec("99.40")) {
		t.Errorf("buy limited at 99.50 filled at %s, want the ask", buy.FillPrice.Decimal)
	}
	h.wantBalance(t, alice, ledger.USD, "9006", "0") // 10000 - 994, the rest of the hold released
	h.wantBalance(t, bob, "AAPL", "5", "5")

	h.tick("AAPL", "101.20", "101.30")
	sell = h.waitFor(t, bob, sell.ID, orders.Filled)
	if !sell.FillPrice.Decimal.Equal(dec("101.20")) {
		t.Errorf("sell limited at 101 filled at %s, want the bid", sell.FillPrice.Decimal)
	}
	h.wantBalance(t, bob, "AAPL", "0", "0")
	h.requireReconciled(t)
}

func TestMarketableLimitOrdersFillAtOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	h.quote("AAPL", "100.00", "100.10")
	o := h.place(t, limit(alice, ledger.Buy, "10", "101"))
	wantStatus(t, o, orders.Filled, "")
	if !o.FillPrice.Decimal.Equal(dec("100.10")) {
		t.Errorf("filled at %s, want the ask", o.FillPrice.Decimal)
	}
	h.wantBalance(t, alice, ledger.USD, "8999", "0")
}

// Neither a stale quote nor another symbol's quote fills an order.
func TestOnlyFreshQuotesOfTheSymbolFill(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	run(t, h.engine)
	alice := h.account(t, "10000")
	o := h.place(t, limit(alice, ledger.Buy, "1", "100"))

	h.tick("MSFT", "1", "1")
	h.quotes.HandleQuote(marketdata.Quote{Symbol: "AAPL", Bid: dec("98"), Ask: dec("99"), Timestamp: friday.Add(-2 * time.Minute)})
	h.engine.HandleQuote(marketdata.Quote{Symbol: "AAPL"})
	time.Sleep(50 * time.Millisecond)
	wantStatus(t, h.waitFor(t, alice, o.ID, orders.Accepted), orders.Accepted, "")

	h.tick("AAPL", "98", "99")
	h.waitFor(t, alice, o.ID, orders.Filled)
}

// Cancels racing with fills leave each order filled or canceled, never
// both, with every hold closed.
func TestCancelsRaceWithMatching(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	run(t, h.engine)
	alice := h.account(t, "100000")
	var ids []string
	for range 30 {
		ids = append(ids, h.place(t, limit(alice, ledger.Buy, "1", "99")).ID)
	}

	var wg sync.WaitGroup
	wg.Go(func() { h.tick("AAPL", "97", "98") })
	for _, id := range ids {
		wg.Go(func() {
			if _, err := h.engine.Cancel(h.ctx, alice, id); err != nil && !errors.Is(err, orders.ErrNotOpen) {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	filled := 0
	for _, id := range ids {
		o, err := h.engine.Order(h.ctx, alice, id)
		noErr(t, err)
		switch o.Status {
		case orders.Filled:
			filled++
		case orders.Canceled:
		default:
			// A fill can still be in flight after its cancel lost.
			h.waitFor(t, alice, id, orders.Filled)
			filled++
		}
	}
	h.wantBalance(t, alice, "AAPL", strconv.Itoa(filled), "0")
	h.wantBalance(t, alice, ledger.USD, strconv.Itoa(100000-98*filled), "0")
	h.requireReconciled(t)
}

func TestDayOrdersExpire(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{ExpiryInterval: 10 * time.Millisecond})
	run(t, h.engine)
	alice := h.account(t, "1000")
	day := limit(alice, ledger.Buy, "1", "10")
	day.TimeInForce = orders.Day
	o := h.place(t, day)
	gtc := h.place(t, limit(alice, ledger.Buy, "1", "10"))

	h.setNow(time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)) // Friday's close
	h.waitFor(t, alice, o.ID, orders.Expired)
	h.wantBalance(t, alice, ledger.USD, "1000", "10") // only the GTC order still holds
	wantStatus(t, h.waitFor(t, alice, gtc.ID, orders.Accepted), orders.Accepted, "")
}

// A day order still open after its close, because expiry hasn't got to it
// yet, expires instead of filling in a later session.
func TestDayOrdersDontFillAfterTheirClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	day := limit(alice, ledger.Buy, "1", "99")
	day.TimeInForce = orders.Day
	o := h.place(t, day)

	h.setNow(time.Date(2026, 10, 19, 14, 0, 0, 0, time.UTC)) // Monday, 10:00 in New York
	h.quote("AAPL", "97", "98")
	h.engine.MatchForTest(h.ctx, "AAPL")
	got, err := h.engine.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Expired, "")
	h.wantBalance(t, alice, ledger.USD, "1000", "0")
	h.requireReconciled(t)
}

// An order that fails to expire doesn't keep the orders behind it from
// expiring.
func TestExpirySkipsOrdersThatFail(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{ExpiryInterval: 10 * time.Millisecond})
	alice := h.account(t, "1000")
	day := limit(alice, ledger.Buy, "1", "10")
	day.TimeInForce = orders.Day
	h.setNow(friday.AddDate(0, 0, -1))
	stuck := h.place(t, day) // expires first, on Thursday
	h.setNow(friday)
	var rest []string
	for range 3 {
		rest = append(rest, h.place(t, day).ID)
	}
	h.failChange(t, stuck.ID, orders.Expired, false)

	h.setNow(time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)) // Friday's close
	run(t, h.engine)
	for _, id := range rest {
		h.waitFor(t, alice, id, orders.Expired)
	}
	wantStatus(t, h.waitFor(t, alice, stuck.ID, orders.Accepted), orders.Accepted, "")
	if n := h.engine.Stats().ExpiryErrors; n == 0 {
		t.Errorf("%d expiry errors counted, want some", n)
	}
}

// A fill whose transaction fails to commit leaves its order in the book, so
// a later quote can fill it.
func TestFailedCommitKeepsTheOrderBooked(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	o := h.place(t, limit(alice, ledger.Buy, "1", "99"))
	h.failChange(t, o.ID, orders.Filled, true)
	h.quote("AAPL", "97", "98")
	h.engine.MatchForTest(h.ctx, "AAPL")
	got, err := h.engine.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Accepted, "")

	_, err = h.pool.Exec(h.ctx, `DROP TRIGGER fail_change ON orders`)
	noErr(t, err)
	h.engine.MatchForTest(h.ctx, "AAPL")
	got, err = h.engine.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Filled, "")
	h.requireReconciled(t)
}

// A resting order whose fill can't be booked is canceled with the reason,
// and its hold released, rather than tried again on every quote.
func TestUnbookableFillsCancelTheOrder(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	h.quote("AAPL", "100.00", "100.10")
	h.place(t, market(alice, ledger.Buy, "1"))
	o := h.place(t, limit(alice, ledger.Sell, "1", "101"))
	_, err := h.pool.Exec(h.ctx, `DELETE FROM fee_rates`)
	noErr(t, err)

	h.quote("AAPL", "101.00", "101.10")
	h.engine.MatchForTest(h.ctx, "AAPL")
	got, err := h.engine.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Canceled, "")
	if got.CancelReason != "regulatory fee rates aren't set up" {
		t.Errorf("cancel reason %q", got.CancelReason)
	}
	h.wantBalance(t, alice, "AAPL", "1", "0")
	h.requireReconciled(t)
}

// A sale worth less than its fees, from an account without the cash for
// the difference, is rejected when it would fill as it's placed: the
// customer can still be told no. A resting sale accepted earlier can't be
// refused once it fills, so the deficits account covers it and the account
// is restricted.
func TestSalesTheAccountCantPayFeesOn(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{Commission: dec("1")})
	alice := h.account(t, "1.41")
	h.quote("AAPL", "0.40", "0.41")
	wantStatus(t, h.place(t, limit(alice, ledger.Buy, "1", "0.41")), orders.Filled, "") // all the cash
	h.wantBalance(t, alice, ledger.USD, "0", "0")

	wantStatus(t, h.place(t, market(alice, ledger.Sell, "1")), orders.Rejected, "not enough cash for the fees on this sale")
	h.wantBalance(t, alice, "AAPL", "1", "0")
	h.wantBalance(t, ledger.Deficits, ledger.USD, "0", "0")

	h.quote("AAPL", "0.39", "0.40")
	resting := h.place(t, limit(alice, ledger.Sell, "1", "0.40"))
	wantStatus(t, resting, orders.Accepted, "")
	h.quote("AAPL", "0.40", "0.41")
	h.engine.MatchForTest(h.ctx, "AAPL")
	got, err := h.engine.Order(h.ctx, alice, resting.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Filled, "")
	h.wantBalance(t, alice, ledger.USD, "0", "0")
	h.wantBalance(t, ledger.Deficits, ledger.USD, "-0.6", "0")
	a, err := ledger.AccountByID(h.ctx, h.pool, alice)
	noErr(t, err)
	if a.Status != ledger.AccountRestricted {
		t.Errorf("account is %s after a resting sale it couldn't pay for, want restricted", a.Status)
	}
	h.requireReconciled(t)
}

// An order's commission is fixed when it's accepted, so a later change to
// the configured commission can't leave its hold short.
func TestCommissionIsFixedAtAccept(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{Commission: dec("1")})
	alice := h.account(t, "1000")
	o := h.place(t, limit(alice, ledger.Buy, "1", "99")) // holds 99 + 1
	later, err := engine.New(h.ctx, engine.Config{
		Pool: h.pool, Quotes: h.quotes, Calendar: calendar.New(), Now: h.clock, Commission: dec("5"),
	})
	noErr(t, err)

	h.quote("AAPL", "97", "98")
	later.MatchForTest(h.ctx, "AAPL")
	got, err := later.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Filled, "")
	h.wantBalance(t, alice, ledger.USD, "901", "0")
	h.requireReconciled(t)
}

// placeResting places n resting buys of one share at 99.
func (h *harness) placeResting(t *testing.T, account string, n int) []string {
	t.Helper()
	var ids []string
	for range n {
		ids = append(ids, h.place(t, limit(account, ledger.Buy, "1", "99")).ID)
	}
	return ids
}

func (h *harness) newEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.New(h.ctx, engine.Config{Pool: h.pool, Quotes: h.quotes, Calendar: calendar.New(), Now: h.clock})
	noErr(t, err)
	return e
}

// An engine started after orders were placed finds them in the database.
func TestNewEngineLoadsOpenOrders(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	ids := h.placeResting(t, alice, 3)

	restarted := h.newEngine(t)
	run(t, restarted)
	h.tick("AAPL", "97", "98", restarted)
	for _, id := range ids {
		h.waitFor(t, alice, id, orders.Filled)
	}
	h.wantBalance(t, alice, ledger.USD, "9706", "0")
}

// Two engines on one database fill each order once.
func TestTwoEnginesFillEachOrderOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	ids := h.placeResting(t, alice, 10)

	second := h.newEngine(t)
	run(t, h.engine)
	run(t, second)
	h.tick("AAPL", "97", "98", h.engine, second)
	for _, id := range ids {
		h.waitFor(t, alice, id, orders.Filled)
	}
	h.wantBalance(t, alice, "AAPL", "10", "0")
	h.wantBalance(t, alice, ledger.USD, "9020", "0")
	h.requireReconciled(t)
}
