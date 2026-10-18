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
