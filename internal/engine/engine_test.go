package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

// Friday, 10:00 in New York.
var friday = time.Date(2026, 10, 16, 14, 0, 0, 0, time.UTC)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

type harness struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	quotes *marketdata.QuoteCache
	engine *engine.Engine

	mu  sync.Mutex
	now time.Time
}

func newHarness(t *testing.T, cfg engine.Config) *harness {
	t.Helper()
	h := &harness{ctx: t.Context(), pool: pgtest.NewPool(t), quotes: &marketdata.QuoteCache{}, now: friday}
	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		for symbol, scale := range map[ledger.Asset]int32{"AAPL": 9, "MSFT": 9, "WHOLE": 0} {
			if err := ledger.AddAsset(h.ctx, tx, symbol, scale); err != nil {
				return err
			}
		}
		for _, r := range []ledger.FeeRate{
			{Fee: ledger.SECFee, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
			{Fee: ledger.TAF, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), MaxPerTrade: decimal.NewNullDecimal(decimal.Zero)},
		} {
			if err := ledger.AddFeeRate(h.ctx, tx, r); err != nil {
				return err
			}
		}
		return nil
	})
	noErr(t, err)
	cfg.Pool, cfg.Quotes, cfg.Calendar = h.pool, h.quotes, calendar.New()
	cfg.Now = h.clock
	h.engine, err = engine.New(h.ctx, cfg)
	noErr(t, err)
	return h
}

func (h *harness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) setNow(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = t
}

// account opens an account with cash in it.
func (h *harness) account(t *testing.T, cash string) string {
	t.Helper()
	var id string
	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		a, err := ledger.OpenAccount(h.ctx, tx)
		if err != nil {
			return err
		}
		id = a.ID
		tr, err := ledger.RequestDeposit(h.ctx, tx, ledger.CashTransfer{IdempotencyKey: "seed", AccountID: id, Amount: dec(cash)})
		if err != nil {
			return err
		}
		_, err = ledger.CompleteTransfer(h.ctx, tx, tr.ID)
		return err
	})
	noErr(t, err)
	return id
}

// deposit adds cash to an account.
func (h *harness) deposit(t *testing.T, account, cash string) {
	t.Helper()
	err := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
		tr, err := ledger.RequestDeposit(h.ctx, tx, ledger.CashTransfer{IdempotencyKey: "deposit-" + cash, AccountID: account, Amount: dec(cash)})
		if err != nil {
			return err
		}
		_, err = ledger.CompleteTransfer(h.ctx, tx, tr.ID)
		return err
	})
	noErr(t, err)
}

func (h *harness) quote(symbol, bid, ask string) {
	h.quotes.HandleQuote(marketdata.Quote{
		Symbol: symbol, Bid: dec(bid), BidSize: dec("100"), Ask: dec(ask), AskSize: dec("100"), Timestamp: h.clock(),
	})
}

func market(account string, side ledger.Side, qty string) orders.Request {
	return orders.Request{
		AccountID: account, Symbol: "AAPL", Side: side, Type: orders.Market, TimeInForce: orders.Day, Quantity: dec(qty),
	}
}

func limit(account string, side ledger.Side, qty, price string) orders.Request {
	return orders.Request{
		AccountID: account, Symbol: "AAPL", Side: side, Type: orders.Limit, TimeInForce: orders.GTC,
		Quantity: dec(qty), LimitPrice: decimal.NewNullDecimal(dec(price)),
	}
}

func (h *harness) place(t *testing.T, r orders.Request) orders.Order {
	t.Helper()
	o, err := h.engine.Place(h.ctx, r)
	noErr(t, err)
	return o
}

func (h *harness) balance(t *testing.T, account string, asset ledger.Asset) ledger.Balance {
	t.Helper()
	balances, err := ledger.Balances(h.ctx, h.pool, account)
	noErr(t, err)
	for _, b := range balances {
		if b.Asset == asset {
			return b
		}
	}
	return ledger.Balance{AccountID: account, Asset: asset}
}

func (h *harness) wantBalance(t *testing.T, account string, asset ledger.Asset, balance, held string) {
	t.Helper()
	if b := h.balance(t, account, asset); !b.Balance.Equal(dec(balance)) || !b.Held.Equal(dec(held)) {
		t.Errorf("%s: balance %s held %s, want balance %s held %s", asset, b.Balance, b.Held, balance, held)
	}
}

func (h *harness) requireReconciled(t *testing.T) {
	t.Helper()
	report, err := ledger.Reconcile(h.ctx, h.pool)
	noErr(t, err)
	for _, m := range report.Mismatches {
		t.Errorf("reconcile: %s mismatch on %s/%s: want %s, got %s", m.Check, m.AccountID, m.Asset, m.Want, m.Got)
	}
}

// failChange makes Postgres refuse to move order id to status, at the
// change or, with atCommit, when its transaction commits.
func (h *harness) failChange(t *testing.T, id string, status orders.Status, atCommit bool) {
	t.Helper()
	when := "BEFORE UPDATE ON orders"
	if atCommit {
		when = "AFTER UPDATE ON orders DEFERRABLE INITIALLY DEFERRED"
	}
	trigger := "TRIGGER"
	if atCommit {
		trigger = "CONSTRAINT TRIGGER"
	}
	_, err := h.pool.Exec(h.ctx, fmt.Sprintf(`
		CREATE FUNCTION fail_change() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'test: order %%s can''t be %%s', NEW.id, NEW.status;
		END;
		$$;
		CREATE %s fail_change %s FOR EACH ROW
			WHEN (NEW.id = '%s' AND NEW.status = '%s') EXECUTE FUNCTION fail_change();`,
		trigger, when, id, status))
	noErr(t, err)
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantStatus(t *testing.T, o orders.Order, status orders.Status, reason string) {
	t.Helper()
	if o.Status != status || o.RejectReason != reason {
		t.Errorf("order %s %q, want %s %q", o.Status, o.RejectReason, status, reason)
	}
}

func TestMarketOrdersFillAtTheQuote(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	h.quote("AAPL", "100.00", "100.10")

	buy := h.place(t, market(alice, ledger.Buy, "10"))
	wantStatus(t, buy, orders.Filled, "")
	if !buy.FillPrice.Decimal.Equal(dec("100.10")) {
		t.Errorf("buy filled at %s, want the ask", buy.FillPrice.Decimal)
	}
	h.wantBalance(t, alice, ledger.USD, "8999", "0") // the collar's extra is released
	h.wantBalance(t, alice, "AAPL", "10", "0")

	var trade, settle string
	noErr(t, h.pool.QueryRow(h.ctx, `SELECT metadata->>'trade_date', metadata->>'settle_date' FROM journal_entries WHERE id = $1`,
		buy.FillEntryID).Scan(&trade, &settle))
	if trade != "2026-10-16" || settle != "2026-10-19" {
		t.Errorf("traded %s, settles %s; want Friday and the next Monday", trade, settle)
	}

	sell := h.place(t, market(alice, ledger.Sell, "4"))
	wantStatus(t, sell, orders.Filled, "")
	if !sell.FillPrice.Decimal.Equal(dec("100.00")) {
		t.Errorf("sell filled at %s, want the bid", sell.FillPrice.Decimal)
	}
	h.wantBalance(t, alice, ledger.USD, "9399", "0")
	h.wantBalance(t, alice, "AAPL", "6", "0")
	if b := h.balance(t, alice, ledger.USD); !b.Unsettled.Equal(dec("400")) {
		t.Errorf("unsettled %s, want the sale's 400 until Monday", b.Unsettled)
	}
	h.requireReconciled(t)
}

func TestRejections(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	h.quote("AAPL", "100.00", "100.10")
	h.quote("MSFT", "0", "415.25")

	restricted := h.account(t, "10000")
	_, err := func() (ledger.Account, error) {
		var a ledger.Account
		txErr := pgx.BeginFunc(h.ctx, h.pool, func(tx pgx.Tx) error {
			var e error
			a, e = ledger.SetAccountStatus(h.ctx, tx, restricted, ledger.AccountRestricted, "review")
			return e
		})
		return a, txErr
	}()
	noErr(t, err)

	staleHarness := newHarness(t, engine.Config{})
	staleHarness.quote("AAPL", "100.00", "100.10")
	staleHarness.setNow(friday.Add(2 * time.Minute))

	for _, tt := range []struct {
		name   string
		h      *harness
		cash   string
		r      func(account string) orders.Request
		reason string
	}{
		{
			"not enough cash", h, "100", func(a string) orders.Request { return market(a, ledger.Buy, "10") },
			"insufficient buying power",
		},
		{
			"enough at the ask but not with the collar", h, "1001", func(a string) orders.Request { return market(a, ledger.Buy, "10") },
			"insufficient buying power",
		},
		{
			"limit buy too big", h, "100", func(a string) orders.Request { return limit(a, ledger.Buy, "2", "99") },
			"insufficient buying power",
		},
		{
			"selling what isn't there", h, "100", func(a string) orders.Request { return market(a, ledger.Sell, "1") },
			"insufficient shares of AAPL",
		},
		{"no quote", h, "10000", func(a string) orders.Request {
			r := market(a, ledger.Buy, "1")
			r.Symbol = "WHOLE"
			return r
		}, "no quote for WHOLE"},
		{"no bid", h, "10000", func(a string) orders.Request {
			r := market(a, ledger.Sell, "1")
			r.Symbol = "MSFT"
			return r
		}, "no bid price for MSFT"},
		{
			"stale quote", staleHarness, "10000", func(a string) orders.Request { return market(a, ledger.Buy, "1") },
			"the quote for AAPL is 2m0s old",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			account := tt.h.account(t, tt.cash)
			o := tt.h.place(t, tt.r(account))
			wantStatus(t, o, orders.Rejected, tt.reason)
			if o.HoldID != 0 {
				t.Errorf("rejected order kept hold %d", o.HoldID)
			}
			tt.h.wantBalance(t, account, ledger.USD, tt.cash, "0")
		})
	}

	o := h.place(t, market(restricted, ledger.Buy, "1"))
	wantStatus(t, o, orders.Rejected, "account is restricted")
	h.requireReconciled(t)
}

func TestLimitOrdersRestAndCancel(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice, bob := h.account(t, "1000"), h.account(t, "1000")

	o := h.place(t, limit(alice, ledger.Buy, "5", "99.99"))
	wantStatus(t, o, orders.Accepted, "")
	if !o.ExpiresAt.IsZero() {
		t.Errorf("GTC order expires at %s", o.ExpiresAt)
	}
	h.wantBalance(t, alice, ledger.USD, "1000", "499.95")

	if _, err := h.engine.Cancel(h.ctx, bob, o.ID); !errors.Is(err, orders.ErrNotFound) {
		t.Errorf("cancel by another account: %v, want %v", err, orders.ErrNotFound)
	}
	if _, err := h.engine.Order(h.ctx, bob, o.ID); !errors.Is(err, orders.ErrNotFound) {
		t.Errorf("get by another account: %v, want %v", err, orders.ErrNotFound)
	}

	canceled, err := h.engine.Cancel(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, canceled, orders.Canceled, "")
	h.wantBalance(t, alice, ledger.USD, "1000", "0")
	if _, err = h.engine.Cancel(h.ctx, alice, o.ID); !errors.Is(err, orders.ErrNotOpen) {
		t.Errorf("second cancel: %v, want %v", err, orders.ErrNotOpen)
	}
	got, err := h.engine.Order(h.ctx, alice, o.ID)
	noErr(t, err)
	wantStatus(t, got, orders.Canceled, "")
	h.requireReconciled(t)
}

func TestDayOrdersExpireAtTheClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	day := limit(alice, ledger.Buy, "1", "10")
	day.TimeInForce = orders.Day

	if o := h.place(t, day); !o.ExpiresAt.Equal(time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)) {
		t.Errorf("placed Friday morning, expires %s, want Friday's close", o.ExpiresAt.UTC())
	}
	h.setNow(time.Date(2026, 10, 16, 21, 0, 0, 0, time.UTC))
	if o := h.place(t, day); !o.ExpiresAt.Equal(time.Date(2026, 10, 19, 20, 0, 0, 0, time.UTC)) {
		t.Errorf("placed after Friday's close, expires %s, want Monday's close", o.ExpiresAt.UTC())
	}
}

// Market orders are only taken while the market is open. Limit orders are
// taken at any time, but don't fill until it opens, even when the quote
// already crosses them.
func TestMarketHours(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	run(t, h.engine)
	alice := h.account(t, "10000")
	var resting []string
	for _, now := range []time.Time{
		time.Date(2026, 10, 16, 13, 29, 0, 0, time.UTC), // Friday, 09:29 in New York
		time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC),  // Friday's close
		time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC),  // Saturday
	} {
		h.setNow(now)
		h.tick("AAPL", "100.00", "100.10")
		wantStatus(t, h.place(t, market(alice, ledger.Buy, "1")), orders.Rejected, "market is closed")
		o := h.place(t, limit(alice, ledger.Buy, "1", "101"))
		wantStatus(t, o, orders.Accepted, "")
		resting = append(resting, o.ID)
	}
	time.Sleep(50 * time.Millisecond) // for the matcher to have seen the quotes
	for _, id := range resting {
		o, err := h.engine.Order(h.ctx, alice, id)
		noErr(t, err)
		wantStatus(t, o, orders.Accepted, "")
	}

	h.setNow(time.Date(2026, 10, 19, 13, 30, 0, 0, time.UTC)) // Monday's open
	h.tick("AAPL", "100.00", "100.10")
	for _, id := range resting {
		h.waitFor(t, alice, id, orders.Filled)
	}
	wantStatus(t, h.place(t, market(alice, ledger.Buy, "1")), orders.Filled, "")
	h.requireReconciled(t)
}

func TestCommission(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{Commission: dec("1")})
	alice := h.account(t, "10000")
	h.quote("AAPL", "100.00", "100.10")
	h.place(t, market(alice, ledger.Buy, "10"))
	h.wantBalance(t, alice, ledger.USD, "8998", "0")
	h.wantBalance(t, ledger.Fees, ledger.USD, "1", "0")
}

func TestClientOrderIDs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	h.quote("AAPL", "100.00", "100.10")
	r := market(alice, ledger.Buy, "1")
	r.ClientOrderID = "retry-me"

	ids := make([]string, 10)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			o, err := h.engine.Place(h.ctx, r)
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = o.ID
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("retries placed different orders: %v", ids)
		}
	}
	h.wantBalance(t, alice, "AAPL", "1", "0")

	r.Quantity = dec("2")
	if _, err := h.engine.Place(h.ctx, r); !errors.Is(err, orders.ErrClientOrderIDConflict) {
		t.Errorf("reusing the id for another order: %v, want %v", err, orders.ErrClientOrderIDConflict)
	}
}

// With $1000, a $100 ask and a 2% collar, each one-share buy holds $102 and
// costs $100. However 20 of them interleave, exactly 9 fill: an order is
// only rejected once 9 are filled or holding, every hold fills, and a tenth
// fill would need $102 of the last $100.
func TestConcurrentMarketBuys(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	h.quote("AAPL", "99.90", "100")

	statuses := make([]orders.Status, 20)
	var wg sync.WaitGroup
	for i := range statuses {
		wg.Go(func() {
			r := market(alice, ledger.Buy, "1")
			r.ClientOrderID = fmt.Sprintf("buy-%d", i)
			o, err := h.engine.Place(h.ctx, r)
			if err != nil {
				t.Error(err)
				return
			}
			statuses[i] = o.Status
		})
	}
	wg.Wait()
	count := map[orders.Status]int{}
	for _, s := range statuses {
		count[s]++
	}
	if count[orders.Filled] != 9 || count[orders.Rejected] != 11 {
		t.Errorf("statuses %v, want 9 filled and 11 rejected", count)
	}
	h.wantBalance(t, alice, ledger.USD, "100", "0")
	h.wantBalance(t, alice, "AAPL", "9", "0")
	h.requireReconciled(t)
}

func TestInvalidOrders(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	unknown := limit(alice, ledger.Buy, "1", "10")
	unknown.Symbol = "NOPE"
	fractional := limit(alice, ledger.Buy, "1.5", "10")
	fractional.Symbol = "WHOLE"
	noAccount := limit("00000000-0000-0000-0000-000000000099", ledger.Buy, "1", "10")

	for _, tt := range []struct {
		name string
		r    orders.Request
		want error
	}{
		{"unknown symbol", unknown, orders.ErrInvalid},
		{"fractional shares of a whole-share symbol", fractional, orders.ErrInvalid},
		{"no limit price", limit(alice, ledger.Buy, "1", "0"), orders.ErrInvalid},
		{"unknown account", noAccount, ledger.ErrAccountNotFound},
	} {
		if _, err := h.engine.Place(h.ctx, tt.r); !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", tt.name, err, tt.want)
		}
	}
	list, err := orders.List(h.ctx, h.pool, alice, false, "", 10)
	noErr(t, err)
	if len(list) != 0 {
		t.Errorf("invalid requests stored %d orders", len(list))
	}
}

func TestSellsNeedFeeRates(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "1000")
	h.quote("AAPL", "100.00", "100.10")
	h.place(t, market(alice, ledger.Buy, "1"))
	_, err := h.pool.Exec(h.ctx, `DELETE FROM fee_rates`)
	noErr(t, err)
	wantStatus(t, h.place(t, market(alice, ledger.Sell, "1")), orders.Rejected, "regulatory fee rates aren't set up")
	h.wantBalance(t, alice, "AAPL", "1", "0")
}

// A fill writes its entry_posted event, then its order_filled event, in the
// same transaction.
func TestFillsWriteEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engine.Config{})
	alice := h.account(t, "10000")
	h.quote("AAPL", "100.00", "100.10")
	o := h.place(t, market(alice, ledger.Buy, "2"))

	rows, err := h.pool.Query(h.ctx, `SELECT type, key, payload->>'order_id', payload->>'price' FROM outbox WHERE payload->>'kind' = 'trade_fill' OR type = 'order_filled' ORDER BY seq`)
	noErr(t, err)
	type event struct{ Type, Key, OrderID, Price *string }
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[event])
	noErr(t, err)
	if len(events) != 2 || *events[0].Type != "entry_posted" || *events[1].Type != "order_filled" {
		t.Fatalf("events %+v, want the fill's entry then the order", events)
	}
	if f := events[1]; *f.Key != alice || *f.OrderID != o.ID || *f.Price != "100.1" {
		t.Errorf("order_filled key %s order %s price %s", *f.Key, *f.OrderID, *f.Price)
	}
}
