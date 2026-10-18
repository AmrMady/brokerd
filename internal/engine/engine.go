// Package engine is the paper-trading engine: it accepts orders against the
// ledger and fills them at the latest quotes.
package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/outbox"
)

// Quotes gives the latest quote for a symbol.
type Quotes interface {
	Quote(symbol string) (marketdata.Quote, bool)
}

// Config configures an Engine.
type Config struct {
	Pool     *pgxpool.Pool
	Quotes   Quotes
	Calendar *calendar.Calendar
	// Commission is charged on every fill, in USD.
	Commission decimal.Decimal
	// Collar is how far above the ask a market buy's hold goes, as a
	// fraction: 0.02 holds 2% more than the order costs at the ask.
	// Default 0.02.
	Collar decimal.Decimal
	// MaxQuoteAge is how old a quote can be for an order to fill at it.
	// Default 1m.
	MaxQuoteAge time.Duration
	// Workers is how many goroutines match resting orders. Default 4.
	Workers int
	// ExpiryInterval is how often day orders are checked for expiry.
	// Default 1s.
	ExpiryInterval time.Duration
	Now            func() time.Time
	Logger         *slog.Logger
	// ObserveFill, if set, gets how long each order took from being
	// placed to filling, and ObservePost how long each fill took to post
	// to the ledger.
	ObserveFill func(time.Duration)
	ObservePost func(time.Duration)
}

// Engine places, fills, cancels and expires orders. Its methods are safe to
// call from any goroutine; resting orders only fill while Run runs.
type Engine struct {
	cfg     Config
	seed    maphash.Seed
	workers []*worker

	mu    sync.Mutex
	books map[ledger.Asset]*book // resting limit orders by symbol
}

// New returns an Engine with the open orders already in the database in
// its books, so a restart picks up where it left off.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Pool == nil || cfg.Quotes == nil || cfg.Calendar == nil {
		return nil, errors.New("engine needs a pool, quotes and a calendar")
	}
	if cfg.Commission.IsNegative() || !cfg.Commission.Equal(cfg.Commission.Truncate(2)) {
		return nil, fmt.Errorf("commission must be whole cents and not negative, got %s", cfg.Commission)
	}
	if cfg.Collar.IsNegative() {
		return nil, fmt.Errorf("collar must not be negative, got %s", cfg.Collar)
	}
	if cfg.Collar.IsZero() {
		cfg.Collar = decimal.New(2, -2)
	}
	if cfg.MaxQuoteAge < 0 {
		return nil, fmt.Errorf("max quote age must not be negative, got %s", cfg.MaxQuoteAge)
	}
	if cfg.MaxQuoteAge == 0 {
		cfg.MaxQuoteAge = time.Minute
	}
	if cfg.Workers < 0 || cfg.ExpiryInterval < 0 {
		return nil, fmt.Errorf("workers and expiry interval must not be negative, got %d and %s", cfg.Workers, cfg.ExpiryInterval)
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.ExpiryInterval == 0 {
		cfg.ExpiryInterval = time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ObserveFill == nil {
		cfg.ObserveFill = func(time.Duration) {}
	}
	if cfg.ObservePost == nil {
		cfg.ObservePost = func(time.Duration) {}
	}

	e := &Engine{cfg: cfg, seed: maphash.MakeSeed(), books: map[ledger.Asset]*book{}}
	for range cfg.Workers {
		e.workers = append(e.workers, &worker{dirty: map[ledger.Asset]struct{}{}, wake: make(chan struct{}, 1)})
	}
	open, err := orders.OpenOrders(ctx, cfg.Pool)
	if err != nil {
		return nil, err
	}
	for _, o := range open {
		e.rest(o)
	}
	return e, nil
}

// rejection is why an order was turned down, as the client sees it.
type rejection string

func (r rejection) Error() string { return string(r) }

// Place records an order and, if the account can cover it, accepts it by
// placing a hold: cash for a buy, shares for a sell. A market order then
// fills at once at the ask or the bid, and so does a limit order the quote
// already crosses while the market is open; other limit orders rest until
// a quote crosses them in a session. An order the account can't cover, or
// a market order while the market is closed or with no usable quote, is
// stored as rejected, with the reason. Placing an order with a client
// order ID that was used before returns the first order.
func (e *Engine) Place(ctx context.Context, r orders.Request) (orders.Order, error) {
	if err := r.Validate(); err != nil {
		return orders.Order{}, err
	}
	if _, err := ledger.AccountByID(ctx, e.cfg.Pool, r.AccountID); err != nil {
		return orders.Order{}, err
	}
	scale, err := ledger.AssetScale(ctx, e.cfg.Pool, r.Symbol)
	if err != nil {
		return orders.Order{}, fmt.Errorf("%w: unknown symbol %s", orders.ErrInvalid, r.Symbol)
	}
	if !r.Quantity.Equal(r.Quantity.Truncate(scale)) {
		return orders.Order{}, fmt.Errorf("%w: %s trades in at most %d decimal places, got %s",
			orders.ErrInvalid, r.Symbol, scale, r.Quantity)
	}

	var (
		placed  orders.Order
		created bool
		began   = time.Now()
	)
	err = pgx.BeginFunc(ctx, e.cfg.Pool, func(tx pgx.Tx) error {
		var o orders.Order
		var txErr error
		if o, created, txErr = orders.Create(ctx, tx, r); txErr != nil || !created {
			placed = o
			return txErr
		}
		placed, txErr = e.accept(ctx, tx, o)
		return txErr
	})
	if err != nil {
		return orders.Order{}, err
	}
	switch {
	case placed.Open():
		e.rest(placed)
	case created && placed.Status == orders.Filled:
		e.cfg.ObserveFill(time.Since(began))
	}
	return placed, nil
}

// accept tries to accept, and for a market order fill, a new order inside a
// savepoint. When that fails for a reason the client should see, the
// savepoint is rolled back, which takes back the hold, and the order is
// rejected instead.
func (e *Engine) accept(ctx context.Context, tx pgx.Tx, o orders.Order) (orders.Order, error) {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return orders.Order{}, err
	}
	accepted, err := e.tryAccept(ctx, sp, o)
	var reason rejection
	if errors.As(err, &reason) {
		if err = sp.Rollback(ctx); err != nil {
			return orders.Order{}, err
		}
		e.cfg.Logger.Info("order rejected", "order", o.ID, "reason", string(reason))
		return orders.Reject(ctx, tx, o.ID, string(reason))
	}
	if err != nil {
		return orders.Order{}, errors.Join(err, sp.Rollback(ctx))
	}
	return accepted, sp.Commit(ctx)
}

func (e *Engine) tryAccept(ctx context.Context, tx pgx.Tx, o orders.Order) (orders.Order, error) {
	now := e.cfg.Now()
	open := e.cfg.Calendar.IsOpen(now)
	var price decimal.Decimal // what a market order fills at
	if o.Type == orders.Market {
		if !open {
			return orders.Order{}, rejection("market is closed")
		}
		q, err := e.quote(o.Symbol)
		if err != nil {
			return orders.Order{}, err
		}
		price = q.Ask
		if o.Side == ledger.Sell {
			price = q.Bid
		}
		if !price.IsPositive() {
			return orders.Order{}, rejection(fmt.Sprintf("no %s price for %s", side(o.Side), o.Symbol))
		}
	}

	hold, err := ledger.PlaceHold(ctx, tx, ledger.HoldRequest{
		IdempotencyKey: "order:" + o.ID,
		AccountID:      o.AccountID,
		Asset:          holdAsset(o),
		Amount:         e.holdAmount(o, price),
	})
	if err != nil {
		return orders.Order{}, rejectionFor(err, o)
	}
	var expires time.Time
	if o.TimeInForce == orders.Day {
		expires = e.cfg.Calendar.Close(e.cfg.Calendar.Session(now))
	}
	if o, err = orders.Accept(ctx, tx, o.ID, hold.ID, expires); err != nil {
		return orders.Order{}, err
	}
	if o.Type == orders.Limit && open {
		price = e.marketable(o)
	}
	if price.IsPositive() {
		if o, err = e.fill(ctx, tx, o, price, now); err != nil {
			return orders.Order{}, rejectionFor(err, o)
		}
	}
	return o, nil
}

// marketable returns the price a limit order would fill at right now, or
// zero if it would rest.
func (e *Engine) marketable(o orders.Order) decimal.Decimal {
	q, err := e.quote(o.Symbol)
	if err != nil {
		return decimal.Zero
	}
	b := book{}
	b.add(o)
	if due := b.crossing(q); len(due) > 0 {
		return due[0].price
	}
	return decimal.Zero
}

func (e *Engine) quote(symbol ledger.Asset) (marketdata.Quote, error) {
	q, ok := e.cfg.Quotes.Quote(string(symbol))
	if !ok {
		return q, rejection("no quote for " + string(symbol))
	}
	if age := e.cfg.Now().Sub(q.Timestamp); age > e.cfg.MaxQuoteAge {
		return q, rejection(fmt.Sprintf("the quote for %s is %s old", symbol, age.Round(time.Second)))
	}
	return q, nil
}

func holdAsset(o orders.Order) ledger.Asset {
	if o.Side == ledger.Sell {
		return o.Symbol
	}
	return ledger.USD
}

// holdAmount is what an order holds: the shares for a sell, and for a buy
// enough cash to pay for it at its limit, or at the ask plus the collar,
// plus the commission. Cash is rounded up to the cent so the hold always
// covers the fill.
func (e *Engine) holdAmount(o orders.Order, ask decimal.Decimal) decimal.Decimal {
	if o.Side == ledger.Sell {
		return o.Quantity
	}
	price := o.LimitPrice.Decimal
	if o.Type == orders.Market {
		price = ask.Mul(decimal.NewFromInt(1).Add(e.cfg.Collar))
	}
	return o.Quantity.Mul(price).RoundCeil(2).Add(e.cfg.Commission)
}

// fill books an accepted order's fill at price, made at now while the
// market is open, releases what's left of its hold, and marks it filled.
// The fill's ID is the ledger's idempotency key, so the same order can't
// fill twice.
func (e *Engine) fill(ctx context.Context, tx pgx.Tx, o orders.Order, price decimal.Decimal, now time.Time) (orders.Order, error) {
	session := e.cfg.Calendar.Session(now)
	began := time.Now()
	entry, err := ledger.SettleFill(ctx, tx, ledger.Fill{
		ID:         "fill:" + o.ID,
		HoldID:     o.HoldID,
		AccountID:  o.AccountID,
		Symbol:     o.Symbol,
		Side:       o.Side,
		Quantity:   o.Quantity,
		Price:      price,
		Fee:        e.cfg.Commission,
		TradeDate:  session,
		SettleDate: e.cfg.Calendar.SettlementDate(session),
	})
	if err != nil {
		return o, err
	}
	e.cfg.ObservePost(time.Since(began))
	hold, err := ledger.HoldByID(ctx, tx, o.HoldID)
	if err != nil {
		return o, err
	}
	if hold.Status == ledger.HoldActive {
		if _, err = ledger.ReleaseHold(ctx, tx, o.HoldID); err != nil {
			return o, err
		}
	}
	if o, err = orders.Fill(ctx, tx, o.ID, price, entry.ID); err != nil {
		return o, err
	}
	return o, outbox.Add(ctx, tx, outbox.OrderFilled, o.AccountID, filledEvent{
		OrderID: o.ID, AccountID: o.AccountID, ClientOrderID: o.ClientOrderID, Symbol: o.Symbol, Side: o.Side,
		Quantity: o.Quantity, Price: price, EntryID: entry.ID, FilledAt: o.ClosedAt,
	})
}

// filledEvent is the order_filled event.
type filledEvent struct {
	OrderID       string          `json:"order_id"`
	AccountID     string          `json:"account_id"`
	ClientOrderID string          `json:"client_order_id,omitempty"`
	Symbol        ledger.Asset    `json:"symbol"`
	Side          ledger.Side     `json:"side"`
	Quantity      decimal.Decimal `json:"qty"`
	Price         decimal.Decimal `json:"price"`
	EntryID       int64           `json:"entry_id"`
	FilledAt      time.Time       `json:"filled_at"`
}

// rejectionFor turns the ledger errors a client can cause into rejections,
// and leaves other errors alone.
func rejectionFor(err error, o orders.Order) error {
	switch {
	case errors.Is(err, ledger.ErrInsufficientFunds) && o.Side == ledger.Buy:
		return rejection("insufficient buying power")
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return rejection("insufficient shares of " + string(o.Symbol))
	case errors.Is(err, ledger.ErrAccountRestricted):
		return rejection("account is restricted")
	case errors.Is(err, ledger.ErrAccountClosed):
		return rejection("account is closed")
	case errors.Is(err, ledger.ErrNoFeeRate):
		return rejection("regulatory fee rates aren't set up")
	default:
		return err
	}
}

func side(s ledger.Side) string {
	if s == ledger.Buy {
		return "ask"
	}
	return "bid"
}

// Cancel cancels an open order and releases its hold. An order of another
// account is not found.
func (e *Engine) Cancel(ctx context.Context, accountID, orderID string) (orders.Order, error) {
	var canceled orders.Order
	err := pgx.BeginFunc(ctx, e.cfg.Pool, func(tx pgx.Tx) error {
		o, err := orders.Lock(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.AccountID != accountID {
			return fmt.Errorf("%w: %s", orders.ErrNotFound, orderID)
		}
		if !o.Open() {
			return fmt.Errorf("%w: %s is %s", orders.ErrNotOpen, orderID, o.Status)
		}
		if _, err = ledger.ReleaseHold(ctx, tx, o.HoldID); err != nil {
			return err
		}
		canceled, err = orders.Cancel(ctx, tx, orderID)
		return err
	})
	if err != nil {
		return orders.Order{}, err
	}
	e.unbook(canceled.Symbol, canceled.ID)
	return canceled, nil
}

// Order returns one of an account's orders.
func (e *Engine) Order(ctx context.Context, accountID, orderID string) (orders.Order, error) {
	o, err := orders.Get(ctx, e.cfg.Pool, orderID)
	if err != nil {
		return orders.Order{}, err
	}
	if o.AccountID != accountID {
		return orders.Order{}, fmt.Errorf("%w: %s", orders.ErrNotFound, orderID)
	}
	return o, nil
}
