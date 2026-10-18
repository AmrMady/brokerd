package engine

import (
	"context"
	"hash/maphash"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/orders"
)

// expiryBatch is how many orders one pass of the expiry loop takes at a time.
const expiryBatch = 100

// worker matches the symbols that hash to it, one at a time, so a symbol's
// orders always fill in book order while other symbols fill in parallel.
type worker struct {
	mu    sync.Mutex
	dirty map[ledger.Asset]struct{} // symbols with a new quote
	wake  chan struct{}
}

// HandleQuote marks the quote's symbol for matching. It's called on the
// feed's goroutine, so it only records the symbol and wakes its worker;
// the worker reads the latest quote when it gets to it, so a burst of
// quotes is matched once. Quotes must reach the engine's Quotes first.
func (e *Engine) HandleQuote(q marketdata.Quote) {
	e.markDirty(ledger.Asset(q.Symbol))
}

// HandleTrade does nothing: orders fill at quotes, not trades.
func (*Engine) HandleTrade(marketdata.Trade) {}

func (e *Engine) markDirty(symbol ledger.Asset) {
	w := e.workers[maphash.String(e.seed, string(symbol))%uint64(len(e.workers))]
	w.mu.Lock()
	w.dirty[symbol] = struct{}{}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run matches resting orders as quotes arrive and expires day orders at
// the close, until ctx is done.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, w := range e.workers {
		wg.Go(func() { e.work(ctx, w) })
	}
	wg.Go(func() { e.expireLoop(ctx) })
	wg.Wait()
	return ctx.Err()
}

func (e *Engine) work(ctx context.Context, w *worker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		}
		w.mu.Lock()
		dirty := w.dirty
		w.dirty = map[ledger.Asset]struct{}{}
		w.mu.Unlock()
		for symbol := range dirty {
			e.match(ctx, symbol)
		}
	}
}

// match fills the symbol's resting orders that its latest quote crosses,
// while the market is open.
func (e *Engine) match(ctx context.Context, symbol ledger.Asset) {
	if !e.cfg.Calendar.IsOpen(e.cfg.Now()) {
		return
	}
	q, err := e.quote(symbol)
	if err != nil {
		return // no fresh quote, nothing fills
	}
	e.mu.Lock()
	var due []candidate
	if b := e.books[symbol]; b != nil {
		due = b.crossing(q)
	}
	e.mu.Unlock()
	for _, c := range due {
		if ctx.Err() != nil {
			return
		}
		if err = e.fillResting(ctx, symbol, c.id, c.price); err != nil && ctx.Err() == nil {
			e.cfg.Logger.Error("fill resting order", "order", c.id, "error", err)
		}
	}
}

// fillResting fills a resting order at price, unless a cancel or an expiry
// got to it first, or the market has closed since it was matched.
func (e *Engine) fillResting(ctx context.Context, symbol ledger.Asset, id string, price decimal.Decimal) error {
	var (
		closed bool
		filled orders.Order
	)
	err := pgx.BeginFunc(ctx, e.cfg.Pool, func(tx pgx.Tx) error {
		o, err := orders.Lock(ctx, tx, id)
		if err != nil {
			return err
		}
		if !o.Open() {
			closed = true
			return nil
		}
		now := e.cfg.Now()
		if !e.cfg.Calendar.IsOpen(now) {
			return nil
		}
		if filled, err = e.fill(ctx, tx, o, price, now); err != nil {
			return err
		}
		closed = true
		return nil
	})
	if closed {
		e.unbook(symbol, id)
	}
	if err == nil && filled.Status == orders.Filled {
		e.cfg.ObserveFill(max(0, filled.ClosedAt.Sub(filled.CreatedAt)))
	}
	return err
}

func (e *Engine) expireLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.ExpiryInterval)
	defer ticker.Stop()
	for {
		if err := e.expireDue(ctx); err != nil && ctx.Err() == nil {
			e.cfg.Logger.Error("expire orders", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// expireDue expires the day orders whose session has closed.
func (e *Engine) expireDue(ctx context.Context) error {
	for {
		now := e.cfg.Now()
		due, err := orders.Expiring(ctx, e.cfg.Pool, now, expiryBatch)
		if err != nil {
			return err
		}
		for _, o := range due {
			if err = e.expire(ctx, o.ID, now); err != nil {
				return err
			}
		}
		if len(due) < expiryBatch {
			return nil
		}
	}
}

func (e *Engine) expire(ctx context.Context, id string, now time.Time) error {
	var symbol ledger.Asset
	err := pgx.BeginFunc(ctx, e.cfg.Pool, func(tx pgx.Tx) error {
		o, err := orders.Lock(ctx, tx, id)
		if err != nil {
			return err
		}
		symbol = o.Symbol
		if !o.Open() || o.ExpiresAt.After(now) {
			return nil
		}
		if _, err = ledger.ReleaseHold(ctx, tx, o.HoldID); err != nil {
			return err
		}
		_, err = orders.Expire(ctx, tx, id)
		return err
	})
	if err == nil {
		e.unbook(symbol, id)
	}
	return err
}

// rest adds an accepted limit order to its book and checks it against the
// latest quote, which may have moved since it was placed.
func (e *Engine) rest(o orders.Order) {
	e.mu.Lock()
	b := e.books[o.Symbol]
	if b == nil {
		b = &book{}
		e.books[o.Symbol] = b
	}
	b.add(o)
	e.mu.Unlock()
	e.markDirty(o.Symbol)
}

func (e *Engine) unbook(symbol ledger.Asset, id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if b := e.books[symbol]; b != nil {
		b.remove(id)
		if b.empty() {
			delete(e.books, symbol)
		}
	}
}
