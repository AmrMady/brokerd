package marketdata

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

// Bar is a symbol's trading over one interval of event time.
type Bar struct {
	Symbol   string
	Start    time.Time
	Interval time.Duration
	Open     decimal.Decimal
	High     decimal.Decimal
	Low      decimal.Decimal
	Close    decimal.Decimal
	Volume   decimal.Decimal
	Trades   int
}

// BarHandler receives bars as their windows close. Like a Handler, it runs
// on the feed's goroutine and must not block.
type BarHandler interface {
	HandleBar(Bar)
}

// Aggregator builds bars of one interval from trades, by event time.
// Windows start at multiples of the interval. A window closes once an event
// at least grace past its end has been seen, from any symbol and including
// quotes, and then its bars are sent on, sorted by symbol. A trade for a
// window that has closed is late: it's dropped and counted. Windows with no
// trades have no bar.
//
// If the feed goes quiet, no event moves time on, so Tick, called on a
// timer, does: while no events arrive, event time is taken to keep pace
// with the wall clock from when they stopped.
//
// Its methods can be called from any goroutine.
type Aggregator struct {
	interval time.Duration
	grace    time.Duration
	out      BarHandler

	mu           sync.Mutex
	windows      []*window // open, oldest first
	watermark    time.Time // the latest event time seen
	closedBefore time.Time // windows starting before this have closed
	fed          bool      // an event arrived since the last Tick
	quietSince   time.Time // wall-clock time of the first Tick with no events
	quietFrom    time.Time // the watermark then

	bars, late atomic.Uint64
}

type window struct {
	start time.Time
	bars  map[string]*openBar
}

type openBar struct {
	Bar
	first, last time.Time // times of the trades that set Open and Close
}

// AggregatorStats counts what an Aggregator has done.
type AggregatorStats struct {
	Bars uint64 // bars sent on
	Late uint64 // trades dropped because their window had closed
}

// NewAggregator returns an Aggregator that sends bars of the given interval
// to out.
func NewAggregator(interval, grace time.Duration, out BarHandler) (*Aggregator, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("bar interval must be positive, got %s", interval)
	}
	if grace < 0 {
		return nil, fmt.Errorf("grace must not be negative, got %s", grace)
	}
	return &Aggregator{interval: interval, grace: grace, out: out}, nil
}

// HandleTrade adds t to its bar.
func (a *Aggregator) HandleTrade(t Trade) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fed = true
	a.advance(t.Timestamp)
	start := t.Timestamp.Truncate(a.interval)
	if start.Before(a.closedBefore) {
		a.late.Add(1)
		return
	}

	w := a.window(start)
	b := w.bars[t.Symbol]
	if b == nil {
		w.bars[t.Symbol] = &openBar{
			Bar: Bar{
				Symbol: t.Symbol, Start: start, Interval: a.interval,
				Open: t.Price, High: t.Price, Low: t.Price, Close: t.Price, Volume: t.Size, Trades: 1,
			},
			first: t.Timestamp,
			last:  t.Timestamp,
		}
		return
	}
	// Trades can arrive out of order within the grace period, so Open and
	// Close go by event time. Of trades at the same time, the first to
	// arrive opens and the last closes.
	if t.Timestamp.Before(b.first) {
		b.Open, b.first = t.Price, t.Timestamp
	}
	if !t.Timestamp.Before(b.last) {
		b.Close, b.last = t.Price, t.Timestamp
	}
	b.High = decimal.Max(b.High, t.Price)
	b.Low = decimal.Min(b.Low, t.Price)
	b.Volume = b.Volume.Add(t.Size)
	b.Trades++
}

// HandleQuote moves event time forward, which can close windows.
func (a *Aggregator) HandleQuote(q Quote) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fed = true
	a.advance(q.Timestamp)
}

// Tick moves event time on by the wall-clock time since the feed went
// quiet, if no event has arrived since the last Tick, so the last bars
// still close. Call it every second or so.
func (a *Aggregator) Tick(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.watermark.IsZero() {
		return
	}
	if a.fed || a.quietSince.IsZero() {
		a.fed, a.quietSince, a.quietFrom = false, now, a.watermark
		return
	}
	a.advance(a.quietFrom.Add(now.Sub(a.quietSince)))
}

// Flush closes every open window, for when the feed has stopped.
func (a *Aggregator) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range a.windows {
		a.emit(w)
		a.closedBefore = w.start.Add(a.interval)
	}
	a.windows = nil
}

// Stats returns the counts so far.
func (a *Aggregator) Stats() AggregatorStats {
	return AggregatorStats{Bars: a.bars.Load(), Late: a.late.Load()}
}

func (a *Aggregator) advance(t time.Time) {
	if !t.After(a.watermark) {
		return
	}
	a.watermark = t
	// [s, s+interval) has closed once s+interval+grace <= watermark, which
	// for s on the interval grid is s < (watermark-grace) truncated.
	cut := t.Add(-a.grace).Truncate(a.interval)
	if !cut.After(a.closedBefore) {
		return
	}
	a.closedBefore = cut
	n := 0
	for _, w := range a.windows {
		if !w.start.Before(cut) {
			break
		}
		a.emit(w)
		n++
	}
	a.windows = slices.Delete(a.windows, 0, n)
}

// window returns the open window starting at start, opening it if needed.
// There are only a few open windows, and most trades are for the newest.
func (a *Aggregator) window(start time.Time) *window {
	i := len(a.windows)
	for i > 0 && !a.windows[i-1].start.Before(start) {
		if a.windows[i-1].start.Equal(start) {
			return a.windows[i-1]
		}
		i--
	}
	w := &window{start: start, bars: map[string]*openBar{}}
	a.windows = slices.Insert(a.windows, i, w)
	return w
}

func (a *Aggregator) emit(w *window) {
	for _, symbol := range slices.Sorted(maps.Keys(w.bars)) {
		a.out.HandleBar(w.bars[symbol].Bar)
		a.bars.Add(1)
	}
}
