package marketdata

import (
	"sync"
	"time"
)

// BarHandlers passes each bar to every handler in it, in order.
type BarHandlers []BarHandler

// HandleBar implements BarHandler.
func (hs BarHandlers) HandleBar(b Bar) {
	for _, h := range hs {
		h.HandleBar(b)
	}
}

// BarStore keeps the most recent bars of each symbol and interval, for
// reading while the feed runs. It's written once a second at most per
// symbol, so a plain read-write lock is enough.
type BarStore struct {
	size int

	mu   sync.RWMutex
	bars map[barKey][]Bar // oldest first
}

type barKey struct {
	symbol   string
	interval time.Duration
}

// NewBarStore returns a store that keeps size bars of each symbol and
// interval.
func NewBarStore(size int) *BarStore {
	return &BarStore{size: max(size, 1), bars: map[barKey][]Bar{}}
}

// HandleBar implements BarHandler. Bars arrive in time order, so keeping the
// newest is keeping the last.
func (s *BarStore) HandleBar(b Bar) {
	k := barKey{b.Symbol, b.Interval}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bars[k] = append(s.bars[k], b)
	// Trimming only when there's twice as many as needed keeps appends cheap.
	if bars := s.bars[k]; len(bars) >= 2*s.size {
		s.bars[k] = append([]Bar(nil), bars[len(bars)-s.size:]...)
	}
}

// Bars returns up to limit of the most recent bars, oldest first.
func (s *BarStore) Bars(symbol string, interval time.Duration, limit int) []Bar {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bars := s.bars[barKey{symbol, interval}]
	n := min(limit, s.size, len(bars))
	return append([]Bar(nil), bars[len(bars)-n:]...)
}
