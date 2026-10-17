package marketdata

import "sync/atomic"

// Sequencer passes events on to the next Handler in sequence order. A
// symbol's first event starts its sequence. An event that skips ahead is
// passed on and counted as a gap. An event at or behind the last one passed
// on is stale: a duplicate, or one that arrived after a later event. It is
// dropped and counted.
//
// The Handle methods are called from the feed's goroutine only; Stats can be
// called from any goroutine.
type Sequencer struct {
	next Handler
	last map[string]uint64

	gaps, missed, stale atomic.Uint64
}

// SequenceStats counts what a Sequencer has seen.
type SequenceStats struct {
	Gaps   uint64 // events that skipped ahead
	Missed uint64 // sequence numbers those events skipped
	Stale  uint64 // events dropped as duplicates or out of order
}

// NewSequencer returns a Sequencer that passes events on to next.
func NewSequencer(next Handler) *Sequencer {
	return &Sequencer{next: next, last: map[string]uint64{}}
}

// HandleTrade implements Handler.
func (s *Sequencer) HandleTrade(t Trade) {
	if s.accept(t.Symbol, t.Seq) {
		s.next.HandleTrade(t)
	}
}

// HandleQuote implements Handler.
func (s *Sequencer) HandleQuote(q Quote) {
	if s.accept(q.Symbol, q.Seq) {
		s.next.HandleQuote(q)
	}
}

// Stats returns the counts so far.
func (s *Sequencer) Stats() SequenceStats {
	return SequenceStats{Gaps: s.gaps.Load(), Missed: s.missed.Load(), Stale: s.stale.Load()}
}

func (s *Sequencer) accept(symbol string, seq uint64) bool {
	last, seen := s.last[symbol]
	switch {
	case !seen:
	case seq <= last:
		s.stale.Add(1)
		return false
	case seq > last+1:
		s.gaps.Add(1)
		s.missed.Add(seq - last - 1)
	}
	s.last[symbol] = seq
	return true
}
