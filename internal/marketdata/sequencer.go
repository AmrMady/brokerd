package marketdata

import "sync/atomic"

// maxMissing is how many skipped sequence numbers a Sequencer remembers per
// symbol, so an event that arrives after a later one can still fill its
// gap.
const maxMissing = 1024

// Sequencer passes events on to the next Handler in sequence order. A
// symbol's first event starts its sequence. An event that skips ahead is
// passed on and counted as a gap, and the numbers it skipped as missed.
//
// An event behind the last one passed on is late. A late trade that fills a
// gap is still passed on, so bars count its volume; a late quote is
// dropped, since only the newest price matters. Either way its number is no
// longer missed. Anything else behind the last one, like a duplicate, is
// dropped as stale. Only the newest maxMissing skipped numbers of each
// symbol are remembered; an event older than those is stale.
//
// The Handle methods are called from the feed's goroutine only; Stats can be
// called from any goroutine.
type Sequencer struct {
	next    Handler
	symbols map[string]*sequence

	events, gaps, missed, late, stale atomic.Uint64
}

type sequence struct {
	last    uint64
	missing map[uint64]struct{} // skipped numbers, the newest maxMissing
}

// SequenceStats counts what a Sequencer has seen.
type SequenceStats struct {
	Events uint64 // events passed on
	Gaps   uint64 // events that skipped ahead
	Missed uint64 // sequence numbers skipped and not filled since
	Late   uint64 // events that filled a gap
	Stale  uint64 // events dropped as duplicates, too late, or late quotes
}

// NewSequencer returns a Sequencer that passes events on to next.
func NewSequencer(next Handler) *Sequencer {
	return &Sequencer{next: next, symbols: map[string]*sequence{}}
}

// HandleTrade implements Handler.
func (s *Sequencer) HandleTrade(t Trade) {
	if s.accept(t.Symbol, t.Seq, true) {
		s.next.HandleTrade(t)
	}
}

// HandleQuote implements Handler.
func (s *Sequencer) HandleQuote(q Quote) {
	if s.accept(q.Symbol, q.Seq, false) {
		s.next.HandleQuote(q)
	}
}

// Stats returns the counts so far.
func (s *Sequencer) Stats() SequenceStats {
	return SequenceStats{
		Events: s.events.Load(), Gaps: s.gaps.Load(), Missed: s.missed.Load(),
		Late: s.late.Load(), Stale: s.stale.Load(),
	}
}

func (s *Sequencer) accept(symbol string, seq uint64, trade bool) bool {
	sq := s.symbols[symbol]
	switch {
	case sq == nil:
		s.symbols[symbol] = &sequence{last: seq, missing: map[uint64]struct{}{}}
	case seq > sq.last:
		if seq > sq.last+1 {
			s.gaps.Add(1)
			s.missed.Add(seq - sq.last - 1)
			sq.skip(sq.last+1, seq-1)
		}
		sq.last = seq
	case sq.fill(seq):
		s.missed.Add(^uint64(0)) // -1
		s.late.Add(1)
		if !trade {
			s.stale.Add(1)
			return false
		}
	default:
		s.stale.Add(1)
		return false
	}
	s.events.Add(1)
	return true
}

// skip remembers that from to to were skipped, keeping the newest
// maxMissing numbers.
func (sq *sequence) skip(from, to uint64) {
	if to-from >= maxMissing {
		from = to - maxMissing + 1
	}
	for n := from; n <= to; n++ {
		sq.missing[n] = struct{}{}
	}
	if len(sq.missing) > maxMissing {
		for n := range sq.missing {
			if n+maxMissing <= to {
				delete(sq.missing, n)
			}
		}
	}
}

// fill reports whether seq was missing, and forgets it.
func (sq *sequence) fill(seq uint64) bool {
	if _, ok := sq.missing[seq]; !ok {
		return false
	}
	delete(sq.missing, seq)
	return true
}
