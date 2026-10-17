package marketdata

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/shopspring/decimal"
)

// SimulatorConfig configures a Simulator.
type SimulatorConfig struct {
	Symbols []string
	Seed    uint64
	Rate    int       // events per second, across all symbols
	Start   time.Time // time of the first event; now if zero
	Paced   bool      // send events at Rate in real time, not as fast as possible
	Limit   int       // stop after this many events; 0 for no limit
}

// Simulator is a Feed of random-walk prices. Each symbol's midpoint moves by
// at most a cent per quote, with a spread of one to four cents, and trades
// print at the bid or the ask. Event times are spaced 1/Rate apart, so the
// same config always produces the same events.
type Simulator struct {
	cfg SimulatorConfig
}

// NewSimulator checks cfg and returns a Simulator.
func NewSimulator(cfg SimulatorConfig) (*Simulator, error) {
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("simulator needs at least one symbol")
	}
	seen := make(map[string]bool, len(cfg.Symbols))
	for _, s := range cfg.Symbols {
		if s == "" || seen[s] {
			return nil, fmt.Errorf("simulator symbols must be unique and not empty, got %q", cfg.Symbols)
		}
		seen[s] = true
	}
	if cfg.Rate <= 0 {
		return nil, fmt.Errorf("simulator rate must be positive, got %d", cfg.Rate)
	}
	if cfg.Limit < 0 {
		return nil, fmt.Errorf("simulator limit must not be negative, got %d", cfg.Limit)
	}
	return &Simulator{cfg: cfg}, nil
}

// One trade for every four quotes.
const tradeOdds = 5

// How often an unpaced run checks whether it was canceled.
const checkEvery = 256

type simSymbol struct {
	name   string
	mid    int64 // cents
	spread int64 // cents
	seq    uint64
}

func (s *simSymbol) bid() int64 { return s.mid - s.spread/2 }
func (s *simSymbol) ask() int64 { return s.bid() + s.spread }

type simulation struct {
	rng     *rand.Rand
	symbols []simSymbol
	start   time.Time
	rate    int64
}

// Run implements Feed.
func (s *Simulator) Run(ctx context.Context, h Handler) error {
	sim := simulation{
		rng:     rand.New(rand.NewPCG(s.cfg.Seed, s.cfg.Seed)), //nolint:gosec // simulated prices, not secrets
		symbols: make([]simSymbol, len(s.cfg.Symbols)),
		start:   s.cfg.Start,
		rate:    int64(s.cfg.Rate),
	}
	if sim.start.IsZero() {
		sim.start = time.Now()
	}
	for i, name := range s.cfg.Symbols {
		sim.symbols[i] = simSymbol{name: name, mid: 2000 + sim.rng.Int64N(48000), spread: 2}
	}

	limit := int64(s.cfg.Limit)
	if !s.cfg.Paced {
		for n := int64(0); limit == 0 || n < limit; n++ {
			if n%checkEvery == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
			sim.event(h, n)
		}
		return nil
	}

	// Waking for every event would cost more than the events at high rates,
	// so a paced run wakes every millisecond and sends whatever is due.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	wallStart := time.Now()
	for n := int64(0); ; {
		due := sim.due(time.Since(wallStart))
		if limit > 0 {
			due = min(due, limit)
		}
		for ; n < due; n++ {
			sim.event(h, n)
		}
		if limit > 0 && n == limit {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// offset is how long after the first event the nth one happens.
func (sim *simulation) offset(n int64) time.Duration {
	return time.Duration(n/sim.rate)*time.Second + time.Duration(n%sim.rate)*time.Second/time.Duration(sim.rate)
}

// due returns how many events are due once elapsed has passed.
func (sim *simulation) due(elapsed time.Duration) int64 {
	secs, rest := int64(elapsed/time.Second), int64(elapsed%time.Second)
	return secs*sim.rate + rest*sim.rate/int64(time.Second) + 1
}

func (sim *simulation) event(h Handler, n int64) {
	s := &sim.symbols[sim.rng.IntN(len(sim.symbols))]
	s.seq++
	ts := sim.start.Add(sim.offset(n))

	if sim.rng.IntN(tradeOdds) == 0 {
		price := s.bid()
		if sim.rng.IntN(2) == 0 {
			price = s.ask()
		}
		h.HandleTrade(Trade{
			Symbol:    s.name,
			Price:     cents(price),
			Size:      decimal.NewFromInt(1 + sim.rng.Int64N(500)),
			Timestamp: ts,
			Seq:       s.seq,
		})
		return
	}

	// The bid stays at least a cent: with a spread of up to four cents, the
	// midpoint has to stay at three or more.
	s.mid = max(s.mid+sim.rng.Int64N(3)-1, 3)
	s.spread = 1 + sim.rng.Int64N(4)
	h.HandleQuote(Quote{
		Symbol:    s.name,
		Bid:       cents(s.bid()),
		BidSize:   decimal.NewFromInt(100 * (1 + sim.rng.Int64N(10))),
		Ask:       cents(s.ask()),
		AskSize:   decimal.NewFromInt(100 * (1 + sim.rng.Int64N(10))),
		Timestamp: ts,
		Seq:       s.seq,
	})
}

func cents(c int64) decimal.Decimal {
	return decimal.New(c, -2)
}
