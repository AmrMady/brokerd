package marketdata_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func trade(symbol string, seq uint64) marketdata.Trade {
	return marketdata.Trade{Symbol: symbol, Seq: seq}
}

func quote(symbol string, seq uint64) marketdata.Quote {
	return marketdata.Quote{Symbol: symbol, Seq: seq}
}

func TestSequencer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		in    []any
		want  []string
		stats marketdata.SequenceStats
	}{
		{
			name: "in order",
			in:   []any{trade("AAPL", 1), quote("AAPL", 2), trade("AAPL", 3)},
			want: []string{"AAPL 1", "AAPL 2", "AAPL 3"},
		},
		{
			name: "first event starts the sequence",
			in:   []any{trade("AAPL", 5), trade("AAPL", 6)},
			want: []string{"AAPL 5", "AAPL 6"},
		},
		{
			name:  "gap",
			in:    []any{trade("AAPL", 1), trade("AAPL", 2), quote("AAPL", 5), trade("AAPL", 6)},
			want:  []string{"AAPL 1", "AAPL 2", "AAPL 5", "AAPL 6"},
			stats: marketdata.SequenceStats{Gaps: 1, Missed: 2},
		},
		{
			name:  "duplicate",
			in:    []any{trade("AAPL", 1), trade("AAPL", 2), trade("AAPL", 2), trade("AAPL", 3)},
			want:  []string{"AAPL 1", "AAPL 2", "AAPL 3"},
			stats: marketdata.SequenceStats{Stale: 1},
		},
		{
			name:  "a late trade fills its gap",
			in:    []any{trade("AAPL", 1), trade("AAPL", 3), trade("AAPL", 2), trade("AAPL", 4)},
			want:  []string{"AAPL 1", "AAPL 3", "AAPL 2", "AAPL 4"},
			stats: marketdata.SequenceStats{Gaps: 1, Late: 1},
		},
		{
			name:  "a late trade comes once",
			in:    []any{trade("AAPL", 1), trade("AAPL", 3), trade("AAPL", 2), trade("AAPL", 2)},
			want:  []string{"AAPL 1", "AAPL 3", "AAPL 2"},
			stats: marketdata.SequenceStats{Gaps: 1, Late: 1, Stale: 1},
		},
		{
			name:  "a late quote is dropped, since the newest wins",
			in:    []any{quote("AAPL", 1), quote("AAPL", 3), quote("AAPL", 2)},
			want:  []string{"AAPL 1", "AAPL 3"},
			stats: marketdata.SequenceStats{Gaps: 1, Late: 1, Stale: 1},
		},
		{
			name: "only the newest skipped numbers are remembered",
			in:   []any{trade("AAPL", 1), trade("AAPL", 2000), trade("AAPL", 2), trade("AAPL", 1999)},
			want: []string{"AAPL 1", "AAPL 2000", "AAPL 1999"},
			// 2 to 1999 were skipped; 2 is too far back to be remembered.
			stats: marketdata.SequenceStats{Gaps: 1, Missed: 1997, Late: 1, Stale: 1},
		},
		{
			name:  "trades and quotes share a sequence",
			in:    []any{trade("AAPL", 1), quote("AAPL", 2), trade("AAPL", 2)},
			want:  []string{"AAPL 1", "AAPL 2"},
			stats: marketdata.SequenceStats{Stale: 1},
		},
		{
			name:  "symbols are separate",
			in:    []any{trade("AAPL", 1), trade("MSFT", 1), quote("AAPL", 2), quote("MSFT", 3)},
			want:  []string{"AAPL 1", "MSFT 1", "AAPL 2", "MSFT 3"},
			stats: marketdata.SequenceStats{Gaps: 1, Missed: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var r recorder
			s := marketdata.NewSequencer(&r)
			for _, e := range tt.in {
				switch e := e.(type) {
				case marketdata.Trade:
					s.HandleTrade(e)
				case marketdata.Quote:
					s.HandleQuote(e)
				}
			}
			var got []string
			for _, e := range r.events {
				switch e := e.(type) {
				case marketdata.Trade:
					got = append(got, fmt.Sprintf("%s %d", e.Symbol, e.Seq))
				case marketdata.Quote:
					got = append(got, fmt.Sprintf("%s %d", e.Symbol, e.Seq))
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("passed on %q, want %q", got, tt.want)
			}
			tt.stats.Events = uint64(len(tt.want))
			if s.Stats() != tt.stats {
				t.Errorf("stats = %+v, want %+v", s.Stats(), tt.stats)
			}
		})
	}
}

// Stats can be read while the feed runs.
func TestSequencerStatsWhileRunning(t *testing.T) {
	t.Parallel()
	sim, err := marketdata.NewSimulator(marketdata.SimulatorConfig{Symbols: []string{"AAPL"}, Rate: 1000, Start: start, Limit: 10000})
	if err != nil {
		t.Fatal(err)
	}
	s := marketdata.NewSequencer(discard{})
	var wg sync.WaitGroup
	wg.Go(func() {
		if runErr := sim.Run(t.Context(), s); runErr != nil {
			t.Error(runErr)
		}
	})
	for range 100 {
		if stats := s.Stats(); stats.Gaps != 0 || stats.Missed != 0 || stats.Stale != 0 {
			t.Errorf("stats = %+v for a feed in sequence, want no gaps", stats)
		}
	}
	wg.Wait()
}
