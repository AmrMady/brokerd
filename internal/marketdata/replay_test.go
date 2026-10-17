package marketdata_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

const replayHeader = "type,symbol,seq,timestamp,price,size,bid,bid_size,ask,ask_size\n"

func replay(t *testing.T, data string, speed float64) (*recorder, error) {
	t.Helper()
	feed, err := marketdata.NewReplay(strings.NewReader(data), speed)
	if err != nil {
		t.Fatal(err)
	}
	var r recorder
	err = feed.Run(t.Context(), &r)
	return &r, err
}

func TestReplay(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/events.csv")
	if err != nil {
		t.Fatal(err)
	}
	r, err := replay(t, string(data), 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"quote AAPL #1 13:30:00.000 300x227.14 200x227.16",
		"trade AAPL #2 13:30:00.250 100@227.16",
		"quote MSFT #1 13:30:00.500 100x415.2 0x415.25",
		"trade MSFT #2 13:30:01.000 0.5@415.2",
	}
	if got := r.lines(); !slices.Equal(got, want) {
		t.Errorf("events:\n got %q\nwant %q", got, want)
	}
}

func TestReplayRejectsBadInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, data, want string
	}{
		{"no header", "", "read replay header: EOF"},
		{"short header", "type,symbol\n", "read replay header: record on line 1: wrong number of fields"},
		{"wrong header", "kind,symbol,seq,timestamp,price,size,bid,bid_size,ask,ask_size\n", "replay header is"},
		{"wrong field count", replayHeader + "trade,AAPL,1\n", "wrong number of fields"},
		{"unknown type", replayHeader + "bar,AAPL,1,2026-10-16T13:30:00Z,1,1,,,,\n", `line 2: type must be trade or quote, got "bar"`},
		{"no symbol", replayHeader + "trade,,1,2026-10-16T13:30:00Z,1,1,,,,\n", "line 2: symbol is empty"},
		{"bad seq", replayHeader + "trade,AAPL,-1,2026-10-16T13:30:00Z,1,1,,,,\n", "line 2: seq:"},
		{"bad timestamp", replayHeader + "trade,AAPL,1,2026-10-16 13:30,1,1,,,,\n", "line 2: timestamp:"},
		{"zero price", replayHeader + "trade,AAPL,1,2026-10-16T13:30:00Z,0,1,,,,\n", "line 2: price must be positive"},
		{"bad size", replayHeader + "trade,AAPL,1,2026-10-16T13:30:00Z,1,x,,,,\n", "line 2: size:"},
		{"quote fields on a trade", replayHeader + "trade,AAPL,1,2026-10-16T13:30:00Z,1,1,1,1,1,1\n", "line 2: bid must be empty for a trade"},
		{"negative bid size", replayHeader + "quote,AAPL,1,2026-10-16T13:30:00Z,,,1,-1,2,1\n", "line 2: bid_size must not be negative"},
		{"later line", replayHeader + "quote,AAPL,1,2026-10-16T13:30:00Z,,,1,1,2,1\nquote,AAPL,2,2026-10-16T13:30:00Z,,,1,1,,1\n", "line 3: ask:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := replay(t, tt.data, 0)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got error %v, want one containing %q", err, tt.want)
			}
		})
	}
	if _, err := marketdata.NewReplay(strings.NewReader(""), -1); err == nil {
		t.Error("NewReplay accepted a negative speed")
	}
}

func TestReplayPacing(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/events.csv")
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	// The events span one second; at 20 times real time that's 50ms.
	if _, err = replay(t, string(data), 20); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(began); elapsed < 50*time.Millisecond {
		t.Errorf("replay took %s, want at least 50ms", elapsed)
	}
}

func TestReplayStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	data := replayHeader +
		"trade,AAPL,1,2026-10-16T13:30:00Z,1,1,,,,\n" +
		"trade,AAPL,2,2026-10-16T14:30:00Z,1,1,,,,\n"
	feed, err := marketdata.NewReplay(strings.NewReader(data), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	var r recorder
	if err = feed.Run(ctx, &r); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run returned %v, want %v", err, context.DeadlineExceeded)
	}
	if len(r.events) != 1 {
		t.Errorf("got %d events before the cancel, want 1", len(r.events))
	}
}

func FuzzReplay(f *testing.F) {
	seed, readErr := os.ReadFile("testdata/events.csv")
	if readErr != nil {
		f.Fatal(readErr)
	}
	f.Add(seed)
	f.Add([]byte(replayHeader + "trade,AAPL,1,2026-10-16T13:30:00Z,1e3,0.1,,,,\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		feed, err := marketdata.NewReplay(bytes.NewReader(data), 0)
		if err != nil {
			t.Fatal(err)
		}
		var r recorder
		err = feed.Run(t.Context(), &r)
		if err != nil && !strings.Contains(err.Error(), "replay") {
			t.Errorf("error %q doesn't say it came from the replay", err)
		}
		for _, e := range r.events {
			switch e := e.(type) {
			case marketdata.Trade:
				if e.Symbol == "" || !e.Price.IsPositive() || !e.Size.IsPositive() {
					t.Errorf("replayed an invalid trade %+v", e)
				}
			case marketdata.Quote:
				if e.Symbol == "" || !e.Bid.IsPositive() || !e.Ask.IsPositive() || e.BidSize.IsNegative() || e.AskSize.IsNegative() {
					t.Errorf("replayed an invalid quote %+v", e)
				}
			}
		}
	})
}
