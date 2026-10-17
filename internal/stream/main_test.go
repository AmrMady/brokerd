package stream_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/goleak"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var start = time.Date(2026, 10, 16, 13, 30, 0, 0, time.UTC)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// newHub returns a hub that's closed when the test ends.
func newHub(t *testing.T, cfg stream.Config) *stream.Hub {
	t.Helper()
	h, err := stream.NewHub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := h.Close(ctx); closeErr != nil {
			t.Errorf("close hub: %v", closeErr)
		}
	})
	return h
}

func connect(t *testing.T, h *stream.Hub) *stream.Client {
	t.Helper()
	c, err := h.Connect()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// next returns the messages queued for c, waiting up to a second for one.
func next(t *testing.T, c *stream.Client) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	msgs, err := c.Next(ctx, nil, 1000)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	lines := make([]string, len(msgs))
	for i, m := range msgs {
		lines[i] = string(m)
	}
	return lines
}

func wantNothing(t *testing.T, c *stream.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if msgs, err := c.Next(ctx, nil, 1000); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %q, %v, want nothing", msgs, err)
	}
}

func wantMessages(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("messages:\n got %q\nwant %q", got, want)
	}
}

func trade(symbol string, seq uint64) marketdata.Trade {
	return marketdata.Trade{Symbol: symbol, Price: dec("227.16"), Size: dec("100"), Timestamp: start, Seq: seq}
}

func tradeJSON(symbol string, seq uint64) string {
	return `{"type":"trade","symbol":"` + symbol + `","price":"227.16","size":"100","timestamp":"2026-10-16T13:30:00Z","seq":` +
		decimal.NewFromUint64(seq).String() + `}`
}
