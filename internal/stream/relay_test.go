package stream_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

// upstream serves whichever hub is current, so a test can restart it. Its
// first stall connections are held open without being read, so pings to
// them go unanswered, and the bad ones after those get a frame that isn't
// JSON.
type upstream struct {
	t     *testing.T
	cfg   stream.Config
	hub   atomic.Pointer[stream.Hub]
	conns atomic.Int32
	stall int32
	bad   int32
}

func newUpstream(t *testing.T, cfg stream.Config) (*upstream, string) {
	t.Helper()
	u := &upstream{t: t, cfg: cfg}
	u.hub.Store(u.newHub())
	srv := httptest.NewServer(u)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { u.close(u.hub.Load()) })
	return u, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func (u *upstream) newHub() *stream.Hub {
	h, err := stream.NewHub(u.cfg)
	if err != nil {
		u.t.Fatal(err)
	}
	return h
}

func (u *upstream) close(h *stream.Hub) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		u.t.Errorf("close hub: %v", err)
	}
}

// restart replaces the hub with a new one, which has a new session.
func (u *upstream) restart() {
	u.close(u.hub.Swap(u.newHub()))
}

func (u *upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := u.hub.Load()
	n := u.conns.Add(1)
	if n > u.stall+u.bad {
		h.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Stream-Session", h.Session())
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer closeConn(u.t, conn)
	if n > u.stall {
		if err = conn.Write(u.t.Context(), websocket.MessageText, []byte("not json")); err != nil {
			return
		}
	}
	<-u.t.Context().Done()
}

func (u *upstream) waitSubscribed(t *testing.T, n int) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d subscriptions", n), func() bool { return u.hub.Load().Stats().Subscriptions == n })
}

// events records what a relay passes on. It's safe to read while the relay
// runs, and can be made slow.
type events struct {
	pause time.Duration

	mu    sync.Mutex
	lines []string
	seqs  []uint64
}

func (e *events) HandleTrade(t marketdata.Trade) {
	e.add(t.Seq, fmt.Sprintf("trade %s #%d %s %s@%s", t.Symbol, t.Seq, t.Timestamp.Format(time.RFC3339Nano), t.Size, t.Price))
}

func (e *events) HandleQuote(q marketdata.Quote) {
	e.add(q.Seq, fmt.Sprintf("quote %s #%d %s %sx%s %sx%s", q.Symbol, q.Seq, q.Timestamp.Format(time.RFC3339Nano),
		q.BidSize, q.Bid, q.AskSize, q.Ask))
}

func (e *events) add(seq uint64, line string) {
	time.Sleep(e.pause)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lines = append(e.lines, line)
	e.seqs = append(e.seqs, seq)
}

// wait returns the lines once there are n of them.
func (e *events) wait(t *testing.T, n int) []string {
	t.Helper()
	var lines []string
	eventually(t, fmt.Sprintf("%d events", n), func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		lines = slices.Clone(e.lines)
		return len(lines) >= n
	})
	return lines
}

func newRelay(t *testing.T, cfg stream.RelayConfig) *stream.Relay {
	t.Helper()
	if cfg.Backoff == 0 {
		cfg.Backoff = 10 * time.Millisecond
	}
	r, err := stream.NewRelay(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// runRelay runs r until the test ends.
func runRelay(t *testing.T, r *stream.Relay, h marketdata.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, h) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("relay stopped with %v, want %v", err, context.Canceled)
		}
	})
}

func TestRelayPassesEventsOn(t *testing.T) {
	t.Parallel()
	u, url := newUpstream(t, stream.Config{})
	var got events
	runRelay(t, newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL", "MSFT"}}), &got)
	u.waitSubscribed(t, 4)

	h := u.hub.Load()
	h.HandleQuote(marketdata.Quote{
		Symbol: "AAPL", Bid: dec("227.15"), BidSize: dec("300"), Ask: dec("227.17"), AskSize: dec("200"),
		Timestamp: start.Add(123456789), Seq: 5,
	})
	h.HandleTrade(trade("AAPL", 6))
	h.HandleBar(marketdata.Bar{Symbol: "AAPL", Start: start, Interval: time.Second})
	h.HandleTrade(trade("NVDA", 1))
	h.HandleTrade(marketdata.Trade{Symbol: "MSFT", Price: dec("12345678.123456789"), Size: dec("0.5"), Timestamp: start, Seq: 1})
	wantMessages(t, got.wait(t, 3),
		"quote AAPL #5 2026-10-16T13:30:00.123456789Z 300x227.15 200x227.17",
		"trade AAPL #6 2026-10-16T13:30:00Z 100@227.16",
		"trade MSFT #1 2026-10-16T13:30:00Z 0.5@12345678.123456789",
	)
}

// When the upstream hub restarts, its numbers start over, and the relay
// shifts them past the ones it passed on before.
func TestRelayFollowsUpstreamRestart(t *testing.T) {
	t.Parallel()
	u, url := newUpstream(t, stream.Config{})
	var got events
	runRelay(t, newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL", "MSFT"}}), &got)
	u.waitSubscribed(t, 4)
	for seq := range uint64(3) {
		u.hub.Load().HandleTrade(trade("AAPL", seq+1))
	}
	got.wait(t, 3)

	u.restart()
	u.waitSubscribed(t, 4)
	h := u.hub.Load()
	h.HandleTrade(trade("AAPL", 1))
	h.HandleTrade(trade("AAPL", 2))
	h.HandleQuote(marketdata.Quote{Symbol: "AAPL", Timestamp: start, Seq: 3})
	h.HandleTrade(trade("MSFT", 1))
	lines := got.wait(t, 7)
	got.mu.Lock()
	defer got.mu.Unlock()
	if want := []uint64{1, 2, 3, 4, 5, 6, 1}; !slices.Equal(got.seqs, want) {
		t.Errorf("relayed seqs %v, want %v\n%q", got.seqs, want, lines)
	}
}

// What the upstream hub drops because the relay falls behind shows up
// downstream as gaps, not as anything stale.
func TestRelayKeepsUpstreamGaps(t *testing.T) {
	t.Parallel()
	u, url := newUpstream(t, stream.Config{Buffer: 4, MaxLag: time.Hour})
	got := events{pause: time.Millisecond}
	seq := marketdata.NewSequencer(&got)
	runRelay(t, newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL"}}), seq)
	u.waitSubscribed(t, 2)

	// Once the first trade is through, a burst overflows the relay's queue
	// upstream. The first and last trades always arrive, so whatever is
	// dropped in between is a gap.
	const published = 500
	u.hub.Load().HandleTrade(trade("AAPL", 1))
	got.wait(t, 1)
	for n := uint64(2); n <= published; n++ {
		u.hub.Load().HandleTrade(trade("AAPL", n))
	}
	eventually(t, "the last trade", func() bool {
		got.mu.Lock()
		defer got.mu.Unlock()
		return len(got.seqs) > 0 && got.seqs[len(got.seqs)-1] == published
	})

	got.mu.Lock()
	defer got.mu.Unlock()
	if len(got.seqs) == published || !slices.IsSorted(got.seqs) {
		t.Errorf("relayed %d of %d trades, sorted %t; want fewer, in order", len(got.seqs), published, slices.IsSorted(got.seqs))
	}
	s := seq.Stats()
	if missed := uint64(published - len(got.seqs)); s.Gaps == 0 || s.Stale != 0 || s.Missed != missed {
		t.Errorf("sequencer stats %+v, want %d missed, as gaps", s, missed)
	}
	if d := u.hub.Load().Stats().Dropped; d == 0 {
		t.Error("the upstream hub dropped nothing")
	}
}

func TestRelayReconnectsWhenPingsGoUnanswered(t *testing.T) {
	t.Parallel()
	u, url := newUpstream(t, stream.Config{})
	u.stall = 1
	var got events
	runRelay(t, newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL"}, PingInterval: 20 * time.Millisecond}), &got)
	u.waitSubscribed(t, 2)
	u.hub.Load().HandleTrade(trade("AAPL", 1))
	got.wait(t, 1)
	if n := u.conns.Load(); n != 2 {
		t.Errorf("connected %d times, want 2", n)
	}
}

// A frame the relay can't read is that connection's fault, so it
// reconnects rather than giving up.
func TestRelayReconnectsAfterABadFrame(t *testing.T) {
	t.Parallel()
	u, url := newUpstream(t, stream.Config{})
	u.bad = 1
	var got events
	runRelay(t, newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL"}}), &got)
	u.waitSubscribed(t, 2)
	u.hub.Load().HandleTrade(trade("AAPL", 1))
	got.wait(t, 1)
	if n := u.conns.Load(); n != 2 {
		t.Errorf("connected %d times, want 2", n)
	}
}

// Errors that reconnecting can't fix end Run.
func TestRelayGivesUp(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, url string) error {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		return newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL"}}).Run(ctx, &events{})
	}
	t.Run("not a hub", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			closeConn(t, conn)
		}))
		defer srv.Close()
		if err := run(t, "ws"+strings.TrimPrefix(srv.URL, "http")); err == nil || !strings.Contains(err.Error(), "isn't a brokerd stream") {
			t.Errorf("Run returned %v, want not a brokerd stream", err)
		}
	})
	t.Run("subscription refused", func(t *testing.T) {
		t.Parallel()
		_, url := newUpstream(t, stream.Config{MaxSubscriptions: 1})
		if err := run(t, url); err == nil || !strings.Contains(err.Error(), "more than the limit") {
			t.Errorf("Run returned %v, want the subscription refused", err)
		}
	})
}

// Run stops when canceled, including while it waits to reconnect.
func TestRelayStopsWhenCanceled(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	srv.Close()
	r := newRelay(t, stream.RelayConfig{URL: url, Symbols: []string{"AAPL"}, Backoff: time.Hour})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, &events{}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run returned %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run didn't stop when its context was done")
	}
}

func TestNewRelayRejectsBadConfig(t *testing.T) {
	t.Parallel()
	for _, cfg := range []stream.RelayConfig{
		{Symbols: []string{"AAPL"}},
		{URL: "http://core:8080/v1/stream", Symbols: []string{"AAPL"}},
		{URL: "ws://core:8080/v1/stream"},
		{URL: "ws://core:8080/v1/stream", Symbols: []string{"aapl"}},
		{URL: "ws://core:8080/v1/stream", Symbols: []string{"AAPL"}, Backoff: -1},
	} {
		if _, err := stream.NewRelay(cfg); err == nil {
			t.Errorf("NewRelay(%+v) succeeded, want an error", cfg)
		}
	}
}
