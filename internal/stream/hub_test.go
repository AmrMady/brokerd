package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

func TestHubSendsSubscribedEvents(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{})
	c := connect(t, h)
	other := connect(t, h)
	h.HandleTrade(trade("AAPL", 1)) // no one is subscribed yet

	err := c.Subscribe(stream.Subscriptions{Trades: []string{"AAPL"}, Quotes: []string{"MSFT"}, Bars: []string{"AAPL"}})
	if err != nil {
		t.Fatal(err)
	}
	h.HandleTrade(trade("AAPL", 2))
	h.HandleTrade(trade("MSFT", 1))
	h.HandleQuote(marketdata.Quote{
		Symbol: "MSFT", Bid: dec("415.20"), BidSize: dec("100"), Ask: dec("415.25"), AskSize: dec("0"), Timestamp: start, Seq: 2,
	})
	h.HandleBar(marketdata.Bar{
		Symbol: "AAPL", Start: start, Interval: time.Minute,
		Open: dec("10"), High: dec("12"), Low: dec("9"), Close: dec("11"), Volume: dec("7.5"), Trades: 4,
	})
	h.HandleBar(marketdata.Bar{Symbol: "MSFT", Start: start, Interval: time.Second})

	wantMessages(t, next(t, c),
		`{"type":"subscription","trades":["AAPL"],"quotes":["MSFT"],"bars":["AAPL"]}`,
		tradeJSON("AAPL", 2),
		`{"type":"quote","symbol":"MSFT","bid":"415.2","bid_size":"100","ask":"415.25","ask_size":"0","timestamp":"2026-10-16T13:30:00Z","seq":2}`,
		`{"type":"bar","symbol":"AAPL","timeframe":"1m","start":"2026-10-16T13:30:00Z","open":"10","high":"12","low":"9","close":"11","volume":"7.5","trades":4}`,
	)
	wantNothing(t, other)
	if s := h.Stats(); s.Subscriptions != 3 {
		t.Errorf("stats = %+v, want 3 subscriptions", s)
	}
}

// Clients spamming subscription changes don't hold up the feed.
func TestSubscriptionSpamDoesNotDelayPublishing(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{Buffer: 16, MaxSubscriptions: 5000})
	reader := connect(t, h)
	if err := reader.Subscribe(stream.Subscriptions{Quotes: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	var many stream.Subscriptions
	for i := range 5000 {
		many.Trades = append(many.Trades, fmt.Sprintf("S%d", i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		spammer := connect(t, h)
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if spammer.Subscribe(many) != nil || spammer.Unsubscribe(many) != nil {
					return
				}
			}
		})
	}
	const n = 2000
	var total, worst time.Duration
	for i := range n {
		began := time.Now()
		h.HandleQuote(marketdata.Quote{Symbol: "AAPL", Seq: uint64(i + 1)})
		took := time.Since(began)
		total += took
		worst = max(worst, took)
	}
	close(stop)
	wg.Wait()
	if avg := total / n; avg > time.Millisecond || worst > 50*time.Millisecond {
		t.Errorf("publishing took %s on average and %s at worst while clients changed subscriptions", avg, worst)
	}
}

func TestHubUnsubscribe(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{})
	c := connect(t, h)
	aapl := stream.Subscriptions{Trades: []string{"AAPL"}}
	for range 2 {
		if err := c.Subscribe(aapl); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Subscribe(stream.Subscriptions{Trades: []string{"MSFT"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Unsubscribe(aapl); err != nil {
		t.Fatal(err)
	}
	h.HandleTrade(trade("AAPL", 1))
	h.HandleTrade(trade("MSFT", 1))
	wantMessages(t, next(t, c),
		`{"type":"subscription","trades":["AAPL"],"quotes":[],"bars":[]}`,
		`{"type":"subscription","trades":["AAPL"],"quotes":[],"bars":[]}`,
		`{"type":"subscription","trades":["AAPL","MSFT"],"quotes":[],"bars":[]}`,
		`{"type":"subscription","trades":["MSFT"],"quotes":[],"bars":[]}`,
		tradeJSON("MSFT", 1),
	)
	if s := h.Stats(); s.Clients != 1 || s.Subscriptions != 1 {
		t.Errorf("stats = %+v, want 1 client with 1 subscription", s)
	}
}

func TestHubRejectsBadSubscriptions(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{MaxSubscriptions: 2})
	c := connect(t, h)
	for _, s := range []stream.Subscriptions{
		{Trades: []string{"aapl"}},
		{Quotes: []string{""}},
		{Bars: []string{"AAPL", "TOO-LONG-A-SYMBOL"}},
		{Trades: []string{"AAPL", "MSFT"}, Quotes: []string{"AAPL"}},
	} {
		if err := c.Subscribe(s); err == nil {
			t.Errorf("Subscribe(%+v) succeeded, want an error", s)
		}
	}
	wantNothing(t, c)
}

func TestSlowClientLosesOldestMessages(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{Buffer: 4, MaxLag: time.Hour})
	c := connect(t, h)
	if err := c.Subscribe(stream.Subscriptions{Trades: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	for seq := range uint64(10) {
		h.HandleTrade(trade("AAPL", seq+1))
	}
	wantMessages(t, next(t, c), tradeJSON("AAPL", 7), tradeJSON("AAPL", 8), tradeJSON("AAPL", 9), tradeJSON("AAPL", 10))
	// The subscription message and six trades were dropped.
	if c.Dropped() != 7 || h.Stats().Dropped != 7 {
		t.Errorf("dropped %d, hub stats %+v, want 7", c.Dropped(), h.Stats())
	}
	h.HandleTrade(trade("AAPL", 11))
	wantMessages(t, next(t, c), tradeJSON("AAPL", 11))
}

// Catching up, by emptying the queue, starts the MaxLag clock over.
func TestClientThatCatchesUpStaysConnected(t *testing.T) {
	t.Parallel()
	const maxLag = 50 * time.Millisecond
	h := newHub(t, stream.Config{Buffer: 2, MaxLag: maxLag})
	c := connect(t, h)
	if err := c.Subscribe(stream.Subscriptions{Trades: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}
	h.HandleTrade(trade("AAPL", 1))
	h.HandleTrade(trade("AAPL", 2)) // drops the subscription message
	wantMessages(t, next(t, c), tradeJSON("AAPL", 1), tradeJSON("AAPL", 2))
	time.Sleep(2 * maxLag)
	h.HandleTrade(trade("AAPL", 3))
	h.HandleTrade(trade("AAPL", 4))
	h.HandleTrade(trade("AAPL", 5)) // drops seq 3
	wantMessages(t, next(t, c), tradeJSON("AAPL", 4), tradeJSON("AAPL", 5))
	if s := h.Stats(); s.Disconnected != 0 || s.Dropped != 2 {
		t.Errorf("stats = %+v, want 2 dropped and no one disconnected", s)
	}
}

// A client that stops reading is disconnected once it has been dropping
// messages for MaxLag, and doesn't hold up a client that keeps reading.
func TestSlowClientIsDisconnected(t *testing.T) {
	t.Parallel()
	const maxLag = 50 * time.Millisecond
	h := newHub(t, stream.Config{Buffer: 64, MaxLag: maxLag})
	slow, fast := connect(t, h), connect(t, h)
	for _, c := range []*stream.Client{slow, fast} {
		if err := c.Subscribe(stream.Subscriptions{Trades: []string{"AAPL"}}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		wg       sync.WaitGroup
		received atomic.Int64
	)
	ctx, cancel := context.WithCancel(t.Context())
	wg.Go(func() {
		var msgs [][]byte
		for {
			var err error
			if msgs, err = fast.Next(ctx, msgs[:0], 1000); err != nil {
				return
			}
			received.Add(int64(len(msgs)))
		}
	})

	began := time.Now()
	published := 0
	for h.Stats().Disconnected == 0 && time.Since(began) < 5*time.Second {
		published++
		h.HandleTrade(trade("AAPL", uint64(published)))
		time.Sleep(time.Millisecond)
	}
	took := time.Since(began)

	if _, err := slow.Next(t.Context(), nil, 1); !errors.Is(err, stream.ErrTooSlow) {
		t.Errorf("slow client: Next returned %v, want %v", err, stream.ErrTooSlow)
	}
	if took < maxLag {
		t.Errorf("slow client was disconnected after %s, want at least %s", took, maxLag)
	}
	deadline := time.Now().Add(time.Second)
	for received.Load() < int64(published)+1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	if got := received.Load(); got != int64(published)+1 || fast.Dropped() != 0 {
		t.Errorf("fast client got %d messages and dropped %d, want all %d", got, fast.Dropped(), published+1)
	}
}

func TestHubClose(t *testing.T) {
	t.Parallel()
	h, err := stream.NewHub(stream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Connect()
	if err != nil {
		t.Fatal(err)
	}
	nextErr := make(chan error, 1)
	go func() {
		_, nErr := c.Next(context.Background(), nil, 1)
		nextErr <- nErr
	}()

	closed := make(chan error, 1)
	go func() { closed <- h.Close(context.Background()) }()
	if err = <-nextErr; !errors.Is(err, stream.ErrHubClosed) {
		t.Errorf("Next returned %v, want %v", err, stream.ErrHubClosed)
	}
	if _, err = h.Connect(); !errors.Is(err, stream.ErrHubClosed) {
		t.Errorf("Connect after Close returned %v, want %v", err, stream.ErrHubClosed)
	}
	select {
	case err = <-closed:
		t.Fatalf("Close returned %v before the client was closed", err)
	case <-time.After(20 * time.Millisecond):
	}
	c.Close()
	if err = <-closed; err != nil {
		t.Errorf("Close returned %v", err)
	}
}

func TestHubCloseGivesUp(t *testing.T) {
	t.Parallel()
	h, err := stream.NewHub(stream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err = h.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close with a client still open returned %v, want %v", err, context.DeadlineExceeded)
	}
}

// Clients subscribe, unsubscribe and read while events are published. Each
// sees its events in order, with gaps only for what it dropped.
func TestHubConcurrentUse(t *testing.T) {
	t.Parallel()
	h := newHub(t, stream.Config{Buffer: 16, MaxLag: time.Hour})
	symbols := []string{"AAPL", "MSFT", "NVDA", "AMZN"}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		for seq := uint64(1); ctx.Err() == nil; seq++ {
			for _, s := range symbols {
				h.HandleTrade(trade(s, seq))
			}
		}
	})
	for i := range 8 {
		c := connect(t, h)
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 0))
			last := map[string]uint64{}
			var msgs [][]byte
			for ctx.Err() == nil {
				s := stream.Subscriptions{Trades: []string{symbols[rng.IntN(len(symbols))]}}
				var err error
				if rng.IntN(3) == 0 {
					err = c.Unsubscribe(s)
				} else {
					err = c.Subscribe(s)
				}
				if err != nil {
					t.Error(err)
					return
				}
				if msgs, err = c.Next(ctx, msgs[:0], 100); err != nil {
					return
				}
				for _, m := range msgs {
					var e struct {
						Type, Symbol string
						Seq          uint64
					}
					if err = json.Unmarshal(m, &e); err != nil {
						t.Error(err)
						return
					}
					if e.Type != "trade" {
						continue
					}
					if e.Seq <= last[e.Symbol] {
						t.Errorf("client %d: %s seq %d after %d", i, e.Symbol, e.Seq, last[e.Symbol])
						return
					}
					last[e.Symbol] = e.Seq
				}
			}
		})
	}
	wg.Wait()
}
