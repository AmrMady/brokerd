// Package stream fans market data out to WebSocket clients.
package stream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

// Errors that end a client's stream.
var (
	ErrTooSlow      = errors.New("client fell too far behind")
	ErrHubClosed    = errors.New("hub is closed")
	ErrClientClosed = errors.New("client is closed")
)

// Config configures a Hub. Zero fields get defaults.
type Config struct {
	// Buffer is how many messages are queued for each client. When a
	// client's queue is full, its oldest message is dropped. Default 1024.
	Buffer int
	// MaxLag is how long a client can keep dropping messages, wait on one
	// write, or take to answer a ping before it's disconnected. Default 5s.
	MaxLag time.Duration
	// PingInterval is how often each WebSocket connection is pinged.
	// Default 30s.
	PingInterval time.Duration
	// MaxSubscriptions is how many channel and symbol pairs a client can
	// subscribe to. Default 1000.
	MaxSubscriptions int
	// Logger gets a line for each client disconnected for being too slow.
	// Default: no logging.
	Logger *slog.Logger
}

func (c Config) withDefaults() (Config, error) {
	if c.Buffer < 0 || c.MaxLag < 0 || c.PingInterval < 0 || c.MaxSubscriptions < 0 {
		return c, fmt.Errorf("stream config must not be negative: %+v", c)
	}
	if c.Buffer == 0 {
		c.Buffer = 1024
	}
	if c.MaxLag == 0 {
		c.MaxLag = 5 * time.Second
	}
	if c.PingInterval == 0 {
		c.PingInterval = 30 * time.Second
	}
	if c.MaxSubscriptions == 0 {
		c.MaxSubscriptions = 1000
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// Channel is a kind of event a client can subscribe to.
type Channel string

// Channels.
const (
	Trades Channel = "trades"
	Quotes Channel = "quotes"
	Bars   Channel = "bars"
)

type topic struct {
	channel Channel
	symbol  string
}

// Hub sends market data events to the clients subscribed to them. It's a
// marketdata.Handler and BarHandler, called on the feed's goroutine, and it
// never blocks it: each client has its own bounded queue, and a client that
// can't keep up loses its oldest messages and is then disconnected.
//
// Each event is encoded once, however many clients get it. Publishing takes
// no lock: each topic's subscribers are an immutable slice, which a
// subscription change copies and swaps in, so clients changing their
// subscriptions can't hold up the feed.
type Hub struct {
	cfg     Config
	session string

	subscribers sync.Map // topic to []*Client; written under mu

	mu            sync.Mutex
	clients       map[*Client]struct{}
	subscriptions int
	closed        bool
	active        sync.WaitGroup // clients not yet closed

	dropped, disconnected atomic.Uint64
}

// Stats counts what a Hub has done.
type Stats struct {
	Clients       int
	Subscriptions int    // channel and symbol pairs, across clients
	Dropped       uint64 // messages dropped from full client queues
	Disconnected  uint64 // clients disconnected for being too slow
}

// NewHub returns a Hub with the given config.
func NewHub(cfg Config) (*Hub, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Hub{
		cfg:     cfg,
		session: rand.Text(),
		clients: map[*Client]struct{}{},
	}, nil
}

// Session identifies this hub for as long as it runs. Events from a hub keep
// their feed's sequence numbers, which start over when the hub's process
// does, so numbers from different sessions can't be compared.
func (h *Hub) Session() string {
	return h.session
}

// HandleTrade implements marketdata.Handler.
func (h *Hub) HandleTrade(t marketdata.Trade) {
	publish(h, topic{Trades, t.Symbol}, tradeMessage{
		Type: "trade", Symbol: t.Symbol, Price: t.Price, Size: t.Size, Timestamp: t.Timestamp, Seq: t.Seq,
	})
}

// HandleQuote implements marketdata.Handler.
func (h *Hub) HandleQuote(q marketdata.Quote) {
	publish(h, topic{Quotes, q.Symbol}, quoteMessage{
		Type: "quote", Symbol: q.Symbol, Bid: q.Bid, BidSize: q.BidSize, Ask: q.Ask, AskSize: q.AskSize,
		Timestamp: q.Timestamp, Seq: q.Seq,
	})
}

// HandleBar implements marketdata.BarHandler.
func (h *Hub) HandleBar(b marketdata.Bar) {
	publish(h, topic{Bars, b.Symbol}, barMessage{
		Type: "bar", Symbol: b.Symbol, Timeframe: timeframe(b.Interval), Start: b.Start,
		Open: b.Open, High: b.High, Low: b.Low, Close: b.Close, Volume: b.Volume, Trades: b.Trades,
	})
}

func publish[M any](h *Hub, t topic, m M) {
	clients := h.subscribersOf(t)
	if len(clients) == 0 {
		return
	}
	msg, ok := encode(m)
	if !ok {
		return
	}
	for _, c := range clients {
		c.push(msg)
	}
}

func (h *Hub) subscribersOf(t topic) []*Client {
	v, ok := h.subscribers.Load(t)
	if !ok {
		return nil
	}
	clients, ok := v.([]*Client)
	if !ok {
		return nil
	}
	return clients
}

// addSubscriber is called with mu held.
func (h *Hub) addSubscriber(t topic, c *Client) {
	old := h.subscribersOf(t)
	clients := make([]*Client, len(old), len(old)+1)
	copy(clients, old)
	h.subscribers.Store(t, append(clients, c))
	h.subscriptions++
}

// removeSubscriber is called with mu held.
func (h *Hub) removeSubscriber(t topic, c *Client) {
	clients := slices.DeleteFunc(slices.Clone(h.subscribersOf(t)), func(s *Client) bool { return s == c })
	if len(clients) == 0 {
		h.subscribers.Delete(t)
	} else {
		h.subscribers.Store(t, clients)
	}
	h.subscriptions--
}

// Connect adds a client with no subscriptions. The caller must Close it.
func (h *Hub) Connect() (*Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	c := &Client{
		hub:   h,
		subs:  map[topic]struct{}{},
		queue: make([][]byte, h.cfg.Buffer),
		ready: make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
	h.clients[c] = struct{}{}
	h.active.Add(1)
	return c, nil
}

// Close disconnects every client, refuses new ones, and waits until each
// client has been closed or ctx is done.
func (h *Hub) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	for c := range h.clients {
		c.disconnect(ErrHubClosed)
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() {
		h.active.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns the counts so far.
func (h *Hub) Stats() Stats {
	h.mu.Lock()
	clients, subscriptions := len(h.clients), h.subscriptions
	h.mu.Unlock()
	return Stats{
		Clients: clients, Subscriptions: subscriptions,
		Dropped: h.dropped.Load(), Disconnected: h.disconnected.Load(),
	}
}

// Client is one subscriber. Its queue is a ring buffer that never grows:
// pushing to a full queue drops the oldest message. A client that has been
// dropping messages for MaxLag, without catching up in between, is
// disconnected.
type Client struct {
	hub *Hub

	subMu sync.Mutex // held through each subscription change
	subs  map[topic]struct{}

	mu          sync.Mutex
	queue       [][]byte
	head, n     int
	behindSince time.Time // when it started dropping; zero once it catches up
	dropped     uint64
	err         error // why it was disconnected
	closeOnce   sync.Once

	ready chan struct{} // gets a value when a message is queued
	done  chan struct{} // closed when the client is disconnected
}

// Subscribe adds subscriptions, and queues a message listing all of them.
// No event for a new subscription is queued before that message: it's
// queued before the hub publishes to the client.
func (c *Client) Subscribe(s Subscriptions) error {
	topics, err := s.topics()
	if err != nil {
		return err
	}
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if err = c.disconnected(); err != nil {
		return err
	}
	added := map[topic]struct{}{}
	for _, t := range topics {
		if _, ok := c.subs[t]; !ok {
			added[t] = struct{}{}
		}
	}
	h := c.hub
	if total := len(c.subs) + len(added); total > h.cfg.MaxSubscriptions {
		return fmt.Errorf("%d subscriptions is more than the limit of %d", total, h.cfg.MaxSubscriptions)
	}
	for t := range added {
		c.subs[t] = struct{}{}
	}
	c.pushSubscriptions()
	if len(added) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for t := range added {
		h.addSubscriber(t, c)
	}
	return nil
}

// Unsubscribe removes subscriptions, and queues a message listing the ones
// left. An event the hub was already publishing can still follow it.
func (c *Client) Unsubscribe(s Subscriptions) error {
	topics, err := s.topics()
	if err != nil {
		return err
	}
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if err = c.disconnected(); err != nil {
		return err
	}
	var removed []topic
	for _, t := range topics {
		if _, ok := c.subs[t]; ok {
			delete(c.subs, t)
			removed = append(removed, t)
		}
	}
	if len(removed) > 0 {
		h := c.hub
		h.mu.Lock()
		for _, t := range removed {
			h.removeSubscriber(t, c)
		}
		h.mu.Unlock()
	}
	c.pushSubscriptions()
	return nil
}

// pushSubscriptions is called with subMu held.
func (c *Client) pushSubscriptions() {
	m := subscriptionMessage{Type: "subscription"}
	for _, t := range slices.SortedFunc(maps.Keys(c.subs), func(a, b topic) int {
		return strings.Compare(a.symbol, b.symbol)
	}) {
		switch t.channel {
		case Trades:
			m.Trades = append(m.Trades, t.symbol)
		case Quotes:
			m.Quotes = append(m.Quotes, t.symbol)
		case Bars:
			m.Bars = append(m.Bars, t.symbol)
		}
	}
	m.Trades, m.Quotes, m.Bars = orEmpty(m.Trades), orEmpty(m.Quotes), orEmpty(m.Bars)
	if msg, ok := encode(m); ok {
		c.push(msg)
	}
}

// Next waits for at least one message and appends up to limit queued
// messages to buf. Once the client is disconnected it returns why: ErrTooSlow,
// ErrHubClosed or ErrClientClosed.
func (c *Client) Next(ctx context.Context, buf [][]byte, limit int) ([][]byte, error) {
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return buf, err
		}
		took := 0
		for ; c.n > 0 && took < limit; took++ {
			buf = append(buf, c.queue[c.head])
			c.queue[c.head] = nil
			c.head = (c.head + 1) % len(c.queue)
			c.n--
		}
		if c.n == 0 {
			c.behindSince = time.Time{}
		}
		c.mu.Unlock()
		if took > 0 {
			return buf, nil
		}

		select {
		case <-c.ready:
		case <-c.done:
		case <-ctx.Done():
			return buf, ctx.Err()
		}
	}
}

// Dropped returns how many of the client's messages were dropped.
func (c *Client) Dropped() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped
}

// Close removes the client from the hub.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		h := c.hub
		c.subMu.Lock()
		h.mu.Lock()
		for t := range c.subs {
			h.removeSubscriber(t, c)
		}
		clear(c.subs)
		delete(h.clients, c)
		h.mu.Unlock()
		c.subMu.Unlock()
		c.disconnect(ErrClientClosed)
		h.active.Done()
	})
}

// push queues msg. It never blocks: it's called on the feed's goroutine.
func (c *Client) push(msg []byte) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	tooSlow := false
	if c.n == len(c.queue) {
		c.queue[c.head] = nil
		c.head = (c.head + 1) % len(c.queue)
		c.n--
		c.dropped++
		c.hub.dropped.Add(1)
		now := time.Now()
		if c.behindSince.IsZero() {
			c.behindSince = now
		} else if now.Sub(c.behindSince) >= c.hub.cfg.MaxLag {
			tooSlow = true
		}
	}
	c.queue[(c.head+c.n)%len(c.queue)] = msg
	c.n++
	c.mu.Unlock()

	if tooSlow {
		c.disconnect(ErrTooSlow)
		return
	}
	select {
	case c.ready <- struct{}{}:
	default:
	}
}

// disconnect ends the client's stream, once.
func (c *Client) disconnect(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	c.queue, c.head, c.n = nil, 0, 0
	dropped := c.dropped
	close(c.done)
	c.mu.Unlock()

	if errors.Is(err, ErrTooSlow) {
		c.hub.disconnected.Add(1)
		c.hub.cfg.Logger.Warn("stream client disconnected", "reason", err, "dropped", dropped)
	}
}

func (c *Client) disconnected() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
