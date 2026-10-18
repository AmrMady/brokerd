package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

// RelayConfig configures a Relay.
type RelayConfig struct {
	// URL is the upstream hub's stream, like ws://core:8080/v1/stream.
	URL     string
	Symbols []string
	// Backoff is how long to wait before reconnecting the first time. It
	// doubles after each failed attempt, up to a minute. Default 1s.
	Backoff time.Duration
	// PingInterval is how often the connection is pinged, and how long a
	// ping can go unanswered before it's dropped. Default 30s.
	PingInterval time.Duration
	Logger       *slog.Logger
}

const (
	maxBackoff            = time.Minute
	relayHandshakeTimeout = 10 * time.Second
	maxFrame              = 1 << 20 // up to maxBatch messages
)

// Errors that reconnecting can't fix.
var (
	errNotAHub = errors.New("upstream isn't a brokerd stream")
	errRefused = errors.New("upstream refused the subscription")
)

// Relay is a marketdata.Feed of the trades and quotes from another hub, so
// hubs can be chained: one reads the real feed, and relays closer to the
// clients fan it out further. Bars aren't relayed; build them from the
// relayed trades.
//
// Events keep their upstream sequence numbers, so a Sequencer downstream
// counts what the relay missed while reconnecting, and what the upstream
// hub dropped because the relay fell behind, as gaps. An upstream hub that
// restarts starts its numbers over. The relay sees its new session and
// shifts the new numbers past the ones it already passed on, so they keep
// increasing.
//
// When the connection drops, the relay reconnects with backoff and
// subscribes again. Run must not be called again while it's running.
type Relay struct {
	cfg      RelayConfig
	upstream string            // the upstream hub's session
	offset   map[string]uint64 // added to upstream numbers in this session
	last     map[string]uint64 // the highest number passed on
}

// NewRelay checks cfg and returns a Relay.
func NewRelay(cfg RelayConfig) (*Relay, error) {
	if u, err := url.Parse(cfg.URL); err != nil || (u.Scheme != "ws" && u.Scheme != "wss") {
		return nil, fmt.Errorf("relay URL must be a ws or wss URL, got %q", cfg.URL)
	}
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("relay needs at least one symbol")
	}
	for _, s := range cfg.Symbols {
		if !symbolPattern.MatchString(s) {
			return nil, fmt.Errorf("relay: invalid symbol %q", s)
		}
	}
	if cfg.Backoff < 0 || cfg.PingInterval < 0 {
		return nil, fmt.Errorf("relay backoff and ping interval must not be negative, got %s and %s",
			cfg.Backoff, cfg.PingInterval)
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = time.Second
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Relay{cfg: cfg, offset: map[string]uint64{}, last: map[string]uint64{}}, nil
}

// Run implements marketdata.Feed. It only returns when ctx is done or on an
// error that reconnecting can't fix.
func (r *Relay) Run(ctx context.Context, h marketdata.Handler) error {
	b := backoff{first: r.cfg.Backoff}
	for {
		streamed, err := r.connect(ctx, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errNotAHub) || errors.Is(err, errRefused) {
			return err
		}
		wait := b.after(streamed)
		r.cfg.Logger.Warn("relay disconnected", "error", err, "reconnect_in", wait)
		if err = sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// backoff doubles the wait after each failed attempt to connect, up to
// maxBackoff, and starts over after a connection that got as far as
// streaming.
type backoff struct {
	first, next time.Duration
}

func (b *backoff) after(streamed bool) time.Duration {
	if streamed || b.next == 0 {
		b.next = b.first
	}
	wait := b.next
	b.next = min(2*b.next, maxBackoff)
	return wait
}

// relayMessage has the fields of the messages a relay uses.
type relayMessage struct {
	Type      string          `json:"type"`
	Message   string          `json:"message"`
	Symbol    string          `json:"symbol"`
	Price     decimal.Decimal `json:"price"`
	Size      decimal.Decimal `json:"size"`
	Bid       decimal.Decimal `json:"bid"`
	BidSize   decimal.Decimal `json:"bid_size"`
	Ask       decimal.Decimal `json:"ask"`
	AskSize   decimal.Decimal `json:"ask_size"`
	Timestamp time.Time       `json:"timestamp"`
	Seq       uint64          `json:"seq"`
}

// connect subscribes upstream and passes on events until the connection
// fails. streamed reports whether anything came back after subscribing.
func (r *Relay) connect(ctx context.Context, h marketdata.Handler) (streamed bool, err error) {
	hctx, cancel := context.WithTimeout(ctx, relayHandshakeTimeout)
	defer cancel()
	conn, resp, err := websocket.Dial(hctx, r.cfg.URL, nil)
	if resp != nil && resp.Body != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	if err != nil {
		return false, fmt.Errorf("connect to upstream: %w", err)
	}
	defer r.closeNow(conn)
	conn.SetReadLimit(maxFrame)

	session := resp.Header.Get(sessionHeader)
	if session == "" {
		return false, errNotAHub
	}
	if session != r.upstream {
		if r.upstream != "" {
			r.offset = maps.Clone(r.last)
			r.cfg.Logger.Info("relay upstream restarted", "session", session)
		}
		r.upstream = session
	}

	req, err := json.Marshal(request{
		Action:        "subscribe",
		Subscriptions: Subscriptions{Trades: r.cfg.Symbols, Quotes: r.cfg.Symbols, Bars: []string{}},
	})
	if err != nil {
		return false, err
	}
	if err = conn.Write(hctx, websocket.MessageText, req); err != nil {
		return false, fmt.Errorf("subscribe upstream: %w", err)
	}

	pctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		stop()
		wg.Wait()
	}()
	wg.Go(func() { r.ping(pctx, conn) })
	for {
		_, data, readErr := conn.Read(ctx)
		if readErr != nil {
			return streamed, fmt.Errorf("read from upstream: %w", readErr)
		}
		var msgs []relayMessage
		if err = json.Unmarshal(data, &msgs); err != nil {
			return streamed, fmt.Errorf("%w: %w", errNotAHub, err)
		}
		streamed = true
		for _, m := range msgs {
			switch m.Type {
			case "trade":
				h.HandleTrade(marketdata.Trade{
					Symbol: m.Symbol, Price: m.Price, Size: m.Size, Timestamp: m.Timestamp, Seq: r.seq(m.Symbol, m.Seq),
				})
			case "quote":
				h.HandleQuote(marketdata.Quote{
					Symbol: m.Symbol, Bid: m.Bid, BidSize: m.BidSize, Ask: m.Ask, AskSize: m.AskSize,
					Timestamp: m.Timestamp, Seq: r.seq(m.Symbol, m.Seq),
				})
			case "error":
				return streamed, fmt.Errorf("%w: %s", errRefused, m.Message)
			}
		}
	}
}

// seq shifts an upstream number by the symbol's offset for this session.
func (r *Relay) seq(symbol string, upstream uint64) uint64 {
	n := upstream + r.offset[symbol]
	r.last[symbol] = max(r.last[symbol], n)
	return n
}

// ping drops the connection when a ping goes unanswered, which ends
// connect's read.
func (r *Relay) ping(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(r.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pctx, cancel := context.WithTimeout(ctx, r.cfg.PingInterval)
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.cfg.Logger.Warn("relay upstream stopped answering pings", "error", err)
				r.closeNow(conn)
			}
			return
		}
	}
}

func (r *Relay) closeNow(conn *websocket.Conn) {
	if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
		r.cfg.Logger.Debug("close relay connection", "error", err)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
