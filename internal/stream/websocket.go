package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Messages per WebSocket frame. A frame is a JSON array of messages.
const maxBatch = 256

// errLagged is the cause of a write or ping that took longer than MaxLag.
// When one times out the connection is closed, and the reader can see that
// and cancel the stream's context first, so the timeout is told apart by its
// cause rather than by which context is done.
var errLagged = errors.New("client took too long")

// ServeHTTP streams events over a WebSocket. The client sends
//
//	{"action": "subscribe", "trades": ["AAPL"], "quotes": ["AAPL"], "bars": ["AAPL"]}
//
// or "unsubscribe" with the same fields, and gets back frames that are JSON
// arrays of messages: a "subscription" message listing its subscriptions
// after each request, an "error" message for a bad one, and "trade",
// "quote" and "bar" messages.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := h.Connect()
	if err != nil {
		http.Error(w, "stream is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer c.Close()
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has replied
	}

	// The reader and pinger stop the writer, through ctx or the client,
	// when the connection fails. The writer closes the connection when it
	// stops, which stops the reader; ctx stops the pinger.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		defer cancel()
		readRequests(r.Context(), conn, c)
	})
	wg.Go(func() {
		h.ping(ctx, conn, c)
	})

	// A client cut off for being too slow gets no close handshake: it can
	// take 10s, and the client couldn't keep up with the stream anyway.
	if err = h.writeEvents(ctx, conn, c); errors.Is(err, ErrHubClosed) {
		err = conn.Close(websocket.StatusGoingAway, "shutting down")
	} else {
		err = conn.CloseNow()
	}
	if err != nil {
		h.cfg.Logger.Debug("close stream connection", "error", err)
	}
	cancel()
	wg.Wait()
}

func (h *Hub) writeEvents(ctx context.Context, conn *websocket.Conn, c *Client) error {
	var (
		msgs  [][]byte
		frame []byte
		err   error
	)
	for {
		if msgs, err = c.Next(ctx, msgs[:0], maxBatch); err != nil {
			return err
		}
		frame = append(frame[:0], '[')
		frame = append(frame, bytes.Join(msgs, []byte{','})...)
		frame = append(frame, ']')

		wctx, cancel := context.WithTimeoutCause(ctx, h.cfg.MaxLag, errLagged)
		err = conn.Write(wctx, websocket.MessageText, frame)
		lagged := errors.Is(context.Cause(wctx), errLagged)
		cancel()
		if lagged {
			c.disconnect(ErrTooSlow)
			return ErrTooSlow
		}
		if err != nil {
			return err
		}
	}
}

// ping disconnects the client if it doesn't answer a ping within MaxLag.
func (h *Hub) ping(ctx context.Context, conn *websocket.Conn, c *Client) {
	ticker := time.NewTicker(h.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pctx, cancel := context.WithTimeoutCause(ctx, h.cfg.MaxLag, errLagged)
		err := conn.Ping(pctx)
		lagged := errors.Is(context.Cause(pctx), errLagged)
		cancel()
		if lagged {
			c.disconnect(ErrTooSlow)
			return
		}
		if err != nil {
			return
		}
	}
}

func readRequests(ctx context.Context, conn *websocket.Conn, c *Client) {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var req request
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err = dec.Decode(&req); err != nil {
			c.reply(fmt.Errorf("invalid request: %w", err))
			continue
		}
		switch req.Action {
		case "subscribe":
			err = c.Subscribe(req.Subscriptions)
		case "unsubscribe":
			err = c.Unsubscribe(req.Subscriptions)
		default:
			err = fmt.Errorf("action must be subscribe or unsubscribe, got %q", req.Action)
		}
		if err != nil {
			c.reply(err)
		}
	}
}

// reply queues an error message for the client.
func (c *Client) reply(err error) {
	if msg, ok := encode(errorMessage{Type: "error", Message: err.Error()}); ok {
		c.push(msg)
	}
}
