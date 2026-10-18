package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/AmrMady/brokerd/internal/stream"
)

// serve returns a hub served over HTTP. The hub is closed before the server
// when the test ends.
func serve(t *testing.T, cfg stream.Config) (*stream.Hub, string) {
	t.Helper()
	h, err := stream.NewHub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if closeErr := h.Close(ctx); closeErr != nil {
			t.Errorf("close hub: %v", closeErr)
		}
	})
	return h, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.Dial(t.Context(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Body != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	t.Cleanup(func() {
		if closeErr := conn.CloseNow(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Error(closeErr)
		}
	})
	return conn
}

func send(t *testing.T, conn *websocket.Conn, msg string) {
	t.Helper()
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(msg)); err != nil {
		t.Fatal(err)
	}
}

// receive reads one frame and returns its messages.
func receive(t *testing.T, conn *websocket.Conn) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []json.RawMessage
	if err = json.Unmarshal(data, &msgs); err != nil {
		t.Fatalf("frame %s is not a JSON array: %v", data, err)
	}
	lines := make([]string, len(msgs))
	for i, m := range msgs {
		lines[i] = string(m)
	}
	return lines
}

// eventually waits up to five seconds for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebSocketStream(t *testing.T) {
	t.Parallel()
	h, url := serve(t, stream.Config{})
	conn := dial(t, url)

	send(t, conn, `{"action":"subscribe","trades":["AAPL"]}`)
	wantMessages(t, receive(t, conn), `{"type":"subscription","trades":["AAPL"],"quotes":[],"bars":[]}`)
	h.HandleTrade(trade("AAPL", 1))
	wantMessages(t, receive(t, conn), tradeJSON("AAPL", 1))

	for _, tt := range []struct{ request, want string }{
		{`not json`, `{"type":"error","message":"invalid request: invalid character 'o' in literal null (expecting 'u')"}`},
		{`{"action":"subscribe","trade":["AAPL"]}`, `{"type":"error","message":"invalid request: json: unknown field \"trade\""}`},
		{`{"action":"watch"}`, `{"type":"error","message":"action must be subscribe or unsubscribe, got \"watch\""}`},
		{`{"action":"subscribe","quotes":["aapl"]}`, `{"type":"error","message":"invalid symbol \"aapl\""}`},
		{`{"action":"unsubscribe","trades":["AAPL"]}`, `{"type":"subscription","trades":[],"quotes":[],"bars":[]}`},
	} {
		send(t, conn, tt.request)
		wantMessages(t, receive(t, conn), tt.want)
	}
}

// A client that stops reading is disconnected, whether its queue fills up
// first or a write blocks for MaxLag first.
func TestWebSocketDisconnectsSlowClient(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		buffer int
	}{
		{"queue fills", 8},
		{"write blocks", 1 << 20}, // never fills before the socket does
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, url := serve(t, stream.Config{Buffer: tt.buffer, MaxLag: 100 * time.Millisecond, PingInterval: time.Hour})
			conn := dial(t, url)
			send(t, conn, `{"action":"subscribe","trades":["AAPL"]}`)
			receive(t, conn)

			for seq := uint64(1); h.Stats().Disconnected == 0; seq++ {
				if seq > 10_000_000 {
					t.Fatal("client never disconnected")
				}
				h.HandleTrade(trade("AAPL", seq))
				// A real feed waits on the network now and then. Without
				// that, on one CPU the writer would only run when this loop
				// is preempted.
				if seq%64 == 0 {
					runtime.Gosched()
				}
			}
			eventually(t, "the connection to close", func() bool { return h.Stats().Clients == 0 })
			if dropped := h.Stats().Dropped; (tt.buffer == 8) != (dropped > 0) {
				t.Errorf("%d messages dropped", dropped)
			}
		})
	}
}

// A connected client that doesn't answer pings is disconnected even when no
// events are sent to it.
func TestWebSocketPingsIdleClients(t *testing.T) {
	t.Parallel()
	h, url := serve(t, stream.Config{MaxLag: 50 * time.Millisecond, PingInterval: 20 * time.Millisecond})
	dial(t, url) // never reads, so never answers a ping
	eventually(t, "the client to be disconnected", func() bool {
		s := h.Stats()
		return s.Disconnected == 1 && s.Clients == 0
	})
}

// The handshake names the hub's session, which differs between hubs.
func TestWebSocketSession(t *testing.T) {
	t.Parallel()
	var sessions []string
	for range 2 {
		h, url := serve(t, stream.Config{})
		conn, resp, err := websocket.Dial(t.Context(), url, nil)
		if err != nil {
			t.Fatal(err)
		}
		closeConn(t, conn)
		if got := resp.Header.Get("Stream-Session"); got == "" || got != h.Session() {
			t.Errorf("Stream-Session header %q, want the hub's session %q", got, h.Session())
		}
		sessions = append(sessions, h.Session())
	}
	if sessions[0] == sessions[1] {
		t.Errorf("two hubs have the same session %q", sessions[0])
	}
}

func TestWebSocketHubClose(t *testing.T) {
	t.Parallel()
	h, err := stream.NewHub(stream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := dial(t, url)
	eventually(t, "the client to connect", func() bool { return h.Stats().Clients == 1 })

	readErr := make(chan error, 1)
	go func() {
		_, _, rErr := conn.Read(context.Background())
		readErr <- rErr
	}()
	if err = h.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-readErr; websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("client read %v, want close status %d", err, websocket.StatusGoingAway)
	}

	_, resp, err := websocket.Dial(t.Context(), url, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("dial after close: %v, want status %d", err, http.StatusServiceUnavailable)
	}
	if resp != nil && resp.Body != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
}

func closeConn(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Error(err)
	}
}
