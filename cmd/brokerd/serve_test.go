package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

type served struct {
	url  string
	logs *bytes.Buffer // read only after done
	done chan error
	stop context.CancelFunc
}

// startServe runs serve on a free port until stopped or the test ends.
func startServe(t *testing.T, cfg serveConfig) *served {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &served{url: "ws://" + ln.Addr().String() + "/v1/stream", logs: &bytes.Buffer{}, done: make(chan error, 1), stop: cancel}
	logger := slog.New(slog.NewJSONHandler(s.logs, nil))
	go func() { s.done <- serve(ctx, cfg, ln, logger) }()
	t.Cleanup(func() {
		cancel()
		<-s.done
	})
	return s
}

// wait returns serve's error once it has returned.
func (s *served) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.done:
		s.done <- err // for the cleanup
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("serve didn't return")
		return nil
	}
}

// stopped returns the attributes of the "market data stopped" log line.
func (s *served) stopped(t *testing.T) map[string]any {
	t.Helper()
	for line := range strings.Lines(s.logs.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		if m["msg"] == "stopped" {
			return m
		}
	}
	t.Fatalf("no stats logged:\n%s", s.logs)
	return nil
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		conn, resp, err := websocket.Dial(ctx, url, nil)
		if resp != nil && resp.Body != nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if err == nil {
			return conn
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readUntil reads messages until each of the wanted types has been seen,
// and returns them all.
func readUntil(t *testing.T, conn *websocket.Conn, types ...string) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	seen := map[string]bool{}
	var all []map[string]any
	for len(seen) < len(types) {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v (saw %v)", err, seen)
		}
		var msgs []map[string]any
		if err = json.Unmarshal(data, &msgs); err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			for _, typ := range types {
				if m["type"] == typ {
					seen[typ] = true
				}
			}
		}
		all = append(all, msgs...)
	}
	return all
}

// drain reads until the connection closes and returns why.
func drain(conn *websocket.Conn) <-chan error {
	closed := make(chan error, 1)
	go func() {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				closed <- err
				return
			}
		}
	}()
	return closed
}

func subscribe(t *testing.T, conn *websocket.Conn, request string) {
	t.Helper()
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(request)); err != nil {
		t.Fatal(err)
	}
}

func TestServeStreamsSimulatedMarketData(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{feed: "sim", symbols: []string{"AAPL", "MSFT"}, rate: 2000, seed: 1})
	conn := dial(t, s.url)
	subscribe(t, conn, `{"action":"subscribe","trades":["AAPL"],"quotes":["AAPL"],"bars":["AAPL"]}`)

	var lastSeq float64
	for _, m := range readUntil(t, conn, "subscription", "trade", "quote", "bar") {
		if m["type"] != "subscription" && m["symbol"] != "AAPL" {
			t.Errorf("got %v, want only AAPL", m)
		}
		if m["type"] == "bar" && m["timeframe"] != "1s" {
			t.Errorf("first bar %v, want a 1s bar", m)
		}
		if seq, ok := m["seq"].(float64); ok {
			if seq <= lastSeq {
				t.Errorf("seq %v after %v", seq, lastSeq)
			}
			lastSeq = seq
		}
	}

	closed := drain(conn)
	s.stop()
	if err := s.wait(t); err != nil {
		t.Errorf("serve returned %v", err)
	}
	if err := <-closed; websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("client saw %v, want going away", err)
	}
	if stats := s.stopped(t); stats["gaps"] != 0.0 || stats["stale"] != 0.0 {
		t.Errorf("stats %v, want no gaps or stale events", stats)
	}
}

// A replay that runs out ends serve, after its last bars are flushed.
func TestServeEndsWithTheReplay(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{feed: "replay", file: "../../internal/marketdata/testdata/events.csv", speed: 0})
	if err := s.wait(t); err != nil {
		t.Fatalf("serve returned %v", err)
	}
	// AAPL and MSFT trade in different seconds of the same minute.
	if stats := s.stopped(t); stats["bars"] != 4.0 {
		t.Errorf("stats %v, want 4 bars", stats)
	}
}

func TestServeFailsWithTheFeed(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "events.csv")
	data := "type,symbol,seq,timestamp,price,size,bid,bid_size,ask,ask_size\ntrade,AAPL,1,yesterday,1,1,,,,\n"
	if err := os.WriteFile(file, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	s := startServe(t, serveConfig{feed: "replay", file: file})
	if err := s.wait(t); err == nil || !strings.Contains(err.Error(), "replay line 2") {
		t.Errorf("serve returned %v, want the replay's error", err)
	}
}

// One brokerd reads the simulator and another relays it.
func TestServeRelaysAnotherServe(t *testing.T) {
	t.Parallel()
	core := startServe(t, serveConfig{feed: "sim", symbols: []string{"AAPL"}, rate: 2000, seed: 1})
	dial(t, core.url) // wait until it's up
	edge := startServe(t, serveConfig{feed: "relay", upstream: core.url, symbols: []string{"AAPL"}})
	conn := dial(t, edge.url)
	subscribe(t, conn, `{"action":"subscribe","trades":["AAPL"]}`)
	readUntil(t, conn, "trade")

	closed := drain(conn)
	edge.stop()
	if err := edge.wait(t); err != nil {
		t.Errorf("edge returned %v", err)
	}
	<-closed
	core.stop()
	if err := core.wait(t); err != nil {
		t.Errorf("core returned %v", err)
	}
}

func TestParseServe(t *testing.T) {
	t.Parallel()
	cfg, err := parseServe(nil, io.Discard)
	if err != nil || cfg.feed != "sim" || cfg.addr != ":8080" || len(cfg.symbols) != 5 || cfg.grace != time.Second ||
		cfg.readTimeout != 30*time.Second || cfg.writeTimeout != 30*time.Second || cfg.idleTimeout != 2*time.Minute ||
		cfg.drainDelay != 5*time.Second {
		t.Errorf("defaults: %+v, %v", cfg, err)
	}
	cfg, err = parseServe([]string{"-feed", "relay", "-upstream", "ws://core:8080/v1/stream", "-symbols", "AAPL,MSFT"}, io.Discard)
	if err != nil || cfg.upstream != "ws://core:8080/v1/stream" || strings.Join(cfg.symbols, " ") != "AAPL MSFT" {
		t.Errorf("relay: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-feed", "kafka"},
		{"-feed", "replay"},
		{"-feed", "relay"},
		{"-rate", "fast"},
		{"extra"},
	} {
		if _, err = parseServe(args, io.Discard); err == nil {
			t.Errorf("parseServe(%q) succeeded, want an error", args)
		}
	}
	if _, err = parseServe([]string{"-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h returned %v, want %v", err, flag.ErrHelp)
	}
}

// A keep-alive connection that sends nothing more is closed after the idle
// timeout, rather than held open for good.
func TestServeClosesIdleConnections(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{feed: "sim", symbols: []string{"AAPL"}, rate: 10, seed: 1, idleTimeout: 100 * time.Millisecond})
	addr := strings.TrimSuffix(strings.TrimPrefix(s.url, "ws://"), "/v1/stream")
	var (
		conn net.Conn
		err  error
	)
	for range 100 {
		if conn, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }() //nolint:errcheck // the server closed it
	if _, err = io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: brokerd\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if err = conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("idle connection: read returned %v, want the server to close it", err)
	}
}

// On shutdown, serve says it isn't ready and keeps serving for the drain
// delay before it stops, so a load balancer's readiness checks see the 503
// and stop sending it requests first.
func TestServeDrainsBeforeStopping(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{feed: "sim", symbols: []string{"AAPL"}, rate: 10, seed: 1, drainDelay: time.Second})
	base := "http" + strings.TrimSuffix(strings.TrimPrefix(s.url, "ws"), "/v1/stream")
	deadline := time.Now().Add(10 * time.Second)
	for status, _ := request(t, "GET", base+"/readyz", ""); status != http.StatusOK; status, _ = request(t, "GET", base+"/readyz", "") {
		if time.Now().After(deadline) {
			t.Fatalf("not ready: %d", status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	s.stop()
	began := time.Now()
	for {
		status, err := tryGet(t, base+"/readyz")
		if status == http.StatusServiceUnavailable {
			break
		}
		if err != nil || time.Since(began) > time.Second {
			t.Fatalf("readyz while stopping: %d, %v; want 503", status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.wait(t); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took < time.Second {
		t.Errorf("stopped %s after the signal, before the drain delay was up", took)
	}
}

// tryGet returns the status of a GET, or the error if there's no response.
func tryGet(t *testing.T, url string) (int, error) {
	req, err := http.NewRequestWithContext(t.Context(), "GET", url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, resp.Body.Close()
}

// The request timeouts don't apply to the stream, which lasts as long as
// the client keeps up.
func TestServeStreamsPastTheRequestTimeouts(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{
		feed: "sim", symbols: []string{"AAPL"}, rate: 100, seed: 1,
		readTimeout: 100 * time.Millisecond, writeTimeout: 100 * time.Millisecond,
	})
	conn := dial(t, s.url)
	subscribe(t, conn, `{"action":"subscribe","quotes":["AAPL"]}`)
	readUntil(t, conn, "subscription", "quote")
	time.Sleep(300 * time.Millisecond)
	readUntil(t, conn, "quote")
	if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Error(err)
	}
}

// With a database, serve runs the API next to the stream.
func TestServeWithAPI(t *testing.T) {
	t.Parallel()
	s := startServe(t, serveConfig{databaseURL: pgtest.NewURL(t), feed: "sim", symbols: []string{"AAPL"}, rate: 1000, seed: 1})
	base := "http" + strings.TrimSuffix(strings.TrimPrefix(s.url, "ws"), "/v1/stream")
	get := func(path string, headers ...string) (int, map[string]any) {
		return request(t, "GET", base+path, "", headers...)
	}
	deadline := time.Now().Add(10 * time.Second)
	for status, _ := get("/readyz"); status != http.StatusOK; status, _ = get("/readyz") {
		if time.Now().After(deadline) {
			t.Fatalf("not ready: %d", status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	status, created := request(t, "POST", base+"/v1/accounts", "")
	if status != http.StatusCreated {
		t.Fatalf("create account: %d %v", status, created)
	}
	id, key := fmt.Sprint(field(created, "account", "id")), fmt.Sprint(field(created, "api_key"))
	if status, _ = request(t, "POST", base+"/v1/accounts/"+id+"/deposits", `{"amount":"500"}`, "Authorization", "Bearer "+key); status != http.StatusCreated {
		t.Fatalf("deposit: %d", status)
	}
	status, got := get("/v1/accounts/"+id, "Authorization", "Bearer "+key)
	if status != http.StatusOK || field(got, "cash", "balance") != "500" {
		t.Errorf("account: %d %v", status, got)
	}
	req, err := http.NewRequestWithContext(t.Context(), "GET", base+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	scraped, err := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	for _, name := range []string{"brokerd_marketdata_events_total", "brokerd_ledger_post_seconds_count 1", "brokerd_engine_expiry_errors_total 0", `route="POST /v1/accounts"`} {
		if !strings.Contains(string(scraped), name) {
			t.Errorf("metrics have no %s", name)
		}
	}

	// The stream is mounted on the API server.
	conn := dial(t, s.url)
	subscribe(t, conn, `{"action":"subscribe","quotes":["AAPL"]}`)
	readUntil(t, conn, "quote")
	closed := drain(conn)

	s.stop()
	if err = s.wait(t); err != nil {
		t.Errorf("serve returned %v", err)
	}
	<-closed
}

// field digs a value out of decoded JSON objects.
func field(v any, path ...string) any {
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}

func request(t *testing.T, method, url, body string, headers ...string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}
