package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

// testHandler counts its calls and answers with the count. It can be made
// to fail, to find its client gone, to run until its deadline, or to wait
// until released.
type testHandler struct {
	calls    atomic.Int32
	fail     atomic.Bool
	canceled atomic.Bool
	slow     atomic.Bool
	started  chan struct{}
	release  chan struct{}
}

func (h *testHandler) serve(w http.ResponseWriter, r *http.Request) error {
	n := h.calls.Add(1)
	if h.slow.Load() {
		<-r.Context().Done()
		return r.Context().Err()
	}
	if h.started != nil {
		h.started <- struct{}{}
		<-h.release
	}
	if h.fail.Load() {
		return fmt.Errorf("handler failed")
	}
	if h.canceled.Load() {
		return context.Canceled
	}
	writeJSON(w, http.StatusCreated, map[string]int32{"call": n})
	return nil
}

// idempotentServer serves h with the handler deadline, if one is given.
func idempotentServer(t *testing.T, h *testHandler, deadline ...time.Duration) (string, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.NewPool(t)
	if err := MaintainIdempotency(t.Context(), pool, time.Now()); err != nil {
		t.Fatal(err)
	}
	s := New(Config{Pool: pool})
	if len(deadline) > 0 {
		s.handlerTimeout = deadline[0]
	}
	s.route("POST /things/{account}", s.idempotent(h.serve))
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return srv.URL, pool
}

func post(t *testing.T, url, key, body string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), "POST", url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, strings.TrimSpace(string(data)), resp.Header
}

func TestIdempotentReplay(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, _ := idempotentServer(t, h)

	status, body, header := post(t, url+"/things/a", "k1", `{"x":1}`)
	if status != http.StatusCreated || body != `{"call":1}` || header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("first: %d %s %v", status, body, header)
	}
	status, body, header = post(t, url+"/things/a", "k1", `{"x":1}`)
	if status != http.StatusCreated || body != `{"call":1}` || header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("retry: %d %s %v, want the first response replayed", status, body, header)
	}
	// Keys belong to their account, and requests without one always run.
	if _, body, _ = post(t, url+"/things/b", "k1", `{"x":1}`); body != `{"call":2}` {
		t.Errorf("the same key for another account: %s, want a new call", body)
	}
	if _, body, _ = post(t, url+"/things/a", "", `{"x":1}`); body != `{"call":3}` {
		t.Errorf("no key: %s, want a new call", body)
	}

	status, body, _ = post(t, url+"/things/a", "k1", `{"x":2}`)
	if status != http.StatusUnprocessableEntity || !strings.Contains(body, "idempotency_key_reused") {
		t.Errorf("same key, different body: %d %s", status, body)
	}
	for _, key := range []string{strings.Repeat("k", 256), "a\tb"} {
		if status, _, _ = post(t, url+"/things/a", key, `{}`); status != http.StatusUnprocessableEntity {
			t.Errorf("key %q: status %d, want 422", key, status)
		}
	}
	if n := h.calls.Load(); n != 3 {
		t.Errorf("handler ran %d times, want 3", n)
	}
}

func TestIdempotentInFlight(t *testing.T) {
	t.Parallel()
	h := &testHandler{started: make(chan struct{}), release: make(chan struct{})}
	url, _ := idempotentServer(t, h)

	first := make(chan string, 1)
	go func() {
		_, body, _ := post(t, url+"/things/a", "k1", `{}`)
		first <- body
	}()
	<-h.started
	if status, body, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusConflict || !strings.Contains(body, "request_in_progress") {
		t.Errorf("duplicate while running: %d %s", status, body)
	}
	close(h.release)
	if body := <-first; body != `{"call":1}` {
		t.Errorf("first: %s", body)
	}
	if _, body, _ := post(t, url+"/things/a", "k1", `{}`); body != `{"call":1}` {
		t.Errorf("after it finished: %s, want the replay", body)
	}
}

// Server errors aren't kept, so the request can be retried.
func TestIdempotentServerErrors(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, _ := idempotentServer(t, h)
	h.fail.Store(true)
	if status, _, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusInternalServerError {
		t.Fatalf("failing handler: %d", status)
	}
	h.fail.Store(false)
	if status, body, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusCreated || body != `{"call":2}` {
		t.Errorf("retry after a server error: %d %s, want it run again", status, body)
	}
}

// A request whose client went away may not have had its effect, so, like a
// server error, it isn't kept and a retry runs it.
func TestIdempotentCanceled(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, _ := idempotentServer(t, h)
	h.canceled.Store(true)
	if status, _, _ := post(t, url+"/things/a", "k1", `{}`); status != statusClientClosed {
		t.Fatalf("canceled handler: %d", status)
	}
	h.canceled.Store(false)
	if status, body, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusCreated || body != `{"call":2}` {
		t.Errorf("retry after a canceled request: %d %s, want it run again", status, body)
	}
}

// A handler has less time than its claim's lease, so a slow one is stopped,
// and its claim given back, before another request could take the claim
// over and run it a second time.
func TestIdempotentHandlersHaveADeadline(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, _ := idempotentServer(t, h, 50*time.Millisecond)
	h.slow.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", url+"/things/a", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "k1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("slow handler: %v, want it stopped at its deadline", err)
	}
	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("slow handler: %d, want 503", resp.StatusCode)
	}
	h.slow.Store(false)
	if status, body, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusCreated || body != `{"call":2}` {
		t.Errorf("retry after the deadline: %d %s, want it run again", status, body)
	}
}

// A key claimed by a request that never finished, because its server
// crashed, can be claimed again once the lease is up.
func TestIdempotentAbandonedClaim(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, pool := idempotentServer(t, h)
	_, err := pool.Exec(t.Context(), `
		INSERT INTO idempotent_requests (scope, key, fingerprint, updated_at)
		VALUES ('a', 'k1', sha256(convert_to('POST /things/a' || chr(10) || '{}', 'UTF8')), now() - interval '2 minutes')`)
	if err != nil {
		t.Fatal(err)
	}
	if status, body, _ := post(t, url+"/things/a", "k1", `{}`); status != http.StatusCreated || body != `{"call":1}` {
		t.Errorf("abandoned claim: %d %s, want the request run", status, body)
	}
}

// Many copies of a request racing to claim its key run it once. The
// connections are opened first, and the requests start together, so the
// claims really do race.
func TestIdempotentConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, _ := idempotentServer(t, h)
	const n = 50
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: n}}
	send := func(method, path, key string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, url+path, strings.NewReader(`{}`))
		if err != nil {
			t.Error(err)
			return 0
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return 0
		}
		if _, err = io.Copy(io.Discard, resp.Body); err != nil {
			t.Error(err)
		}
		if err = resp.Body.Close(); err != nil {
			t.Error(err)
		}
		return resp.StatusCode
	}

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { send("GET", "/healthz", "") })
	}
	wg.Wait()

	for round := range 5 {
		start := make(chan struct{})
		statuses := make([]int, n)
		key := fmt.Sprintf("k%d", round)
		before := h.calls.Load()
		for i := range statuses {
			wg.Go(func() {
				<-start
				statuses[i] = send("POST", "/things/a", key)
			})
		}
		close(start)
		wg.Wait()
		for _, s := range statuses {
			if s != http.StatusCreated && s != http.StatusConflict {
				t.Fatalf("statuses %v, want only 201s and 409s", statuses)
			}
		}
		if calls := h.calls.Load() - before; calls != 1 {
			t.Fatalf("round %d: handler ran %d times, want once", round, calls)
		}
	}
	client.CloseIdleConnections()
}

func TestMaintainIdempotency(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	partitions := func() []string {
		rows, err := pool.Query(t.Context(), `
			SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = 'idempotent_requests' ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return names
	}
	day := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	for _, now := range []time.Time{day, day.Add(time.Hour)} {
		if err := MaintainIdempotency(t.Context(), pool, now); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"idempotent_requests_20261016", "idempotent_requests_20261017", "idempotent_requests_20261018"}
	if got := partitions(); !slices.Equal(got, want) {
		t.Errorf("partitions %v, want %v", got, want)
	}

	// The 16th's keys are kept until the end of the 17th.
	if err := MaintainIdempotency(t.Context(), pool, day.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if got := partitions(); got[0] != "idempotent_requests_20261016" {
		t.Errorf("partitions %v, want the 16th kept a day", got)
	}
	if err := MaintainIdempotency(t.Context(), pool, day.AddDate(0, 0, 2)); err != nil {
		t.Fatal(err)
	}
	want = []string{"idempotent_requests_20261017", "idempotent_requests_20261018", "idempotent_requests_20261019", "idempotent_requests_20261020"}
	if got := partitions(); !slices.Equal(got, want) {
		t.Errorf("partitions %v, want %v", got, want)
	}
}

// After a day, a key is forgotten: the same request runs again.
func TestIdempotentKeysExpire(t *testing.T) {
	t.Parallel()
	h := &testHandler{}
	url, pool := idempotentServer(t, h)
	if err := MaintainIdempotency(t.Context(), pool, time.Now().Add(-26*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(t.Context(), `
		INSERT INTO idempotent_requests (scope, key, fingerprint, status, body, created_at, updated_at)
		VALUES ('a', 'k1', sha256(convert_to('POST /things/a' || chr(10) || '{}', 'UTF8')), 201, '{"call":0}',
		        now() - interval '25 hours', now() - interval '25 hours')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, body, header := post(t, url+"/things/a", "k1", `{}`); body != `{"call":1}` || header.Get("Idempotent-Replayed") != "" {
		t.Errorf("a day-old key: %s, want the request run", body)
	}
}
