package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/api"
	"github.com/AmrMady/brokerd/internal/calendar"
	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

type client struct {
	t      *testing.T
	url    string
	server *api.Server
	pool   *pgxpool.Pool
	quotes *marketdata.QuoteCache
}

func newClient(t *testing.T) *client {
	t.Helper()
	pool := pgtest.NewPool(t)
	quotes := &marketdata.QuoteCache{}
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		if err := ledger.AddAsset(t.Context(), tx, "AAPL", 9); err != nil {
			return err
		}
		for _, r := range []ledger.FeeRate{
			{Fee: ledger.SECFee, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
			{Fee: ledger.TAF, EffectiveOn: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), MaxPerTrade: decimal.NewNullDecimal(decimal.Zero)},
		} {
			if err := ledger.AddFeeRate(t.Context(), tx, r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = api.MaintainIdempotency(t.Context(), pool, time.Now()); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(t.Context(), engine.Config{Pool: pool, Quotes: quotes, Calendar: calendar.New()})
	if err != nil {
		t.Fatal(err)
	}
	s := api.New(api.Config{Pool: pool, Engine: eng, Quotes: quotes})
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return &client{t: t, url: srv.URL, server: s, pool: pool, quotes: quotes}
}

type response struct {
	status int
	header http.Header
	body   map[string]any
	list   []any
}

// do sends a request. headers are name, value pairs.
func (c *client) do(method, path, body string, headers ...string) response {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.url+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.t.Error(closeErr)
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	out := response{status: resp.StatusCode, header: resp.Header}
	if bytes.HasPrefix(bytes.TrimSpace(data), []byte("[")) {
		err = json.Unmarshal(data, &out.list)
	} else if len(data) > 0 {
		err = json.Unmarshal(data, &out.body)
	}
	if err != nil {
		c.t.Fatalf("%s %s: %s is not JSON: %v", method, path, data, err)
	}
	return out
}

// account creates an account and returns its ID and an Authorization header
// value.
func (c *client) account() (id, auth string) {
	c.t.Helper()
	r := c.do("POST", "/v1/accounts", "")
	if r.status != http.StatusCreated {
		c.t.Fatalf("create account: %d %v", r.status, r.body)
	}
	return fmt.Sprint(field(r, "account", "id")), "Bearer " + fmt.Sprint(field(r, "api_key"))
}

func wantStatus(t *testing.T, r response, status int, code string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status %d (%v), want %d", r.status, r.body, status)
	}
	if code == "" {
		return
	}
	if field(r, "error", "code") != code || field(r, "error", "request_id") == "" || field(r, "error", "message") == "" {
		t.Errorf("error %v, want code %q with a message and request id", r.body, code)
	}
}

func field(r response, path ...string) any {
	var v any = r.body
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}

func TestHealth(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	wantStatus(t, c.do("GET", "/healthz", ""), http.StatusOK, "")
	wantStatus(t, c.do("GET", "/readyz", ""), http.StatusOK, "")
	c.server.Drain()
	wantStatus(t, c.do("GET", "/readyz", ""), http.StatusServiceUnavailable, "")
	wantStatus(t, c.do("GET", "/healthz", ""), http.StatusOK, "")
}

func TestRequestIDs(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	if r := c.do("GET", "/healthz", "", "X-Request-Id", "trace-123"); r.header.Get("X-Request-Id") != "trace-123" {
		t.Errorf("request id %q, want the client's", r.header.Get("X-Request-Id"))
	}
	r := c.do("GET", "/v1/accounts/x", "", "X-Request-Id", "not valid!")
	id := r.header.Get("X-Request-Id")
	if id == "" || id == "not valid!" || field(r, "error", "request_id") != id {
		t.Errorf("request id %q, error %v; want a new id, in the error too", id, r.body)
	}
}

func TestAccounts(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, aliceKey := c.account()
	_, bobKey := c.account()
	if !strings.HasPrefix(aliceKey, "Bearer bk_") {
		t.Errorf("API key %q, want bk_ prefix", aliceKey)
	}

	r := c.do("GET", "/v1/accounts/"+alice, "", "Authorization", aliceKey)
	wantStatus(t, r, http.StatusOK, "")
	if field(r, "id") != alice || field(r, "status") != "active" || field(r, "cash", "balance") != "0" {
		t.Errorf("account %v", r.body)
	}

	wantStatus(t, c.do("GET", "/v1/accounts/"+alice, ""), http.StatusUnauthorized, "unauthorized")
	wantStatus(t, c.do("GET", "/v1/accounts/"+alice, "", "Authorization", "Bearer bk_nope"), http.StatusUnauthorized, "unauthorized")
	wantStatus(t, c.do("GET", "/v1/accounts/"+alice, "", "Authorization", bobKey), http.StatusNotFound, "not_found")
}

func TestTransfers(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, key := c.account()
	path := "/v1/accounts/" + alice

	r := c.do("POST", path+"/deposits", `{"amount": "1000.00"}`, "Authorization", key, "Idempotency-Key", "dep-1")
	wantStatus(t, r, http.StatusCreated, "")
	if field(r, "status") != "completed" || field(r, "direction") != "deposit" || field(r, "amount") != "1000" {
		t.Errorf("deposit %v", r.body)
	}
	again := c.do("POST", path+"/deposits", `{"amount": "1000.00"}`, "Authorization", key, "Idempotency-Key", "dep-1")
	if field(again, "id") != field(r, "id") {
		t.Errorf("retried deposit %v, want transfer %v again", again.body, field(r, "id"))
	}

	wantStatus(t, c.do("POST", path+"/withdrawals", `{"amount": 300}`, "Authorization", key), http.StatusCreated, "")
	r = c.do("GET", path, "", "Authorization", key)
	if field(r, "cash", "balance") != "700" || field(r, "cash", "withdrawable") != "700" {
		t.Errorf("cash %v, want 700", field(r, "cash"))
	}

	for _, tt := range []struct {
		body   string
		status int
		code   string
	}{
		{`{"amount": "10000"}`, http.StatusUnprocessableEntity, "insufficient_funds"},
		{`{"amount": "-5"}`, http.StatusUnprocessableEntity, "invalid_request"},
		{`{"amount": "1.001"}`, http.StatusUnprocessableEntity, "invalid_request"},
		{`{"amount": `, http.StatusBadRequest, "invalid_json"},
		{`{"amount": "1", "currency": "EUR"}`, http.StatusBadRequest, "invalid_json"},
		{`{"amount": "1"} {}`, http.StatusBadRequest, "invalid_json"},
	} {
		wantStatus(t, c.do("POST", path+"/withdrawals", tt.body, "Authorization", key), tt.status, tt.code)
	}
}

func TestPanicsBecomeInternalErrors(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.server.Handle("GET /panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	r := c.do("GET", "/panic", "")
	wantStatus(t, r, http.StatusInternalServerError, "internal")
	if field(r, "error", "message") != "internal error" {
		t.Errorf("message %v, want nothing about the panic", field(r, "error", "message"))
	}
	wantStatus(t, c.do("GET", "/healthz", ""), http.StatusOK, "")
}

func TestBodyTooLarge(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, key := c.account()
	body := `{"amount": "` + strings.Repeat("1", 2<<20) + `"}`
	wantStatus(t, c.do("POST", "/v1/accounts/"+alice+"/deposits", body, "Authorization", key), http.StatusRequestEntityTooLarge, "too_large")
}

// A request whose client went away isn't an internal error.
func TestCanceledRequests(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.server.Handle("GET /canceled", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.server.WriteErrorForTest(w, r, context.Canceled)
	}))
	wantStatus(t, c.do("GET", "/canceled", ""), 499, "canceled")
}
