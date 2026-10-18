package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

func (c *client) quote(bid, ask string) {
	c.quotes.HandleQuote(marketdata.Quote{
		Symbol: "AAPL", Bid: decimal.RequireFromString(bid), Ask: decimal.RequireFromString(ask), Timestamp: friday,
	})
}

func (c *client) funded(cash string) (id, key string) {
	c.t.Helper()
	id, key = c.account()
	wantStatus(c.t, c.do("POST", "/v1/accounts/"+id+"/deposits", `{"amount":"`+cash+`"}`, "Authorization", key), http.StatusCreated, "")
	return id, key
}

func TestOrders(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, key := c.funded("10000")
	_, bobKey := c.account()
	path := "/v1/accounts/" + alice + "/orders"
	c.quote("100.00", "100.10")

	r := c.do("POST", path, `{"symbol":"AAPL","side":"buy","type":"market","time_in_force":"day","qty":"10"}`, "Authorization", key)
	wantStatus(t, r, http.StatusCreated, "")
	if field(r, "status") != "filled" || field(r, "fill_price") != "100.1" || field(r, "closed_at") == nil {
		t.Errorf("market buy %v", r.body)
	}

	resting := c.do("POST", path, `{"symbol":"AAPL","side":"buy","type":"limit","time_in_force":"day","qty":"5","limit_price":"90"}`,
		"Authorization", key)
	wantStatus(t, resting, http.StatusCreated, "")
	id := fmt.Sprint(field(resting, "id"))
	if field(resting, "status") != "accepted" || field(resting, "expires_at") == nil {
		t.Errorf("limit buy %v", resting.body)
	}

	rejected := c.do("POST", path, `{"symbol":"AAPL","side":"buy","type":"limit","time_in_force":"gtc","qty":"1000","limit_price":"90"}`,
		"Authorization", key)
	if field(rejected, "status") != "rejected" || field(rejected, "reject_reason") != "insufficient buying power" {
		t.Errorf("too big an order %d %v", rejected.status, rejected.body)
	}

	if open := c.do("GET", path+"?status=open", "", "Authorization", key); len(open.list) != 1 {
		t.Errorf("open orders %v, want the resting one", open.list)
	}
	if all := c.do("GET", path+"?limit=2", "", "Authorization", key); len(all.list) != 2 {
		t.Errorf("a page of 2 has %d orders", len(all.list))
	}
	wantStatus(t, c.do("GET", path+"/"+id, "", "Authorization", key), http.StatusOK, "")
	wantStatus(t, c.do("GET", path+"/"+id, "", "Authorization", bobKey), http.StatusNotFound, "not_found")
	wantStatus(t, c.do("GET", path+"/not-an-order", "", "Authorization", key), http.StatusNotFound, "not_found")

	canceled := c.do("DELETE", path+"/"+id, "", "Authorization", key)
	if canceled.status != http.StatusOK || field(canceled, "status") != "canceled" {
		t.Errorf("cancel: %d %v", canceled.status, canceled.body)
	}
	wantStatus(t, c.do("DELETE", path+"/"+id, "", "Authorization", key), http.StatusConflict, "order_not_open")

	for _, tt := range []struct {
		body   string
		status int
		code   string
	}{
		{`{"symbol":"AAPL","side":"hold","type":"market","time_in_force":"day","qty":"1"}`, http.StatusUnprocessableEntity, "invalid_request"},
		{`{"symbol":"NOPE","side":"buy","type":"market","time_in_force":"day","qty":"1"}`, http.StatusUnprocessableEntity, "invalid_request"},
		{`{"symbol":"AAPL","side":"buy","type":"limit","time_in_force":"day","qty":"1","limit_price":"1.001"}`, http.StatusUnprocessableEntity, "invalid_request"},
		{`{"symbol":"AAPL","side":"buy","type":"market","time_in_force":"day","qty":"1","stop":"1"}`, http.StatusBadRequest, "invalid_json"},
	} {
		wantStatus(t, c.do("POST", path, tt.body, "Authorization", key), tt.status, tt.code)
	}
	for _, q := range []string{"?limit=0", "?limit=x", "?status=done", "?before=nope"} {
		wantStatus(t, c.do("GET", path+q, "", "Authorization", key), http.StatusUnprocessableEntity, "invalid_request")
	}
}

// Without a client_order_id, one is made from the Idempotency-Key, so a
// retry returns the first order. Keys can be longer than client order IDs,
// so it's a hash of the key.
func TestOrderIdempotencyKey(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, key := c.funded("10000")
	c.quote("100.00", "100.10")
	body := `{"symbol":"AAPL","side":"buy","type":"market","time_in_force":"day","qty":"1"}`
	for _, idem := range []string{"buy-1", strings.Repeat("k", 200)} {
		first := c.do("POST", "/v1/accounts/"+alice+"/orders", body, "Authorization", key, "Idempotency-Key", idem)
		wantStatus(t, first, http.StatusCreated, "")
		again := c.do("POST", "/v1/accounts/"+alice+"/orders", body, "Authorization", key, "Idempotency-Key", idem)
		if field(first, "id") != field(again, "id") || field(first, "client_order_id") == "" {
			t.Errorf("first %v, retry %v; want the same order", first.body, again.body)
		}
	}
}

func TestPositionsAndActivities(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	alice, key := c.funded("10000")
	c.quote("100.00", "100.10")
	c.do("POST", "/v1/accounts/"+alice+"/orders", `{"symbol":"AAPL","side":"buy","type":"market","time_in_force":"day","qty":"10"}`,
		"Authorization", key)

	p := c.do("GET", "/v1/accounts/"+alice+"/positions", "", "Authorization", key)
	if len(p.list) != 1 {
		t.Fatalf("positions %v", p.list)
	}
	pos := item(p, 0)
	for name, want := range map[string]string{
		"symbol": "AAPL", "qty": "10", "qty_available": "10", "cost_basis": "1001", "avg_entry_price": "100.1",
		"current_price": "100", "market_value": "1000", "unrealized_pl": "-1",
	} {
		if field(pos, name) != want {
			t.Errorf("position %s = %v, want %s", name, field(pos, name), want)
		}
	}

	path := "/v1/accounts/" + alice + "/activities"
	first := c.do("GET", path+"?limit=1", "", "Authorization", key)
	if len(first.list) != 1 {
		t.Fatalf("activities %v", first.list)
	}
	fill := item(first, 0)
	if field(fill, "type") != "trade_fill" || field(fill, "details", "symbol") != "AAPL" {
		t.Errorf("newest activity %v, want the fill", fill.body)
	}
	next := c.do("GET", fmt.Sprintf("%s?limit=10&before=%v", path, field(fill, "seq")), "", "Authorization", key)
	if len(next.list) != 1 {
		t.Fatalf("second page %v, want the deposit", next.list)
	}
	if deposit := item(next, 0); field(deposit, "type") != "deposit" {
		t.Errorf("second page %v, want the deposit", deposit.body)
	}
}

// item returns the i-th object of a list response.
func item(r response, i int) response {
	m, ok := r.list[i].(map[string]any)
	if !ok {
		return response{}
	}
	return response{body: m}
}
