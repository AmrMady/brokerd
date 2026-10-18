package orders_test

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/orders"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func limit(s string) decimal.NullDecimal { return decimal.NewNullDecimal(dec(s)) }

func TestRequestValidate(t *testing.T) {
	t.Parallel()
	good := orders.Request{
		AccountID: "a", Symbol: "AAPL", Side: ledger.Buy, Type: orders.Limit, TimeInForce: orders.Day,
		Quantity: dec("1.5"), LimitPrice: limit("227.15"),
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*orders.Request)
	}{
		{"no account", func(r *orders.Request) { r.AccountID = "" }},
		{"no symbol", func(r *orders.Request) { r.Symbol = "" }},
		{"cash as a symbol", func(r *orders.Request) { r.Symbol = ledger.USD }},
		{"bad side", func(r *orders.Request) { r.Side = "short" }},
		{"bad type", func(r *orders.Request) { r.Type = "stop" }},
		{"bad time in force", func(r *orders.Request) { r.TimeInForce = "ioc" }},
		{"zero quantity", func(r *orders.Request) { r.Quantity = dec("0") }},
		{"limit without a price", func(r *orders.Request) { r.LimitPrice = decimal.NullDecimal{} }},
		{"market with a price", func(r *orders.Request) { r.Type = orders.Market }},
		{"negative price", func(r *orders.Request) { r.LimitPrice = limit("-1") }},
		{"sub-penny price over $1", func(r *orders.Request) { r.LimitPrice = limit("227.155") }},
		{"too fine under $1", func(r *orders.Request) { r.LimitPrice = limit("0.50001") }},
		{"long client order id", func(r *orders.Request) { r.ClientOrderID = strings.Repeat("x", 65) }},
		{"control character in client order id", func(r *orders.Request) { r.ClientOrderID = "a\nb" }},
	} {
		r := good
		tt.change(&r)
		if err := r.Validate(); err == nil {
			t.Errorf("%s: Validate succeeded, want an error", tt.name)
		}
	}
	r := good
	r.LimitPrice = limit("0.5001")
	if err := r.Validate(); err != nil {
		t.Errorf("a price under $1 with four decimals: %v", err)
	}
}
