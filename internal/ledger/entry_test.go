package ledger

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func line(owner string, asset Asset, amount string) Line {
	return Line{AccountID: owner, Asset: asset, Amount: d(amount)}
}

func TestValidateLines(t *testing.T) {
	tests := []struct {
		name  string
		lines []Line
		want  error // nil when the lines are valid
	}{
		{"deposit", []Line{
			line(ExternalBank, USD, "-100"), line("alice", USD, "100"),
		}, nil},
		{"buy with fee", []Line{
			line("alice", USD, "-500.00"), line(Clearing, USD, "495.00"), line(Fees, USD, "5.00"),
			line(Clearing, "AAPL", "-10"), line("alice", "AAPL", "10"),
		}, nil},
		{"nine-place shares", []Line{
			line(Clearing, "AAPL", "-0.000000001"), line("alice", "AAPL", "0.000000001"),
		}, nil},
		{"no lines", nil, ErrUnbalanced},
		{"one line", []Line{line("alice", USD, "100")}, ErrUnbalanced},
		{"cash does not balance", []Line{
			line(ExternalBank, USD, "-100"), line("alice", USD, "99.99"),
		}, ErrUnbalanced},
		{"shares do not balance", []Line{
			line("alice", USD, "-50"), line(Clearing, USD, "50"),
			line(Clearing, "AAPL", "-1"), line("alice", "AAPL", "2"),
		}, ErrUnbalanced},
		{"assets cannot offset each other", []Line{
			line("alice", USD, "-100"), line("alice", "AAPL", "100"),
		}, ErrUnbalanced},
		{"zero amount", []Line{
			line(ExternalBank, USD, "0"), line("alice", USD, "0"),
		}, ErrInvalid},
		{"account twice", []Line{
			line(ExternalBank, USD, "-100"), line("alice", USD, "50"), line("alice", USD, "50"),
		}, ErrInvalid},
		{"ten-place shares", []Line{
			line(Clearing, "AAPL", "-0.0000000001"), line("alice", "AAPL", "0.0000000001"),
		}, ErrInvalid},
		{"too large for the database", []Line{
			line(ExternalBank, USD, "-1e29"), line("alice", USD, "1e29"),
		}, ErrInvalid},
		{"missing owner", []Line{
			line(ExternalBank, USD, "-1"), line("", USD, "1"),
		}, ErrInvalid},
		{"invalid asset", []Line{
			line(Clearing, "aapl", "-1"), line("alice", "aapl", "1"),
		}, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLines(tt.lines)
			if tt.want == nil && err != nil {
				t.Fatalf("validateLines() = %v, want nil", err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("validateLines() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestFillLines(t *testing.T) {
	tests := []struct {
		name string
		fill Fill
		fees regulatoryFees
		want map[accountRef]string
	}{
		{
			name: "buy",
			fill: Fill{AccountID: "alice", Symbol: "AAPL", Side: Buy, Quantity: d("10"), Price: d("49.50"), Fee: d("5")},
			want: map[accountRef]string{
				{"alice", USD}:     "-500",
				{Clearing, USD}:    "495",
				{Fees, USD}:        "5",
				{Clearing, "AAPL"}: "-10",
				{"alice", "AAPL"}:  "10",
			},
		},
		{
			name: "sell",
			fill: Fill{AccountID: "alice", Symbol: "AAPL", Side: Sell, Quantity: d("4"), Price: d("60"), Fee: d("1")},
			want: map[accountRef]string{
				{"alice", "AAPL"}:  "-4",
				{Clearing, "AAPL"}: "4",
				{Clearing, USD}:    "-240",
				{"alice", USD}:     "239",
				{Fees, USD}:        "1",
			},
		},
		{
			name: "sell with regulatory fees",
			fill: Fill{AccountID: "alice", Symbol: "AAPL", Side: Sell, Quantity: d("4"), Price: d("60"), Fee: d("1")},
			fees: regulatoryFees{sec: d("0.01"), taf: d("0.02")},
			want: map[accountRef]string{
				{"alice", "AAPL"}:    "-4",
				{Clearing, "AAPL"}:   "4",
				{Clearing, USD}:      "-240",
				{"alice", USD}:       "238.97",
				{Fees, USD}:          "1",
				{SECFeePayable, USD}: "0.01",
				{TAFPayable, USD}:    "0.02",
			},
		},
		{
			name: "no fee, no fee line",
			fill: Fill{AccountID: "alice", Symbol: "AAPL", Side: Buy, Quantity: d("1"), Price: d("10")},
			want: map[accountRef]string{
				{"alice", USD}:     "-10",
				{Clearing, USD}:    "10",
				{Clearing, "AAPL"}: "-1",
				{"alice", "AAPL"}:  "1",
			},
		},
		{
			name: "sell whose fee equals its proceeds",
			fill: Fill{AccountID: "alice", Symbol: "AAPL", Side: Sell, Quantity: d("0.01"), Price: d("100"), Fee: d("1")},
			want: map[accountRef]string{
				{"alice", "AAPL"}:  "-0.01",
				{Clearing, "AAPL"}: "0.01",
				{Clearing, USD}:    "-1",
				{Fees, USD}:        "1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := tt.fill.lines(tt.fees)
			if err := validateLines(lines); err != nil {
				t.Fatalf("fill lines are not a valid entry: %v", err)
			}
			if len(lines) != len(tt.want) {
				t.Fatalf("got %d lines %v, want %d", len(lines), lines, len(tt.want))
			}
			for _, l := range lines {
				ref := accountRef{account: l.AccountID, asset: l.Asset}
				want, ok := tt.want[ref]
				if !ok || !l.Amount.Equal(d(want)) {
					t.Errorf("line %s = %s, want %s", ref, l.Amount, want)
				}
			}
		})
	}
}

func TestNotionalRoundsHalfToEven(t *testing.T) {
	tests := []struct{ quantity, price, want string }{
		{"1", "0.125", "0.12"},
		{"1", "0.135", "0.14"},
		{"0.5", "33.33", "16.66"},  // 16.665
		{"0.5", "33.35", "16.68"},  // 16.675
		{"3", "0.335", "1"},        // 1.005
		{"0.333333333", "3", "1"},  // 0.999999999
		{"2", "100.004", "200.01"}, // 200.008
	}
	for _, tt := range tests {
		f := Fill{Quantity: d(tt.quantity), Price: d(tt.price)}
		if got := f.Notional(); !got.Equal(d(tt.want)) {
			t.Errorf("%s × %s = %s, want %s", tt.quantity, tt.price, got, tt.want)
		}
	}
}

func TestFillValidate(t *testing.T) {
	valid := Fill{
		ID: "fill-1", HoldID: 1, AccountID: "3f6c1a52-8d4e-4b7a-9c21-5e0d7f8a9b10", Symbol: "AAPL", Side: Buy,
		Quantity: d("1.5"), Price: d("100.25"), Fee: d("1.00"),
		TradeDate: time.Date(2026, 10, 16, 15, 30, 0, 0, time.UTC), SettleDate: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC),
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid fill: %v", err)
	}

	tests := []struct {
		name   string
		modify func(*Fill)
	}{
		{"missing id", func(f *Fill) { f.ID = "" }},
		{"missing hold", func(f *Fill) { f.HoldID = 0 }},
		{"system owner", func(f *Fill) { f.AccountID = Clearing }},
		{"owner not a UUID", func(f *Fill) { f.AccountID = "alice" }},
		{"cash as symbol", func(f *Fill) { f.Symbol = USD }},
		{"lowercase symbol", func(f *Fill) { f.Symbol = "aapl" }},
		{"unknown side", func(f *Fill) { f.Side = "short" }},
		{"zero quantity", func(f *Fill) { f.Quantity = d("0") }},
		{"ten-place quantity", func(f *Fill) { f.Quantity = d("0.0000000001") }},
		{"negative price", func(f *Fill) { f.Price = d("-1") }},
		{"negative fee", func(f *Fill) { f.Fee = d("-0.01") }},
		{"fraction of a cent fee", func(f *Fill) { f.Fee = d("0.001") }},
		{"worth less than half a cent", func(f *Fill) { f.Quantity, f.Price = d("0.000000001"), d("1") }},
		{"no trade date", func(f *Fill) { f.TradeDate = time.Time{} }},
		{"settles before it trades", func(f *Fill) { f.SettleDate = f.TradeDate.AddDate(0, 0, -1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := valid
			tt.modify(&f)
			if err := f.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate() = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestPostingHash(t *testing.T) {
	deposit := func(amount string) posting {
		return posting{key: "k", kind: KindDeposit, lines: []Line{
			line(ExternalBank, USD, "-"+amount), line("alice", USD, amount),
		}}
	}
	base := deposit("100")

	same := map[string]posting{
		"trailing zeros": deposit("100.00"),
		"reordered lines": {key: "k", kind: KindDeposit, lines: []Line{
			line("alice", USD, "100"), line(ExternalBank, USD, "-100"),
		}},
		"different key": {key: "other", kind: KindDeposit, lines: base.lines},
	}
	for name, p := range same {
		if !bytes.Equal(p.hash(), base.hash()) {
			t.Errorf("%s: hash differs, want the same", name)
		}
	}

	different := map[string]posting{
		"amount":   deposit("100.01"),
		"kind":     {key: "k", kind: KindWithdrawal, lines: base.lines},
		"metadata": {key: "k", kind: KindDeposit, lines: base.lines, metadata: map[string]string{"ref": "x"}},
	}
	for name, p := range different {
		if bytes.Equal(p.hash(), base.hash()) {
			t.Errorf("different %s: same hash, want different", name)
		}
	}
}

func TestHoldRequestHash(t *testing.T) {
	a := HoldRequest{IdempotencyKey: "a", AccountID: "alice", Asset: USD, Amount: d("5")}
	b := HoldRequest{IdempotencyKey: "b", AccountID: "alice", Asset: USD, Amount: d("5.00")}
	c := HoldRequest{IdempotencyKey: "a", AccountID: "alice", Asset: USD, Amount: d("5.01")}
	if !bytes.Equal(a.hash(), b.hash()) {
		t.Error("5 and 5.00 hash differently")
	}
	if bytes.Equal(a.hash(), c.hash()) {
		t.Error("5 and 5.01 hash the same")
	}
}
