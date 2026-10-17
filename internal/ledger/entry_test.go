package ledger

import (
	"bytes"
	"errors"
	"testing"

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
