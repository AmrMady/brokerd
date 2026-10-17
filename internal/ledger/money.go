package ledger

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

// Asset is USD or a ticker symbol.
type Asset string

// USD is the cash asset.
const USD Asset = "USD"

// System accounts, created by the first migration. They are the other side
// of every movement, and their balances can go negative.
const (
	ExternalBank = "00000000-0000-0000-0000-000000000001"
	Clearing     = "00000000-0000-0000-0000-000000000002"
	Fees         = "00000000-0000-0000-0000-000000000003"
)

var systemAccounts = []string{ExternalBank, Clearing, Fees}

var symbolPattern = regexp.MustCompile(`^[A-Z][A-Z0-9.]{0,9}$`)

// maxAmount is the first value that doesn't fit in NUMERIC(38,9).
var maxAmount = decimal.New(1, 29)

const (
	maxScale = 9 // NUMERIC(38,9)
	usdScale = 2 // the scale of the USD row in assets
)

func (a Asset) validate() error {
	if a != USD && !symbolPattern.MatchString(string(a)) {
		return fmt.Errorf("%w: invalid asset %q", ErrInvalid, string(a))
	}
	return nil
}

// checkDecimal rejects extra decimal places rather than rounding them away.
func checkDecimal(name string, d decimal.Decimal, places int32) error {
	if !d.Equal(d.Truncate(places)) {
		return fmt.Errorf("%w: %s %s has more than %d decimal places", ErrInvalid, name, d, places)
	}
	if d.Abs().GreaterThanOrEqual(maxAmount) {
		return fmt.Errorf("%w: %s %s is out of range", ErrInvalid, name, d)
	}
	return nil
}

func checkPositive(name string, d decimal.Decimal, places int32) error {
	if !d.IsPositive() {
		return fmt.Errorf("%w: %s must be positive, got %s", ErrInvalid, name, d)
	}
	return checkDecimal(name, d, places)
}

func validateAccountID(id string) error {
	var u pgtype.UUID
	if err := u.Scan(id); err != nil {
		return fmt.Errorf("%w: account id %q is not a UUID", ErrInvalid, id)
	}
	return nil
}

func validateCustomer(id string) error {
	if err := validateAccountID(id); err != nil {
		return err
	}
	if slices.Contains(systemAccounts, id) {
		return fmt.Errorf("%w: %s is a system account", ErrInvalid, id)
	}
	return nil
}
