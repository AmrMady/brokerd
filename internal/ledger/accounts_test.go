package ledger_test

import (
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestOpenAccount(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)

	a, err := inTx(l, func(tx pgx.Tx) (ledger.Account, error) { return ledger.OpenAccount(l.ctx, tx) })
	noErr(t, err)
	b, err := inTx(l, func(tx pgx.Tx) (ledger.Account, error) { return ledger.OpenAccount(l.ctx, tx) })
	noErr(t, err)

	number := regexp.MustCompile(`^\d{10}$`)
	if !number.MatchString(a.Number) {
		t.Errorf("new account = %+v, want a 10-digit number", a)
	}
	if a.ID == b.ID || a.Number == b.Number {
		t.Errorf("accounts %+v and %+v share an id or number", a, b)
	}

	got, err := ledger.AccountByID(l.ctx, l.pool, a.ID)
	noErr(t, err)
	if got.ID != a.ID || got.Number != a.Number {
		t.Errorf("AccountByID = %+v, want %+v", got, a)
	}
	_, err = ledger.AccountByID(l.ctx, l.pool, "00000000-0000-4000-8000-000000000000")
	wantErr(t, err, ledger.ErrAccountNotFound)
	_, err = ledger.AccountByID(l.ctx, l.pool, "alice")
	wantErr(t, err, ledger.ErrInvalid)
}
