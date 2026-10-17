package ledger_test

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func (l testLedger) setStatus(id string, status ledger.AccountStatus) (ledger.Account, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Account, error) {
		return ledger.SetAccountStatus(l.ctx, tx, id, status, "test")
	})
}

func (l testLedger) closeAccount(id string) (ledger.Account, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Account, error) {
		return ledger.CloseAccount(l.ctx, tx, id, "customer request")
	})
}

func TestOpenAccount(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)

	a, err := inTx(l, func(tx pgx.Tx) (ledger.Account, error) { return ledger.OpenAccount(l.ctx, tx) })
	noErr(t, err)
	b, err := inTx(l, func(tx pgx.Tx) (ledger.Account, error) { return ledger.OpenAccount(l.ctx, tx) })
	noErr(t, err)

	number := regexp.MustCompile(`^\d{10}$`)
	if a.Status != ledger.AccountActive || !number.MatchString(a.Number) {
		t.Errorf("new account = %+v, want active with a 10-digit number", a)
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

func TestRestrictedAccount(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	h := l.mustHold(t, "h1", alice, ledger.USD, "505")

	_, err := l.setStatus(alice, ledger.AccountRestricted)
	noErr(t, err)

	// Money can still come in and existing orders still settle, but nothing
	// new can go out.
	_, err = l.deposit("d2", alice, "100")
	noErr(t, err)
	_, err = l.withdraw("w1", alice, "10")
	wantErr(t, err, ledger.ErrAccountRestricted)
	_, err = l.placeHold("h2", alice, ledger.USD, "10")
	wantErr(t, err, ledger.ErrAccountRestricted)
	_, err = l.settle(ledger.Fill{
		ID: "fill-1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("10"), Price: dec("50"), Fee: dec("5"),
	})
	noErr(t, err)

	_, err = l.setStatus(alice, ledger.AccountActive)
	noErr(t, err)
	_, err = l.withdraw("w2", alice, "10")
	noErr(t, err)

	_, err = l.setStatus(alice, ledger.AccountClosed)
	wantErr(t, err, ledger.ErrInvalid)
	_, err = inTx(l, func(tx pgx.Tx) (ledger.Account, error) {
		return ledger.SetAccountStatus(l.ctx, tx, alice, ledger.AccountRestricted, "")
	})
	wantErr(t, err, ledger.ErrInvalid)

	// Setting the status an account already has records nothing.
	_, err = l.setStatus(alice, ledger.AccountActive)
	noErr(t, err)
	changes, err := ledger.AccountStatusChanges(l.ctx, l.pool, alice)
	noErr(t, err)
	if len(changes) != 2 || changes[0].To != ledger.AccountRestricted || changes[1].To != ledger.AccountActive ||
		changes[1].From != ledger.AccountRestricted || changes[1].Reason != "test" {
		t.Errorf("status history = %+v, want restricted then active", changes)
	}

	l.wantBalance(t, alice, ledger.USD, "585", "0") // 1000 + 100 - 505 - 10
	l.requireReconciled(t)
}

func TestCloseAccount(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	d1, err := l.deposit("d1", alice, "100")
	noErr(t, err)
	h := l.mustHold(t, "h1", alice, ledger.USD, "40")

	_, err = l.closeAccount(alice)
	wantErr(t, err, ledger.ErrAccountNotEmpty)
	_, err = l.releaseHold(h.ID)
	noErr(t, err)
	_, err = l.closeAccount(alice)
	wantErr(t, err, ledger.ErrAccountNotEmpty)
	_, err = l.withdraw("w1", alice, "100")
	noErr(t, err)

	a, err := l.closeAccount(alice)
	noErr(t, err)
	if a.Status != ledger.AccountClosed {
		t.Fatalf("status after close = %s, want closed", a.Status)
	}
	_, err = l.closeAccount(alice)
	noErr(t, err)

	_, err = l.deposit("d2", alice, "100")
	wantErr(t, err, ledger.ErrAccountClosed)
	_, err = l.setStatus(alice, ledger.AccountActive)
	wantErr(t, err, ledger.ErrAccountClosed)

	// Retries of requests made before the account closed still return their
	// original result.
	replayed, err := l.deposit("d1", alice, "100")
	noErr(t, err)
	if replayed.ID != d1.ID {
		t.Errorf("replayed deposit returned entry %d, want %d", replayed.ID, d1.ID)
	}

	changes, err := ledger.AccountStatusChanges(l.ctx, l.pool, alice)
	noErr(t, err)
	if len(changes) != 1 || changes[0].To != ledger.AccountClosed || changes[0].Reason != "customer request" {
		t.Errorf("status history = %+v, want one change to closed", changes)
	}
	l.wantBalance(t, alice, ledger.USD, "0", "0")
	l.requireReconciled(t)
}

// Workers deposit and then withdraw the same amount while another goroutine
// keeps trying to close the account. A posting that got in after the close
// would leave money in a closed account.
func TestCloseAccountRacesWithPostings(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	const workers = 10
	var (
		deposited = make(chan struct{})
		once      sync.Once
		running   atomic.Int64
	)
	running.Store(workers)
	runConcurrently(workers+1, func(i int) {
		if i == 0 {
			<-deposited
			for {
				done := running.Load() == 0
				_, err := l.closeAccount(alice)
				if err == nil {
					return
				}
				if !errors.Is(err, ledger.ErrAccountNotEmpty) || done {
					t.Errorf("close: %v", err)
					return
				}
			}
		}
		defer running.Add(-1)
		for j := range 20 {
			key := fmt.Sprintf("%d-%d", i, j)
			_, err := l.deposit("d"+key, alice, "10")
			if errors.Is(err, ledger.ErrAccountClosed) {
				return
			}
			if err != nil {
				t.Errorf("deposit %s: %v", key, err)
				return
			}
			once.Do(func() { close(deposited) })
			if _, err = l.withdraw("w"+key, alice, "10"); err != nil {
				t.Errorf("withdraw %s after its deposit: %v", key, err)
				return
			}
		}
	})

	a, err := ledger.AccountByID(l.ctx, l.pool, alice)
	noErr(t, err)
	if a.Status != ledger.AccountClosed {
		t.Fatalf("status = %s, want closed", a.Status)
	}
	l.wantBalance(t, alice, ledger.USD, "0", "0")
	l.requireReconciled(t)
}
