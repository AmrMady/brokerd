package ledger_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// testLedger runs each call in its own transaction.
type testLedger struct {
	ctx  context.Context
	pool *pgxpool.Pool
}

func newTestLedger(t *testing.T) testLedger {
	return testLedger{ctx: t.Context(), pool: pgtest.NewPool(t)}
}

func inTx[T any](l testLedger, fn func(pgx.Tx) (T, error)) (T, error) {
	var out T
	err := pgx.BeginFunc(l.ctx, l.pool, func(tx pgx.Tx) error {
		var err error
		out, err = fn(tx)
		return err
	})
	return out, err
}

func (l testLedger) openAccount(t *testing.T) string {
	t.Helper()
	a, err := inTx(l, func(tx pgx.Tx) (ledger.Account, error) {
		return ledger.OpenAccount(l.ctx, tx)
	})
	if err != nil {
		t.Fatalf("open account: %v", err)
	}
	return a.ID
}

func (l testLedger) deposit(key, owner, amount string) (ledger.Entry, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Entry, error) {
		return ledger.Deposit(l.ctx, tx, ledger.CashTransfer{IdempotencyKey: key, AccountID: owner, Amount: dec(amount)})
	})
}

func (l testLedger) withdraw(key, owner, amount string) (ledger.Entry, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Entry, error) {
		return ledger.Withdraw(l.ctx, tx, ledger.CashTransfer{IdempotencyKey: key, AccountID: owner, Amount: dec(amount)})
	})
}

func (l testLedger) mustDeposit(t *testing.T, key, owner, amount string) {
	t.Helper()
	if _, err := l.deposit(key, owner, amount); err != nil {
		t.Fatalf("deposit %s to %s: %v", amount, owner, err)
	}
}

func (l testLedger) balance(t *testing.T, owner string, asset ledger.Asset) ledger.Balance {
	t.Helper()
	balances, err := ledger.Balances(l.ctx, l.pool, owner)
	if err != nil {
		t.Fatalf("balances of %s: %v", owner, err)
	}
	for _, b := range balances {
		if b.Asset == asset {
			return b
		}
	}
	return ledger.Balance{AccountID: owner, Asset: asset}
}

func (l testLedger) wantBalance(t *testing.T, owner string, asset ledger.Asset, balance, held string) {
	t.Helper()
	b := l.balance(t, owner, asset)
	if !b.Balance.Equal(dec(balance)) || !b.Held.Equal(dec(held)) {
		t.Errorf("%s/%s: balance %s held %s, want balance %s held %s",
			owner, asset, b.Balance, b.Held, balance, held)
	}
}

func (l testLedger) requireReconciled(t *testing.T) {
	t.Helper()
	report, err := ledger.Reconcile(l.ctx, l.pool)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, m := range report.Mismatches {
		t.Errorf("reconcile: %s mismatch on %s/%s: want %s, got %s", m.Check, m.AccountID, m.Asset, m.Want, m.Got)
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got error %v, want %v", err, want)
	}
}

// runConcurrently starts n goroutines at the same time and waits for them.
func runConcurrently(n int, fn func(i int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			fn(i)
		})
	}
	close(start)
	wg.Wait()
}
