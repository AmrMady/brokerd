package ledger_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

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
	t.Helper()
	return newTestLedgerOf(t, pgtest.NewPool(t))
}

// newTestLedgerOf sets up a ledger in pool's database.
func newTestLedgerOf(t *testing.T, pool *pgxpool.Pool) testLedger {
	t.Helper()
	l := testLedger{ctx: t.Context(), pool: pool}
	for _, symbol := range []ledger.Asset{"AAPL", "MSFT"} {
		if err := l.addAsset(symbol, 9); err != nil {
			t.Fatalf("add asset %s: %v", symbol, err)
		}
	}
	// Regulatory fees are zero unless a test sets a rate.
	noErr(t, l.addFeeRate(ledger.SECFee, "2000-01-01", "0", ""))
	noErr(t, l.addFeeRate(ledger.TAF, "2000-01-01", "0", "0"))
	return l
}

func (l testLedger) addFeeRate(fee ledger.RegulatoryFee, effectiveOn, rate, maxPerTrade string) error {
	r := ledger.FeeRate{Fee: fee, Rate: dec(rate)}
	var err error
	if r.EffectiveOn, err = time.Parse(time.DateOnly, effectiveOn); err != nil {
		return err
	}
	if maxPerTrade != "" {
		r.MaxPerTrade = decimal.NewNullDecimal(dec(maxPerTrade))
	}
	return l.addRate(r)
}

func (l testLedger) addRate(r ledger.FeeRate) error {
	return pgx.BeginFunc(l.ctx, l.pool, func(tx pgx.Tx) error {
		return ledger.AddFeeRate(l.ctx, tx, r)
	})
}

func (l testLedger) addAsset(symbol ledger.Asset, scale int32) error {
	return pgx.BeginFunc(l.ctx, l.pool, func(tx pgx.Tx) error {
		return ledger.AddAsset(l.ctx, tx, symbol, scale)
	})
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

// deposit and withdraw request a transfer and complete it in the same
// transaction, as if the bank confirmed it right away.
func (l testLedger) deposit(key, account, amount string) (ledger.Transfer, error) {
	return l.transfer(ledger.RequestDeposit, key, account, amount)
}

func (l testLedger) withdraw(key, account, amount string) (ledger.Transfer, error) {
	return l.transfer(ledger.RequestWithdrawal, key, account, amount)
}

type transferRequest func(context.Context, pgx.Tx, ledger.CashTransfer) (ledger.Transfer, error)

func (l testLedger) transfer(request transferRequest, key, account, amount string) (ledger.Transfer, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Transfer, error) {
		tr, err := request(l.ctx, tx, ledger.CashTransfer{IdempotencyKey: key, AccountID: account, Amount: dec(amount)})
		if err != nil {
			return tr, err
		}
		return ledger.CompleteTransfer(l.ctx, tx, tr.ID)
	})
}

func (l testLedger) placeHold(key, owner string, asset ledger.Asset, amount string) (ledger.Hold, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Hold, error) {
		return ledger.PlaceHold(l.ctx, tx, ledger.HoldRequest{IdempotencyKey: key, AccountID: owner, Asset: asset, Amount: dec(amount)})
	})
}

func (l testLedger) releaseHold(id int64) (ledger.Hold, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Hold, error) {
		return ledger.ReleaseHold(l.ctx, tx, id)
	})
}

// Fills in tests that aren't about settlement trade on a Friday and settle
// the following Monday.
var (
	tradeDate  = time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	settleDate = time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
)

func (l testLedger) settle(f ledger.Fill) (ledger.Entry, error) {
	if f.TradeDate.IsZero() {
		f.TradeDate, f.SettleDate = tradeDate, settleDate
	}
	return inTx(l, func(tx pgx.Tx) (ledger.Entry, error) {
		return ledger.SettleFill(l.ctx, tx, f)
	})
}

func (l testLedger) settleDue(asOf time.Time) (int, error) {
	return inTx(l, func(tx pgx.Tx) (int, error) {
		return ledger.SettleDue(l.ctx, tx, asOf, 100)
	})
}

func (l testLedger) mustDeposit(t *testing.T, key, owner, amount string) {
	t.Helper()
	if _, err := l.deposit(key, owner, amount); err != nil {
		t.Fatalf("deposit %s to %s: %v", amount, owner, err)
	}
}

func (l testLedger) mustHold(t *testing.T, key, owner string, asset ledger.Asset, amount string) ledger.Hold {
	t.Helper()
	h, err := l.placeHold(key, owner, asset, amount)
	if err != nil {
		t.Fatalf("hold %s %s for %s: %v", amount, asset, owner, err)
	}
	return h
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

// wantRestricted checks that the account was restricted once, for reason.
func (l testLedger) wantRestricted(t *testing.T, account, reason string) {
	t.Helper()
	a, err := ledger.AccountByID(l.ctx, l.pool, account)
	noErr(t, err)
	if a.Status != ledger.AccountRestricted {
		t.Errorf("account is %s, want restricted", a.Status)
	}
	changes, err := ledger.AccountStatusChanges(l.ctx, l.pool, account)
	noErr(t, err)
	if len(changes) != 1 || changes[0].Reason != reason {
		t.Errorf("status history = %+v, want one change for %q", changes, reason)
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

// peakConns samples how many of the pool's connections are in use, until
// the returned function is called, which returns the most it saw.
func peakConns(pool *pgxpool.Pool) func() int32 {
	done, peak := make(chan struct{}), make(chan int32)
	go func() {
		var most int32
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				peak <- most
				return
			case <-ticker.C:
				most = max(most, pool.Stat().AcquiredConns())
			}
		}
	}()
	return func() int32 {
		close(done)
		return <-peak
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
