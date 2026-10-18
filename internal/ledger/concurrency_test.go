package ledger_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

// 1,000 goroutines place holds on one account and then settle or release them.
// The final balances and versions must match the successful operations exactly.
// It has a pool big enough for the goroutines to contend in Postgres rather
// than wait for a connection, and doesn't run in parallel with other tests.
func TestConcurrentHoldsAndFills(t *testing.T) {
	l := newTestLedgerOf(t, pgtest.NewPoolSize(t, 200))
	owner := l.openAccount(t)
	l.mustDeposit(t, "d0", owner, "50000")

	var (
		mu         sync.Mutex
		wantCash   = dec("50000")
		wantShares = decimal.Zero
		// each successful write to a balance row bumps its version
		cashWrites  int64 = 1 // the deposit
		shareWrites int64

		fills, rejected atomic.Int64
	)
	record := func(cash, shares decimal.Decimal, cashW, shareW int64) {
		mu.Lock()
		defer mu.Unlock()
		wantCash = wantCash.Add(cash)
		wantShares = wantShares.Add(shares)
		cashWrites += cashW
		shareWrites += shareW
	}

	peak := peakConns(l.pool)
	runConcurrently(1000, func(i int) {
		holdKey, fillID := fmt.Sprintf("hold-%d", i), fmt.Sprintf("fill-%d", i)
		price := decimal.New(int64(90+i%10), 0)
		fee := dec("1")
		switch i % 4 {
		case 0, 1: // Buy one share: reserve $100 + $1 fee, fill at $90-99, release the rest.
			h, err := l.placeHold(holdKey, owner, ledger.USD, "101")
			if errors.Is(err, ledger.ErrInsufficientFunds) {
				rejected.Add(1)
				return
			}
			if err != nil {
				t.Errorf("hold %d: %v", i, err)
				return
			}
			_, err = l.settle(ledger.Fill{
				ID: fillID, HoldID: h.ID, AccountID: owner, Symbol: "AAPL", Side: ledger.Buy,
				Quantity: dec("1"), Price: price, Fee: fee,
			})
			if err != nil {
				t.Errorf("buy %d: %v", i, err)
				return
			}
			if _, err = l.releaseHold(h.ID); err != nil {
				t.Errorf("release after buy %d: %v", i, err)
				return
			}
			fills.Add(1)
			record(price.Add(fee).Neg(), dec("1"), 3, 1)

		case 2: // Reserve cash for an order, then cancel it.
			h, err := l.placeHold(holdKey, owner, ledger.USD, "101")
			if errors.Is(err, ledger.ErrInsufficientFunds) {
				rejected.Add(1)
				return
			}
			if err != nil {
				t.Errorf("hold %d: %v", i, err)
				return
			}
			if _, err = l.releaseHold(h.ID); err != nil {
				t.Errorf("release %d: %v", i, err)
				return
			}
			record(decimal.Zero, decimal.Zero, 2, 0)

		case 3: // Sell one share, if one is free to sell.
			h, err := l.placeHold(holdKey, owner, "AAPL", "1")
			if errors.Is(err, ledger.ErrInsufficientFunds) {
				rejected.Add(1)
				return
			}
			if err != nil {
				t.Errorf("hold %d: %v", i, err)
				return
			}
			_, err = l.settle(ledger.Fill{
				ID: fillID, HoldID: h.ID, AccountID: owner, Symbol: "AAPL", Side: ledger.Sell,
				Quantity: dec("1"), Price: price, Fee: fee,
			})
			if err != nil {
				t.Errorf("sell %d: %v", i, err)
				return
			}
			fills.Add(1)
			record(price.Sub(fee), dec("-1"), 1, 2)
		}
	})

	if n, most := peak(), l.pool.Config().MaxConns; n < most/2 {
		t.Errorf("at most %d of %d connections in use at once, want the goroutines contending in Postgres, not for connections", n, most)
	}
	t.Logf("%d fills settled, %d holds rejected for insufficient funds", fills.Load(), rejected.Load())
	if fills.Load() == 0 {
		t.Fatal("no fill succeeded")
	}

	cash := l.balance(t, owner, ledger.USD)
	shares := l.balance(t, owner, "AAPL")
	if !cash.Balance.Equal(wantCash) || !cash.Held.IsZero() || cash.Version != cashWrites {
		t.Errorf("cash = %s held %s version %d, want %s held 0 version %d",
			cash.Balance, cash.Held, cash.Version, wantCash, cashWrites)
	}
	if !shares.Balance.Equal(wantShares) || !shares.Held.IsZero() || shares.Version != shareWrites {
		t.Errorf("shares = %s held %s version %d, want %s held 0 version %d",
			shares.Balance, shares.Held, shares.Version, wantShares, shareWrites)
	}
	if cash.Balance.IsNegative() || shares.Balance.IsNegative() {
		t.Errorf("negative balance: cash %s, shares %s", cash.Balance, shares.Balance)
	}
	l.requireReconciled(t)
}

// Fifty customers trade at once, and every fill posts to the same system
// accounts: clearing, fees and the fee payables. Writes lock those rows in
// one order, so none of the transactions deadlock, and the books balance.
func TestManyCustomersTradeAtOnce(t *testing.T) {
	l := newTestLedgerOf(t, pgtest.NewPoolSize(t, 100))
	noErr(t, l.addFeeRate(ledger.SECFee, "2026-01-01", "0.0000278", ""))
	noErr(t, l.addFeeRate(ledger.TAF, "2026-01-01", "0.000166", "8.30"))
	const customers, rounds = 50, 5
	ids := make([]string, customers)
	for i := range ids {
		ids[i] = l.openAccount(t)
		l.mustDeposit(t, "d", ids[i], "10000")
	}

	peak := peakConns(l.pool)
	runConcurrently(customers, func(i int) {
		trade := func(key string, side ledger.Side, symbol ledger.Asset, price string) error {
			asset, amount := ledger.USD, "101" // a share at 100, and the commission
			if side == ledger.Sell {
				asset, amount = symbol, "1"
			}
			h, err := l.placeHold("hold-"+key, ids[i], asset, amount)
			if err != nil {
				return err
			}
			_, err = l.settle(ledger.Fill{
				ID: key, HoldID: h.ID, AccountID: ids[i], Symbol: symbol, Side: side,
				Quantity: dec("1"), Price: dec(price), Fee: dec("1"),
			})
			return err
		}
		for r := range rounds {
			for _, symbol := range []ledger.Asset{"AAPL", "MSFT"} {
				key := fmt.Sprintf("%s-%d", symbol, r)
				if err := trade("buy-"+key, ledger.Buy, symbol, "100"); err != nil {
					t.Errorf("customer %d: buy %s: %v", i, key, err)
					return
				}
				if err := trade("sell-"+key, ledger.Sell, symbol, "101"); err != nil {
					t.Errorf("customer %d: sell %s: %v", i, key, err)
					return
				}
			}
		}
	})
	if n, most := peak(), min(customers, l.pool.Config().MaxConns); n < most/2 {
		t.Errorf("at most %d transactions ran at once, want at least %d", n, most/2)
	}

	// Each round trip costs a customer the two commissions, less the dollar
	// gained, plus a cent each of SEC fee and TAF on the sale.
	for _, id := range ids {
		l.wantBalance(t, id, ledger.USD, "9989.8", "0")
		l.wantBalance(t, id, "AAPL", "0", "0")
		l.wantBalance(t, id, "MSFT", "0", "0")
	}
	l.wantBalance(t, ledger.Fees, ledger.USD, "1000", "0")
	l.wantBalance(t, ledger.SECFeePayable, ledger.USD, "5", "0")
	l.wantBalance(t, ledger.TAFPayable, ledger.USD, "5", "0")
	l.requireReconciled(t)
}
