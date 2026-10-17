package ledger_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// 1,000 goroutines place holds on one account and then settle or release them.
// The final balances and versions must match the successful operations exactly.
func TestConcurrentHoldsAndFills(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
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
