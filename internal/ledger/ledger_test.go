package ledger_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func TestDepositAndWithdraw(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	d, err := l.deposit("d1", alice, "100.00")
	noErr(t, err)
	if d.Direction != ledger.DirectionDeposit || d.Status != ledger.TransferCompleted {
		t.Errorf("deposit = %+v, want a completed deposit", d)
	}
	_, err = l.withdraw("w1", alice, "30")
	noErr(t, err)
	_, err = l.withdraw("w2", alice, "70.01")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	l.wantBalance(t, alice, ledger.USD, "70", "0")
	l.wantBalance(t, ledger.ExternalBank, ledger.USD, "-70", "0")
	// The deposit, the withdrawal's hold and the withdrawal itself.
	if v := l.balance(t, alice, ledger.USD).Version; v != 3 {
		t.Errorf("alice/USD version = %d, want 3", v)
	}
	l.requireReconciled(t)
}

func TestRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "0")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d2", alice, "1.001")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d3", ledger.Clearing, "100")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d4", "alice", "100")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.deposit("d5", "00000000-0000-4000-8000-000000000000", "100")
	wantErr(t, err, ledger.ErrAccountNotFound)
	_, err = l.deposit("", alice, "100")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.placeHold("h1", alice, "usd", "1")
	wantErr(t, err, ledger.ErrInvalid)
	_, err = l.placeHold("h2", alice, "ZZZZ", "1")
	wantErr(t, err, ledger.ErrInvalid)

	// BRK.A trades in whole shares only.
	noErr(t, l.addAsset("BRK.A", 0))
	_, err = l.placeHold("h3", alice, "BRK.A", "0.5")
	wantErr(t, err, ledger.ErrInvalid)
	wantErr(t, l.addAsset("BRK.A", 9), ledger.ErrInvalid)
}

func TestWithdrawExcludesHeldCash(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "100")
	noErr(t, err)
	_, err = l.placeHold("h1", alice, ledger.USD, "80")
	noErr(t, err)

	_, err = l.withdraw("w1", alice, "30")
	wantErr(t, err, ledger.ErrInsufficientFunds)
	_, err = l.withdraw("w2", alice, "20")
	noErr(t, err)

	b := l.balance(t, alice, ledger.USD)
	if !b.Balance.Equal(dec("80")) || !b.Held.Equal(dec("80")) || !b.Available.IsZero() {
		t.Errorf("alice/USD = %+v, want balance 80, held 80, available 0", b)
	}
	l.requireReconciled(t)
}

func TestPlaceAndReleaseHold(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "100")
	noErr(t, err)
	h, err := l.placeHold("h1", alice, ledger.USD, "60")
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "100", "60")

	_, err = l.placeHold("h2", alice, ledger.USD, "50")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	released, err := l.releaseHold(h.ID)
	noErr(t, err)
	if released.Status != ledger.HoldReleased || released.ClosedAt.IsZero() {
		t.Errorf("released hold = %+v, want status released with a close time", released)
	}
	l.wantBalance(t, alice, ledger.USD, "100", "0")

	_, err = l.releaseHold(h.ID)
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "100", "0")

	_, err = l.releaseHold(123456)
	wantErr(t, err, ledger.ErrHoldNotFound)

	// No shares to hold, and the AAPL account created for the hold is rolled back.
	_, err = l.placeHold("h3", alice, "AAPL", "1")
	wantErr(t, err, ledger.ErrInsufficientFunds)
	balances, err := ledger.Balances(l.ctx, l.pool, alice)
	noErr(t, err)
	if len(balances) != 1 {
		t.Errorf("alice has %d accounts, want only USD: %+v", len(balances), balances)
	}
	l.requireReconciled(t)
}

func TestSettleBuyFill(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "1000")
	noErr(t, err)
	// 10 shares at $50 plus a $5 fee
	h, err := l.placeHold("h1", alice, ledger.USD, "505")
	noErr(t, err)

	e, err := l.settle(ledger.Fill{
		ID: "fill-1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("10"), Price: dec("49.50"), Fee: dec("5"),
	})
	noErr(t, err)
	if e.Kind != ledger.KindTradeFill || e.Metadata["notional"] != "495" {
		t.Errorf("fill entry = %+v, want a trade_fill with notional 495", e)
	}

	// The fill used 500 of the 505. The rest stays held until the order is
	// done and the hold is released.
	l.wantBalance(t, alice, ledger.USD, "500", "5")
	l.wantBalance(t, alice, "AAPL", "10", "0")
	l.wantBalance(t, ledger.Clearing, ledger.USD, "495", "0")
	l.wantBalance(t, ledger.Clearing, "AAPL", "-10", "0")
	l.wantBalance(t, ledger.Fees, ledger.USD, "5", "0")
	if e.HoldID != h.ID {
		t.Errorf("entry hold = %d, want %d", e.HoldID, h.ID)
	}

	released, err := l.releaseHold(h.ID)
	noErr(t, err)
	if released.Status != ledger.HoldReleased || !released.Remaining.Equal(dec("5")) {
		t.Errorf("released hold = %+v, want released with 5 remaining", released)
	}
	l.wantBalance(t, alice, ledger.USD, "500", "0")
	l.requireReconciled(t)
}

func TestSettleSellFill(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	_, err := l.deposit("d1", alice, "1000")
	noErr(t, err)
	buyHold, err := l.placeHold("h1", alice, ledger.USD, "500")
	noErr(t, err)
	_, err = l.settle(ledger.Fill{
		ID: "fill-1", HoldID: buyHold.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("10"), Price: dec("50"), Fee: dec("0"),
	})
	noErr(t, err)

	sellHold, err := l.placeHold("h2", alice, "AAPL", "4")
	noErr(t, err)
	l.wantBalance(t, alice, "AAPL", "10", "4")
	_, err = l.settle(ledger.Fill{
		ID: "fill-2", HoldID: sellHold.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Sell,
		Quantity: dec("4"), Price: dec("60"), Fee: dec("1"),
	})
	noErr(t, err)

	l.wantBalance(t, alice, "AAPL", "6", "0")
	l.wantBalance(t, alice, ledger.USD, "739", "0") // 1000 - 500 + 240 - 1
	l.wantBalance(t, ledger.Clearing, "AAPL", "-6", "0")
	l.wantBalance(t, ledger.Clearing, ledger.USD, "260", "0")
	l.wantBalance(t, ledger.Fees, ledger.USD, "1", "0")
	l.requireReconciled(t)
}

func TestSettleFillHoldRules(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	bob := l.openAccount(t)

	l.mustDeposit(t, "d1", alice, "1000")
	l.mustDeposit(t, "d2", bob, "1000")
	buy := func(id string, holdID int64, price string) ledger.Fill {
		return ledger.Fill{
			ID: id, HoldID: holdID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
			Quantity: dec("1"), Price: dec(price), Fee: dec("1"),
		}
	}

	h1, err := l.placeHold("h1", alice, ledger.USD, "100")
	noErr(t, err)
	_, err = l.settle(buy("f1", h1.ID, "100")) // needs 101
	wantErr(t, err, ledger.ErrHoldExceeded)
	h, err := ledger.HoldByID(l.ctx, l.pool, h1.ID)
	noErr(t, err)
	if h.Status != ledger.HoldActive {
		t.Errorf("hold after failed fill is %s, want still active", h.Status)
	}

	_, err = l.releaseHold(h1.ID)
	noErr(t, err)
	_, err = l.settle(buy("f2", h1.ID, "50"))
	wantErr(t, err, ledger.ErrHoldClosed)

	// A hold that fills have used up is settled and can't be released.
	h2, err := l.placeHold("h2", alice, ledger.USD, "51")
	noErr(t, err)
	_, err = l.settle(buy("f3", h2.ID, "50"))
	noErr(t, err)
	_, err = l.releaseHold(h2.ID)
	wantErr(t, err, ledger.ErrHoldClosed)

	bobs, err := l.placeHold("h3", bob, ledger.USD, "100")
	noErr(t, err)
	_, err = l.settle(buy("f4", bobs.ID, "50"))
	wantErr(t, err, ledger.ErrInvalid)

	_, err = l.settle(buy("f5", 123456, "50"))
	wantErr(t, err, ledger.ErrHoldNotFound)

	h4, err := l.placeHold("h4", alice, ledger.USD, "100")
	noErr(t, err)
	sell := buy("f6", h4.ID, "50")
	sell.Side = ledger.Sell
	_, err = l.settle(sell)
	wantErr(t, err, ledger.ErrInvalid)

	l.wantBalance(t, alice, ledger.USD, "949", "100") // 1000 - 51; h4 still active
	l.requireReconciled(t)
}

func TestHistoryPagesNewestFirst(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	for i := 1; i <= 25; i++ {
		_, err := l.deposit(fmt.Sprintf("d%d", i), alice, fmt.Sprint(i))
		noErr(t, err)
	}

	var (
		versions []int64
		sizes    []int
		before   int64
	)
	for {
		page, err := ledger.History(l.ctx, l.pool, alice, ledger.USD, before, 10)
		noErr(t, err)
		sizes = append(sizes, len(page.Items))
		for _, it := range page.Items {
			if it.Kind != ledger.KindDeposit || !it.Amount.Equal(dec(fmt.Sprint(it.Version))) {
				t.Errorf("item %+v: want deposit of %d", it, it.Version)
			}
			versions = append(versions, it.Version)
		}
		if page.Next == 0 {
			break
		}
		before = page.Next
	}

	if fmt.Sprint(sizes) != "[10 10 5]" {
		t.Errorf("page sizes = %v, want [10 10 5]", sizes)
	}
	for i, v := range versions {
		if v != int64(25-i) {
			t.Fatalf("versions = %v, want 25 down to 1", versions)
		}
	}

	page, err := ledger.History(l.ctx, l.pool, l.openAccount(t), ledger.USD, 0, 10)
	noErr(t, err)
	if len(page.Items) != 0 || page.Next != 0 {
		t.Errorf("history of a new account = %+v, want empty", page)
	}
	_, err = ledger.History(l.ctx, l.pool, alice, ledger.USD, 0, ledger.MaxPageSize+1)
	wantErr(t, err, ledger.ErrInvalid)
}

func (l testLedger) exec(t *testing.T, sql string, args ...any) error {
	t.Helper()
	return pgx.BeginFunc(l.ctx, l.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(l.ctx, sql, args...)
		return err
	})
}

func (l testLedger) accountID(t *testing.T, owner string, asset ledger.Asset) int64 {
	t.Helper()
	var id int64
	err := l.pool.QueryRow(l.ctx, `SELECT id FROM ledger_accounts WHERE account_id = $1 AND asset = $2`, owner, asset).Scan(&id)
	noErr(t, err)
	return id
}

// forgeLine inserts a one-line entry adding $1 to account $2.
const forgeLine = `
	WITH e AS (
		INSERT INTO journal_entries (kind) VALUES ('deposit') RETURNING id
	)
	INSERT INTO journal_lines (entry_id, ledger_account_id, asset, amount, account_version)
	SELECT id, $2, 'USD', $1, 1000 FROM e`

func TestReconcileReportsTampering(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		tamper func(t *testing.T, l testLedger, cash int64)
		want   func(owner string) []ledger.Mismatch
	}{
		{
			name: "balance edited",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `UPDATE ledger_accounts SET balance = balance + 5 WHERE id = $1`, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckBalance, AccountID: owner, Asset: ledger.USD, Want: dec("100"), Got: dec("105")},
				}
			},
		},
		{
			name: "held edited",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `UPDATE ledger_accounts SET held = 7 WHERE id = $1`, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckHeld, AccountID: owner, Asset: ledger.USD, Want: dec("0"), Got: dec("7")},
				}
			},
		},
		{
			name: "one-sided line",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `ALTER TABLE journal_lines DISABLE TRIGGER journal_lines_balanced`))
				noErr(t, l.exec(t, forgeLine, 1, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckAssetTotal, Asset: ledger.USD, Want: dec("0"), Got: dec("1")},
					{Check: ledger.CheckBalance, AccountID: owner, Asset: ledger.USD, Want: dec("101"), Got: dec("100")},
				}
			},
		},
		{
			name: "negative available",
			tamper: func(t *testing.T, l testLedger, cash int64) {
				noErr(t, l.exec(t, `ALTER TABLE ledger_accounts DROP CONSTRAINT ledger_accounts_available_non_negative`))
				noErr(t, l.exec(t, `UPDATE ledger_accounts SET held = 150 WHERE id = $1`, cash))
			},
			want: func(owner string) []ledger.Mismatch {
				return []ledger.Mismatch{
					{Check: ledger.CheckHeld, AccountID: owner, Asset: ledger.USD, Want: dec("0"), Got: dec("150")},
					{Check: ledger.CheckAvailable, AccountID: owner, Asset: ledger.USD, Want: dec("0"), Got: dec("-50")},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := newTestLedger(t)
			owner := l.openAccount(t)
			l.mustDeposit(t, "d1", owner, "100")
			l.requireReconciled(t)

			tt.tamper(t, l, l.accountID(t, owner, ledger.USD))

			report, err := ledger.Reconcile(l.ctx, l.pool)
			noErr(t, err)
			if want := tt.want(owner); !sameMismatches(report.Mismatches, want) {
				t.Errorf("mismatches = %+v\nwant %+v", report.Mismatches, want)
			}
		})
	}
}

func sameMismatches(got, want []ledger.Mismatch) bool {
	return slices.EqualFunc(got, want, func(g, w ledger.Mismatch) bool {
		return g.Check == w.Check && g.AccountID == w.AccountID && g.Asset == w.Asset && g.Want.Equal(w.Want) && g.Got.Equal(w.Got)
	})
}

func TestDatabaseBackstops(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	owner := l.openAccount(t)
	l.mustDeposit(t, "d1", owner, "100")
	cash := l.accountID(t, owner, ledger.USD)

	wantSQLState := func(t *testing.T, err error, code string) {
		t.Helper()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Errorf("got %v, want SQLSTATE %s", err, code)
		}
	}
	const checkViolation, raiseException = "23514", "P0001"

	t.Run("unbalanced entry fails at commit", func(t *testing.T) {
		wantSQLState(t, l.exec(t, forgeLine, 1, cash), checkViolation)
	})
	t.Run("entry without lines fails at commit", func(t *testing.T) {
		err := l.exec(t, `INSERT INTO journal_entries (kind) VALUES ('deposit')`)
		wantSQLState(t, err, checkViolation)
	})
	t.Run("journal is append-only", func(t *testing.T) {
		for _, sql := range []string{
			`UPDATE journal_lines SET amount = amount * 2`,
			`DELETE FROM journal_lines`,
			`DELETE FROM journal_entries`,
			`TRUNCATE journal_lines`,
		} {
			wantSQLState(t, l.exec(t, sql), raiseException)
		}
	})
	t.Run("customer balance cannot go negative", func(t *testing.T) {
		err := l.exec(t, `UPDATE ledger_accounts SET balance = -1 WHERE id = $1`, cash)
		wantSQLState(t, err, checkViolation)
	})
	l.requireReconciled(t)
}

func TestAssetScale(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	for asset, want := range map[ledger.Asset]int32{ledger.USD: 2, "AAPL": 9} {
		got, err := ledger.AssetScale(l.ctx, l.pool, asset)
		if err != nil || got != want {
			t.Errorf("AssetScale(%s) = %d, %v; want %d", asset, got, err, want)
		}
	}
	_, err := ledger.AssetScale(l.ctx, l.pool, "NOPE")
	wantErr(t, err, ledger.ErrInvalid)
}
