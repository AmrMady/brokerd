package migrations_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
	"github.com/AmrMady/brokerd/migrations"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// rerun applies migration version again, on a database already past it: it
// goes down to the one before and back up.
func rerun(t *testing.T, pool *pgxpool.Pool, version int64) {
	t.Helper()
	db := stdlib.OpenDBFromPool(pool)
	defer func() { noErr(t, db.Close()) }()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	noErr(t, err)
	_, err = p.DownTo(t.Context(), version-1)
	noErr(t, err)
	_, err = p.Up(t.Context())
	noErr(t, err)
}

func inTx[T any](t *testing.T, pool *pgxpool.Pool, fn func(context.Context, pgx.Tx) (T, error)) T {
	t.Helper()
	var out T
	noErr(t, pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		var err error
		out, err = fn(t.Context(), tx)
		return err
	}))
	return out
}

// account opens an account with cash in it.
func account(t *testing.T, pool *pgxpool.Pool, cash string) string {
	t.Helper()
	return inTx(t, pool, func(ctx context.Context, tx pgx.Tx) (string, error) {
		a, err := ledger.OpenAccount(ctx, tx)
		if err != nil {
			return "", err
		}
		tr, err := ledger.RequestDeposit(ctx, tx, ledger.CashTransfer{IdempotencyKey: "seed", AccountID: a.ID, Amount: dec(cash)})
		if err != nil {
			return "", err
		}
		_, err = ledger.CompleteTransfer(ctx, tx, tr.ID)
		return a.ID, err
	})
}

// A limit buy resting when 00016 added the commission got 0, though its
// hold was sized with the commission configured then. 00020 takes the
// commission back from the hold.
func TestCommissionBackfill(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	noErr(t, pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error { return ledger.AddAsset(t.Context(), tx, "AAPL", 9) }))
	alice := account(t, pool, "100")
	place := func(key, qty, price, held string, side ledger.Side) orders.Order {
		return inTx(t, pool, func(ctx context.Context, tx pgx.Tx) (orders.Order, error) {
			o, _, err := orders.Create(ctx, tx, orders.Request{
				AccountID: alice, Symbol: "AAPL", Side: side, Type: orders.Limit, TimeInForce: orders.GTC,
				Quantity: dec(qty), LimitPrice: decimal.NewNullDecimal(dec(price)), ClientOrderID: key,
			})
			if err != nil {
				return o, err
			}
			asset := ledger.USD
			if side == ledger.Sell {
				asset = "AAPL"
			}
			h, err := ledger.PlaceHold(ctx, tx, ledger.HoldRequest{IdempotencyKey: key, AccountID: alice, Asset: asset, Amount: dec(held)})
			if err != nil {
				return o, err
			}
			return orders.Accept(ctx, tx, o.ID, h.ID, time.Time{}, decimal.Zero) // as 00016 left it
		})
	}
	// 3 shares at 0.3335 cost 1.0005, held as 1.01, plus a 2.50 commission.
	buy := place("buy", "3", "0.3335", "3.51", ledger.Buy)
	noCommission := place("no-commission", "1", "5", "5", ledger.Buy)

	rerun(t, pool, 20)
	for _, tt := range []struct {
		order orders.Order
		want  string
	}{{buy, "2.5"}, {noCommission, "0"}} {
		o, err := orders.Get(t.Context(), pool, tt.order.ID)
		noErr(t, err)
		if !o.Commission.Equal(dec(tt.want)) {
			t.Errorf("%s: commission %s, want %s", o.ClientOrderID, o.Commission, tt.want)
		}
	}
}
