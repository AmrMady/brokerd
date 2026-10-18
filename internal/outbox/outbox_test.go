package outbox_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/outbox"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

// An event is in the outbox if and only if its transaction commits.
func TestAdd(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	add := func(key string) func(tx pgx.Tx) error {
		return func(tx pgx.Tx) error {
			return outbox.Add(t.Context(), tx, outbox.EntryPosted, key, map[string]int{"entry_id": 1})
		}
	}
	if err := pgx.BeginFunc(t.Context(), pool, add("committed")); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("posting failed")
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		if addErr := add("rolled-back")(tx); addErr != nil {
			return addErr
		}
		return failed
	})
	if !errors.Is(err, failed) {
		t.Fatal(err)
	}

	var keys []string
	rows, err := pool.Query(context.Background(), `SELECT key FROM outbox ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	if keys, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "committed" {
		t.Errorf("outbox has %v, want only the committed event", keys)
	}
}
