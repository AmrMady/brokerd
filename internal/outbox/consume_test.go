package outbox_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/outbox"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestHandle(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	e := outbox.Event{ID: "11111111-1111-1111-1111-111111111111"}
	var calls atomic.Int32
	count := func(context.Context, pgx.Tx, outbox.Event) error {
		calls.Add(1)
		return nil
	}

	failed := errors.New("handler failed")
	ran, err := outbox.Handle(t.Context(), pool, "audit", e, func(context.Context, pgx.Tx, outbox.Event) error { return failed })
	if !errors.Is(err, failed) || ran {
		t.Fatalf("failing handler: ran %t, %v", ran, err)
	}
	for i, want := range []bool{true, false} {
		if ran, err = outbox.Handle(t.Context(), pool, "audit", e, count); err != nil || ran != want {
			t.Errorf("delivery %d: ran %t, %v; want %t", i+2, ran, err, want)
		}
	}
	if ran, err = outbox.Handle(t.Context(), pool, "billing", e, count); err != nil || !ran {
		t.Errorf("another consumer: ran %t, %v; want it to handle the event too", ran, err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("handled %d times, want once per consumer", n)
	}
}

// Copies of an event delivered at the same time are handled once.
func TestHandleConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	e := outbox.Event{ID: "22222222-2222-2222-2222-222222222222"}
	var (
		calls atomic.Int32
		wg    sync.WaitGroup
	)
	for range 10 {
		wg.Go(func() {
			_, err := outbox.Handle(t.Context(), pool, "audit", e, func(context.Context, pgx.Tx, outbox.Event) error {
				calls.Add(1)
				time.Sleep(10 * time.Millisecond) // hold the claim while the others arrive
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Errorf("handled %d times, want once", n)
	}
}

func TestForgetConsumed(t *testing.T) {
	t.Parallel()
	pool := pgtest.NewPool(t)
	_, err := pool.Exec(t.Context(), `
		INSERT INTO consumed_events (consumer, event_id, consumed_at) VALUES
		('audit', '33333333-3333-3333-3333-333333333333', now() - interval '8 days'),
		('audit', '44444444-4444-4444-4444-444444444444', now())`)
	if err != nil {
		t.Fatal(err)
	}
	n, err := outbox.ForgetConsumed(t.Context(), pool, time.Now().Add(-7*24*time.Hour))
	if err != nil || n != 1 {
		t.Errorf("forgot %d, %v; want the week-old one", n, err)
	}
}
