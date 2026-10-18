package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/AmrMady/brokerd/internal/outbox"
	"github.com/AmrMady/brokerd/internal/outbox/kafka"
	"github.com/AmrMady/brokerd/internal/postgres"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Stop()
	os.Exit(code)
}

func startKafka(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Kafka test in -short mode")
	}
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("brokerd-test"))
	if err != nil {
		t.Fatalf("start Kafka: %v", err)
	}
	t.Cleanup(func() {
		if termErr := c.Terminate(context.Background()); termErr != nil {
			t.Error(termErr)
		}
	})
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return brokers[0]
}

// received keeps what a consumer handled, in order.
type received struct {
	mu     sync.Mutex
	events []outbox.Event
}

func (r *received) handle(_ context.Context, _ pgx.Tx, e outbox.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

func (r *received) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []string
	for _, e := range r.events {
		ids = append(ids, e.ID)
	}
	return ids
}

// Events written in transactions reach a consumer once each, in each
// account's order, even when they're published twice.
func TestOutboxThroughKafka(t *testing.T) {
	brokers := startKafka(t)
	url := pgtest.NewURL(t)
	pool, err := postgres.NewPool(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	const prefix = "test."
	types := []string{outbox.EntryPosted, outbox.OrderFilled}
	pub, err := kafka.NewPublisher(brokers, prefix, slogDiscard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if err = pub.EnsureTopics(ctx, types, 3, 1); err != nil {
		t.Fatal(err)
	}

	var want []string
	for i := range 30 {
		typ := types[i%2]
		key := fmt.Sprintf("account-%d", i%4)
		err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return outbox.Add(ctx, tx, typ, key, map[string]int{"n": i})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := pool.Query(ctx, `SELECT id::text FROM outbox ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	if want, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		t.Fatal(err)
	}

	relay, err := outbox.NewRelay(outbox.RelayConfig{DatabaseURL: url, Publisher: pub, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var got received
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		if runErr := relay.Run(runCtx); !errors.Is(runErr, context.Canceled) {
			t.Errorf("relay: %v", runErr)
		}
	})
	wg.Go(func() {
		consumeErr := kafka.Consume(runCtx, kafka.ConsumerConfig{
			Brokers: brokers, Group: "audit", Prefix: prefix, Types: types, Pool: pool, Handle: got.handle, Logger: slogDiscard(),
		})
		if !errors.Is(consumeErr, context.Canceled) {
			t.Errorf("consumer: %v", consumeErr)
		}
	})
	defer func() {
		stop()
		wg.Wait()
	}()
	waitFor(t, "every event", func() bool { return len(got.ids()) >= len(want) })

	// Publish them all again, as a relay that stopped before marking them
	// would: the consumer has seen them all.
	var events []outbox.Event
	got.mu.Lock()
	events = slices.Clone(got.events)
	got.mu.Unlock()
	if err = pub.Publish(ctx, events); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	ids := got.ids()
	if len(ids) != len(want) {
		t.Fatalf("handled %d events, want each of the %d once", len(ids), len(want))
	}
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	wantSorted := slices.Clone(want)
	slices.Sort(wantSorted)
	if !slices.Equal(sorted, wantSorted) {
		t.Errorf("handled %v, want %v", sorted, wantSorted)
	}
	// Each account's events come in the order they were written.
	position := map[string]int{}
	for i, id := range want {
		position[id] = i
	}
	last := map[string]int{}
	got.mu.Lock()
	defer got.mu.Unlock()
	for _, e := range got.events {
		if p, seen := last[e.Key]; seen && position[e.ID] < p {
			t.Errorf("%s: event %d came after %d", e.Key, position[e.ID], p)
		}
		last[e.Key] = position[e.ID]
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func slogDiscard() *slog.Logger { return slog.New(slog.DiscardHandler) }
