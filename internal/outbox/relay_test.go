package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AmrMady/brokerd/internal/outbox"
	"github.com/AmrMady/brokerd/internal/postgres"
	"github.com/AmrMady/brokerd/internal/postgres/pgtest"
)

// fakePublisher keeps what it's sent. It can fail its first calls, or run a
// function once it has published.
type fakePublisher struct {
	mu        sync.Mutex
	events    []outbox.Event
	failures  int
	onPublish func()
}

func (f *fakePublisher) Publish(_ context.Context, events []outbox.Event) error {
	f.mu.Lock()
	if f.failures > 0 {
		f.failures--
		f.mu.Unlock()
		return errors.New("broker unavailable")
	}
	f.events = append(f.events, events...)
	then := f.onPublish
	f.mu.Unlock()
	if then != nil {
		then()
	}
	return nil
}

// sent returns the IDs and payloads published so far.
func (f *fakePublisher) sent() (ids, payloads []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		ids = append(ids, e.ID)
		payloads = append(payloads, string(e.Payload))
	}
	return ids, payloads
}

type outboxDB struct {
	url  string
	pool *pgxpool.Pool
}

func newOutboxDB(t *testing.T) outboxDB {
	t.Helper()
	url := pgtest.NewURL(t)
	pool, err := postgres.NewPool(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return outboxDB{url: url, pool: pool}
}

// add writes n events, one transaction each, and returns their IDs in
// order.
func (db outboxDB) add(t *testing.T, n int) []string {
	t.Helper()
	for i := range n {
		err := pgx.BeginFunc(t.Context(), db.pool, func(tx pgx.Tx) error {
			return outbox.Add(t.Context(), tx, outbox.EntryPosted, fmt.Sprintf("account-%d", i%3), map[string]int{"n": i})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return db.ids(t, "true")
}

// ids returns the IDs of the events matching where, in seq order.
func (db outboxDB) ids(t *testing.T, where string) []string {
	t.Helper()
	rows, err := db.pool.Query(t.Context(), `SELECT id::text FROM outbox WHERE `+where+` ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// run runs a relay until the test ends or stop is called.
func (db outboxDB) run(t *testing.T, pub outbox.Publisher, batch int) (relay *outbox.Relay, stop func()) {
	t.Helper()
	relay, err := outbox.NewRelay(outbox.RelayConfig{
		DatabaseURL: db.url, Publisher: pub, BatchSize: batch,
		Interval: 10 * time.Millisecond, StandbyInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if runErr := <-done; !errors.Is(runErr, context.Canceled) {
				t.Errorf("relay stopped with %v", runErr)
			}
		})
	}
	t.Cleanup(stop)
	return relay, stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRelayPublishesInOrder(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	want := db.add(t, 12)
	pub := &fakePublisher{}
	relay, _ := db.run(t, pub, 5)

	eventually(t, "12 events", func() bool { ids, _ := pub.sent(); return len(ids) == 12 })
	ids, payloads := pub.sent()
	if !slices.Equal(ids, want) {
		t.Errorf("published %v, want seq order %v", ids, want)
	}
	for i, p := range payloads {
		if p != fmt.Sprintf(`{"n": %d}`, i) {
			t.Errorf("event %d payload %s", i, p)
		}
	}
	eventually(t, "the events marked published", func() bool { return len(db.ids(t, "published_at IS NULL")) == 0 })
	if relay.Published() != 12 {
		t.Errorf("Published() = %d, want 12", relay.Published())
	}
}

func TestRelayRetriesFailedPublishes(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	want := db.add(t, 3)
	pub := &fakePublisher{failures: 2}
	db.run(t, pub, 100)
	eventually(t, "the events", func() bool { ids, _ := pub.sent(); return len(ids) == 3 })
	if ids, _ := pub.sent(); !slices.Equal(ids, want) {
		t.Errorf("published %v, want %v once each", ids, want)
	}
}

// A relay that stops after publishing a batch, before marking it, leaves
// it to be published again: delivery is at least once.
func TestRelayRepublishesAfterAStop(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	want := db.add(t, 3)
	first := &fakePublisher{}
	_, stop := db.run(t, first, 100)
	first.onPublish = func() { go stop() }
	eventually(t, "the first publish", func() bool { ids, _ := first.sent(); return len(ids) == 3 })
	stop()

	second := &fakePublisher{}
	db.run(t, second, 100)
	eventually(t, "the second publish", func() bool { ids, _ := second.sent(); return len(ids) == 3 })
	if ids, _ := second.sent(); !slices.Equal(ids, want) {
		t.Errorf("republished %v, want the same events %v", ids, want)
	}
}

// Of two relays, one publishes; the other takes over when it stops.
func TestOneRelayLeads(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	a, b := &fakePublisher{}, &fakePublisher{}
	_, stopA := db.run(t, a, 100)
	eventually(t, "a to lead", func() bool {
		db.add(t, 1)
		ids, _ := a.sent()
		return len(ids) > 0
	})
	_, stopB := db.run(t, b, 100)
	time.Sleep(100 * time.Millisecond) // b is running, as a standby
	for range 10 {
		db.add(t, 1)
		time.Sleep(15 * time.Millisecond)
	}
	eventually(t, "a to publish everything", func() bool { return len(db.ids(t, "published_at IS NULL")) == 0 })
	if ids, _ := b.sent(); len(ids) != 0 {
		t.Fatalf("the standby published %v", ids)
	}

	stopA()
	later := db.add(t, 4)
	eventually(t, "b to take over", func() bool { ids, _ := b.sent(); return len(ids) == 4 })
	if ids, _ := b.sent(); !slices.Equal(ids, later[len(later)-4:]) {
		t.Errorf("b published %v, want only what a didn't", ids)
	}
	stopB()
}

// A relay that led and then lost its connection reconnects after the first
// backoff again, rather than one that kept doubling over its lifetime.
func TestRelayBackoffStartsOverAfterLeading(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	var logs lockedBuffer
	relay, err := outbox.NewRelay(outbox.RelayConfig{
		DatabaseURL: db.url, Publisher: &fakePublisher{}, Interval: 10 * time.Millisecond,
		Backoff: 10 * time.Millisecond, Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	var pid int32
	for range 3 {
		previous := pid
		eventually(t, "the relay to lead", func() bool {
			pid = db.leaderPID(t)
			return pid != 0 && pid != previous
		})
		if _, err = db.pool.Exec(t.Context(), `SELECT pg_terminate_backend($1)`, pid); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "the relay to lead again", func() bool { return db.leaderPID(t) != pid && db.leaderPID(t) != 0 })
	for line := range strings.Lines(logs.String()) {
		var m struct {
			Msg     string `json:"msg"`
			RetryIn int64  `json:"retry_in"`
		}
		if err = json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		if m.Msg == "outbox relay lost its database connection" && time.Duration(m.RetryIn) != 10*time.Millisecond {
			t.Errorf("retry in %s after leading, want the first backoff", time.Duration(m.RetryIn))
		}
	}
}

// leaderPID returns the backend holding the leader lock, or 0.
func (db outboxDB) leaderPID(t *testing.T) int32 {
	t.Helper()
	var pid int32
	err := db.pool.QueryRow(t.Context(), `
		SELECT COALESCE(max(pid), 0) FROM pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&pid)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// lockedBuffer is a buffer a logger can write to while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRelayPurgesOldEvents(t *testing.T) {
	t.Parallel()
	db := newOutboxDB(t)
	db.add(t, 3)
	ids := db.ids(t, "true")
	_, err := db.pool.Exec(t.Context(), `
		UPDATE outbox SET published_at = CASE id::text WHEN $1 THEN now() - interval '8 days' ELSE now() - interval '1 day' END
		WHERE id::text = ANY($2)`, ids[0], ids[:2])
	if err != nil {
		t.Fatal(err)
	}
	db.run(t, &fakePublisher{}, 100)
	eventually(t, "the purge", func() bool { return !slices.Contains(db.ids(t, "true"), ids[0]) })
	if left := db.ids(t, "true"); !slices.Equal(left, ids[1:]) {
		t.Errorf("left %v, want all but the week-old one", left)
	}
}

func TestNewRelayRejectsBadConfig(t *testing.T) {
	t.Parallel()
	for _, cfg := range []outbox.RelayConfig{
		{Publisher: &fakePublisher{}},
		{DatabaseURL: "postgres://x"},
		{DatabaseURL: "postgres://x", Publisher: &fakePublisher{}, BatchSize: -1},
		{DatabaseURL: "postgres://x", Publisher: &fakePublisher{}, Backoff: -1},
	} {
		if _, err := outbox.NewRelay(cfg); err == nil {
			t.Errorf("NewRelay(%+v) succeeded", cfg)
		}
	}
}
