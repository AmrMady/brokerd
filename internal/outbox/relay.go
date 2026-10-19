package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Publisher sends events on, in the order given, and returns once every one
// of them is acknowledged.
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}

// RelayConfig configures a Relay.
type RelayConfig struct {
	// DatabaseURL is where the outbox is. The relay holds its own
	// connection, for the leader lock.
	DatabaseURL string
	Publisher   Publisher
	// BatchSize is how many events are published at once. Default 500.
	BatchSize int
	// Interval is how often an empty outbox is checked again. Default 100ms.
	Interval time.Duration
	// StandbyInterval is how often a relay that isn't leading tries to
	// take over. Default 1s.
	StandbyInterval time.Duration
	// Retention is how long published events are kept. Default 7 days.
	Retention time.Duration
	// Backoff is how long to wait before reconnecting to the database the
	// first time. It doubles after each failed attempt, up to 30s. Default
	// 1s.
	Backoff time.Duration
	Logger  *slog.Logger
}

// leaderLock is the advisory lock the leading relay holds. Advisory locks
// belong to their database, so relays of other databases don't share it.
const leaderLock = 0x6f7574626f78 // "outbox"

// steadyLead is how long a relay has to lead, with nothing to publish, for
// its session to count as healthy when the connection then fails.
const steadyLead = time.Minute

// Relay publishes the outbox. Only one relay publishes at a time: the one
// holding the leader lock, on its own connection. The others wait to take
// over when it stops or its connection drops, since Postgres releases the
// lock then.
//
// Each batch is read in seq order, published, and then marked published.
// A relay that stops after publishing a batch but before marking it leaves
// it to be published again, so events are delivered at least once, and
// consumers skip the ones they've seen.
type Relay struct {
	cfg       RelayConfig
	published atomic.Uint64
}

// NewRelay checks cfg and returns a Relay.
func NewRelay(cfg RelayConfig) (*Relay, error) {
	if cfg.DatabaseURL == "" || cfg.Publisher == nil {
		return nil, errors.New("outbox relay needs a database URL and a publisher")
	}
	if cfg.BatchSize < 0 || cfg.Interval < 0 || cfg.StandbyInterval < 0 || cfg.Retention < 0 || cfg.Backoff < 0 {
		return nil, fmt.Errorf("outbox relay config must not be negative: %+v", cfg)
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.StandbyInterval == 0 {
		cfg.StandbyInterval = time.Second
	}
	if cfg.Retention == 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Relay{cfg: cfg}, nil
}

// Published returns how many events this relay has published.
func (r *Relay) Published() uint64 {
	return r.published.Load()
}

// Run publishes the outbox until ctx is done. It reconnects with backoff,
// which starts over after a healthy session: one that published a batch,
// or led for steadyLead. A leader that fails as soon as it leads keeps
// backing off.
func (r *Relay) Run(ctx context.Context) error {
	wait := r.cfg.Backoff
	for {
		healthy, err := r.lead(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if healthy {
			wait = r.cfg.Backoff
		}
		r.cfg.Logger.Warn("outbox relay lost its database connection", "error", err, "retry_in", wait)
		if err = sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(2*wait, 30*time.Second)
	}
}

// lead connects, waits to become the leader, and publishes until ctx is
// done or the connection fails. healthy reports whether, as the leader, it
// published a batch or led for steadyLead.
func (r *Relay) lead(ctx context.Context) (healthy bool, err error) {
	// The pool's parser takes out settings only a pool understands, like
	// pool_max_conns, which Postgres itself would refuse.
	cfg, err := pgxpool.ParseConfig(r.cfg.DatabaseURL)
	if err != nil {
		return false, err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, conn.Close(context.WithoutCancel(ctx))) }()

	for led := false; !led; {
		if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLock).Scan(&led); err != nil {
			return false, err
		}
		if !led {
			if err = sleep(ctx, r.cfg.StandbyInterval); err != nil {
				return false, err
			}
		}
	}
	r.cfg.Logger.Info("outbox relay is leading")
	leading := time.Now()
	published, err := r.publish(ctx, conn)
	return published || time.Since(leading) >= steadyLead, err
}

// publish publishes batches, as the leader, until ctx is done or the
// connection fails, and reports whether it published any.
func (r *Relay) publish(ctx context.Context, conn *pgx.Conn) (published bool, err error) {
	var purged time.Time
	for {
		if time.Since(purged) > time.Hour {
			if err = r.purge(ctx, conn); err != nil {
				return published, err
			}
			purged = time.Now()
		}
		var n int
		if n, err = r.publishBatch(ctx, conn); err != nil {
			return published, err
		}
		published = published || n > 0
		if n < r.cfg.BatchSize {
			if err = sleep(ctx, r.cfg.Interval); err != nil {
				return published, err
			}
		}
	}
}

// publishBatch publishes the oldest unpublished events and marks them. A
// publisher that fails is retried with backoff, keeping the lead; only a
// database error gives it up.
func (r *Relay) publishBatch(ctx context.Context, conn *pgx.Conn) (int, error) {
	rows, err := conn.Query(ctx, `
		SELECT seq, id::text, type, key, payload, created_at FROM outbox
		WHERE published_at IS NULL
		ORDER BY seq
		LIMIT $1`, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}
	var (
		seqs   []int64
		events []Event
		seq    int64
		e      Event
	)
	_, err = pgx.ForEachRow(rows, []any{&seq, &e.ID, &e.Type, &e.Key, &e.Payload, &e.CreatedAt}, func() error {
		seqs = append(seqs, seq)
		events = append(events, e)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	wait := r.cfg.Interval
	for {
		err = r.cfg.Publisher.Publish(ctx, events)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		r.cfg.Logger.Warn("publish outbox events", "error", err, "events", len(events), "retry_in", wait)
		if err = sleep(ctx, wait); err != nil {
			return 0, err
		}
		wait = min(2*wait, 30*time.Second)
	}

	if _, err = conn.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE seq = ANY($1)`, seqs); err != nil {
		return 0, fmt.Errorf("mark events published: %w", err)
	}
	r.published.Add(uint64(len(events)))
	return len(events), nil
}

// purge deletes events published longer ago than the retention, in batches.
func (r *Relay) purge(ctx context.Context, conn *pgx.Conn) error {
	for {
		tag, err := conn.Exec(ctx, `
			DELETE FROM outbox WHERE seq IN (
				SELECT seq FROM outbox WHERE published_at < now() - $1::interval LIMIT 10000
			)`, r.cfg.Retention)
		if err != nil {
			return fmt.Errorf("purge outbox: %w", err)
		}
		if tag.RowsAffected() < 10000 {
			return nil
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
