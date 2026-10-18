package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handle runs handle for e unless consumer already handled it. It records
// that it did in the same transaction as handle's own writes, so an event
// delivered more than once is handled once, and one whose handling failed
// is handled again when it's redelivered. A duplicate delivered at the same
// time waits for the first to commit, then skips. It reports whether
// handle ran.
func Handle(ctx context.Context, pool *pgxpool.Pool, consumer string, e Event,
	handle func(ctx context.Context, tx pgx.Tx, e Event) error,
) (bool, error) {
	ran := false
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO consumed_events (consumer, event_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, consumer, e.ID)
		if err != nil {
			return fmt.Errorf("record event %s: %w", e.ID, err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		ran = true
		return handle(ctx, tx, e)
	})
	if err != nil {
		return false, err
	}
	return ran, nil
}

// ForgetConsumed deletes the records of events handled before cutoff. Keep
// them for longer than an event can be redelivered: Kafka's retention.
func ForgetConsumed(ctx context.Context, pool *pgxpool.Pool, cutoff time.Time) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM consumed_events WHERE consumed_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("forget consumed events: %w", err)
	}
	return tag.RowsAffected(), nil
}
