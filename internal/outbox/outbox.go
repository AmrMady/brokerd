// Package outbox publishes events about postings and fills with the
// transactional outbox pattern: an event is written to the outbox table in
// the same transaction as what it describes, and a relay publishes the
// outbox, at least once, in order. Consumers skip events they've seen.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event types.
const (
	EntryPosted = "entry_posted" // a journal entry was posted
	OrderFilled = "order_filled" // an order filled
)

// Event is something that happened, to publish.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Key       string          `json:"key"` // the account; an account's events are published in order
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// Add writes an event to the outbox in tx, so it's published if and only
// if tx commits.
func Add(ctx context.Context, tx pgx.Tx, typ, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s event: %w", typ, err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox (type, key, payload) VALUES ($1, $2, $3)`, typ, key, data); err != nil {
		return fmt.Errorf("add %s event: %w", typ, err)
	}
	return nil
}
