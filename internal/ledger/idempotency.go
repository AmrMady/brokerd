package ledger

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// claimKey records an idempotency key for an account and reports whether it
// was free. If another transaction has just claimed the same key, the insert
// waits for it: the key is taken if it commits and free if it rolls back.
func claimKey(ctx context.Context, tx pgx.Tx, account, key string, hash []byte) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (account_id, idempotency_key, request_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`,
		account, key, hash)
	if err != nil {
		return false, fmt.Errorf("claim idempotency key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// usedKey returns the entry the earlier request with this key created,
// provided it was the same request.
func usedKey(ctx context.Context, tx pgx.Tx, account, key string, hash []byte) (int64, error) {
	var (
		stored []byte
		entry  int64
	)
	err := tx.QueryRow(ctx, `
		SELECT request_hash, entry_id
		FROM idempotency_keys
		WHERE account_id = $1 AND idempotency_key = $2`,
		account, key,
	).Scan(&stored, &entry)
	if err != nil {
		return 0, fmt.Errorf("load idempotency key %q: %w", key, err)
	}
	if !bytes.Equal(stored, hash) {
		return 0, fmt.Errorf("%w: %q", ErrIdempotencyConflict, key)
	}
	return entry, nil
}
