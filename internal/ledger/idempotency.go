package ledger

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// keyOp is the kind of request an idempotency key belongs to. Keys are
// scoped to their account and their operation, so a client's transfer key
// can't collide with the engine's hold and fill keys.
type keyOp string

const (
	opTransfer keyOp = "transfer"
	opHold     keyOp = "hold"
	opFill     keyOp = "fill"
)

// keyResult is what the request recorded under an idempotency key created.
type keyResult struct {
	entryID, holdID, transferID int64
}

// claimKey records an idempotency key for an account and reports whether it
// was free. If another transaction has just claimed the same key, the insert
// waits for it: the key is taken if it commits and free if it rolls back.
func claimKey(ctx context.Context, tx pgx.Tx, op keyOp, account, key string, hash []byte) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (account_id, operation, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`,
		account, op, key, hash)
	if err != nil {
		return false, fmt.Errorf("claim idempotency key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// recordKey points a claimed key at what its request created.
func recordKey(ctx context.Context, tx pgx.Tx, op keyOp, account, key string, r keyResult) error {
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET entry_id = $4, hold_id = $5, transfer_id = $6
		WHERE account_id = $1 AND operation = $2 AND idempotency_key = $3`,
		account, op, key, nullable(r.entryID), nullable(r.holdID), nullable(r.transferID))
	if err != nil {
		return fmt.Errorf("record idempotency key %q: %w", key, err)
	}
	return nil
}

// usedKey returns what the earlier request with this key created, provided
// it was the same request.
func usedKey(ctx context.Context, tx pgx.Tx, op keyOp, account, key string, hash []byte) (keyResult, error) {
	var (
		stored                []byte
		entry, hold, transfer *int64
	)
	err := tx.QueryRow(ctx, `
		SELECT request_hash, entry_id, hold_id, transfer_id
		FROM idempotency_keys
		WHERE account_id = $1 AND operation = $2 AND idempotency_key = $3`,
		account, op, key,
	).Scan(&stored, &entry, &hold, &transfer)
	if err != nil {
		return keyResult{}, fmt.Errorf("load idempotency key %q: %w", key, err)
	}
	if !bytes.Equal(stored, hash) {
		return keyResult{}, fmt.Errorf("%w: %q", ErrIdempotencyConflict, key)
	}
	return keyResult{entryID: deref(entry), holdID: deref(hold), transferID: deref(transfer)}, nil
}

func nullable(id int64) *int64 {
	if id == 0 {
		return nil
	}
	return &id
}

func deref(id *int64) int64 {
	if id == nil {
		return 0
	}
	return *id
}
