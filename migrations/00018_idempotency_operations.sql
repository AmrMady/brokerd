-- +goose Up
-- An idempotency key belongs to its account and to the kind of request
-- that used it, so a client's transfer key can never collide with the
-- engine's hold or fill keys.
ALTER TABLE idempotency_keys ADD COLUMN operation TEXT CHECK (operation IN ('transfer', 'hold', 'fill'));
UPDATE idempotency_keys SET operation = CASE
    WHEN transfer_id IS NOT NULL THEN 'transfer'
    WHEN hold_id IS NOT NULL THEN 'hold'
    ELSE 'fill'
END;
ALTER TABLE idempotency_keys
    ALTER COLUMN operation SET NOT NULL,
    DROP CONSTRAINT idempotency_keys_pkey,
    ADD PRIMARY KEY (account_id, operation, idempotency_key);

-- +goose Down
ALTER TABLE idempotency_keys
    DROP CONSTRAINT idempotency_keys_pkey,
    ADD PRIMARY KEY (account_id, idempotency_key),
    DROP COLUMN operation;
