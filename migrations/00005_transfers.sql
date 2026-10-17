-- +goose Up
INSERT INTO accounts (id, kind, system_name)
VALUES ('00000000-0000-0000-0000-000000000004', 'system', 'deficits')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE transfers (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id    UUID          NOT NULL REFERENCES accounts (id),
    direction     TEXT          NOT NULL CHECK (direction IN ('deposit', 'withdrawal')),
    amount        NUMERIC(38,9) NOT NULL CHECK (amount > 0),
    status        TEXT          NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'completed', 'returned', 'canceled')),
    hold_id       BIGINT        REFERENCES holds (id),
    return_reason TEXT,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK ((direction = 'withdrawal') = (hold_id IS NOT NULL)),
    CHECK ((status = 'returned') = (return_reason IS NOT NULL))
);

CREATE INDEX transfers_pending_idx ON transfers (account_id) WHERE status = 'pending';

ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'deposit_return', 'withdrawal_return', 'trade_fill')),
    ADD COLUMN transfer_id BIGINT REFERENCES transfers (id);

ALTER TABLE idempotency_keys
    DROP CONSTRAINT idempotency_keys_check,
    ADD COLUMN transfer_id BIGINT REFERENCES transfers (id),
    ADD CONSTRAINT idempotency_keys_check CHECK (num_nonnulls(entry_id, hold_id, transfer_id) <= 1);

-- +goose Down
ALTER TABLE idempotency_keys
    DROP CONSTRAINT idempotency_keys_check,
    DROP COLUMN transfer_id,
    ADD CONSTRAINT idempotency_keys_check CHECK (entry_id IS NULL OR hold_id IS NULL);

ALTER TABLE journal_entries
    DROP COLUMN transfer_id,
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'trade_fill'));

DROP TABLE transfers;
