-- +goose Up
CREATE TABLE holds (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ledger_account_id BIGINT        NOT NULL REFERENCES ledger_accounts (id),
    amount            NUMERIC(38,9) NOT NULL CHECK (amount > 0),
    status            TEXT          NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active', 'released', 'settled')),
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    closed_at         TIMESTAMPTZ,
    CHECK ((status = 'active') = (closed_at IS NULL))
);

CREATE INDEX holds_active_idx ON holds (ledger_account_id) WHERE status = 'active';

ALTER TABLE journal_entries ADD COLUMN hold_id BIGINT REFERENCES holds (id);

ALTER TABLE idempotency_keys
    ADD COLUMN hold_id BIGINT REFERENCES holds (id),
    ADD CHECK (entry_id IS NULL OR hold_id IS NULL);

-- +goose Down
ALTER TABLE idempotency_keys DROP COLUMN hold_id;
ALTER TABLE journal_entries DROP COLUMN hold_id;
DROP TABLE holds;
