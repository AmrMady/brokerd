-- +goose Up
ALTER TABLE ledger_accounts
    ADD COLUMN unsettled NUMERIC(38,9) NOT NULL DEFAULT 0,
    ADD CONSTRAINT ledger_accounts_unsettled_non_negative CHECK (unsettled >= 0);

CREATE TABLE settlements (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id          BIGINT        NOT NULL UNIQUE REFERENCES journal_entries (id),
    ledger_account_id BIGINT        NOT NULL REFERENCES ledger_accounts (id),
    unsettled_cash    NUMERIC(38,9) NOT NULL CHECK (unsettled_cash >= 0),
    trade_date        DATE          NOT NULL,
    settle_date       DATE          NOT NULL,
    status            TEXT          NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'settled', 'canceled')),
    settled_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK (settle_date >= trade_date),
    CHECK ((status = 'settled') = (settled_at IS NOT NULL))
);

CREATE INDEX settlements_due_idx ON settlements (settle_date, id) WHERE status = 'pending';

-- +goose Down
DROP TABLE settlements;
ALTER TABLE ledger_accounts DROP COLUMN unsettled;
