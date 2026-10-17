-- +goose Up
ALTER TABLE accounts
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'restricted', 'closed')),
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE account_status_changes (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  UUID        NOT NULL REFERENCES accounts (id),
    from_status TEXT        NOT NULL,
    to_status   TEXT        NOT NULL,
    reason      TEXT        NOT NULL CHECK (reason <> ''),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX account_status_changes_account_idx ON account_status_changes (account_id, id);

-- +goose Down
DROP TABLE account_status_changes;
ALTER TABLE accounts DROP COLUMN updated_at, DROP COLUMN status;
