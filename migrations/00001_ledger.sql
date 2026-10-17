-- +goose Up
CREATE SEQUENCE account_numbers;

CREATE TABLE accounts (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        TEXT        NOT NULL CHECK (kind IN ('customer', 'system')),
    number      TEXT        UNIQUE,
    system_name TEXT        UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, kind),
    CHECK ((kind = 'customer') = (number IS NOT NULL)),
    CHECK ((kind = 'system') = (system_name IS NOT NULL))
);

ALTER SEQUENCE account_numbers OWNED BY accounts.number;

INSERT INTO accounts (id, kind, system_name) VALUES
    ('00000000-0000-0000-0000-000000000001', 'system', 'external_bank'),
    ('00000000-0000-0000-0000-000000000002', 'system', 'clearing'),
    ('00000000-0000-0000-0000-000000000003', 'system', 'fees');

CREATE TABLE assets (
    code       TEXT        PRIMARY KEY,
    scale      SMALLINT    NOT NULL CHECK (scale BETWEEN 0 AND 9),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO assets (code, scale) VALUES ('USD', 2);

CREATE TABLE ledger_accounts (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id UUID          NOT NULL,
    kind       TEXT          NOT NULL,
    asset      TEXT          NOT NULL REFERENCES assets (code),
    balance    NUMERIC(38,9) NOT NULL DEFAULT 0,
    held       NUMERIC(38,9) NOT NULL DEFAULT 0,
    version    BIGINT        NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (account_id, asset),
    UNIQUE (id, asset),
    FOREIGN KEY (account_id, kind) REFERENCES accounts (id, kind),
    CONSTRAINT ledger_accounts_held_non_negative CHECK (held >= 0),
    CONSTRAINT ledger_accounts_available_non_negative CHECK (kind = 'system' OR balance >= held)
);

CREATE TABLE journal_entries (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind       TEXT        NOT NULL CHECK (kind IN ('deposit', 'withdrawal', 'trade_fill')),
    metadata   JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE journal_lines (
    entry_id          BIGINT        NOT NULL REFERENCES journal_entries (id),
    ledger_account_id BIGINT        NOT NULL,
    asset             TEXT          NOT NULL,
    amount            NUMERIC(38,9) NOT NULL CHECK (amount <> 0),
    account_version   BIGINT        NOT NULL, -- ledger_accounts.version after this line
    PRIMARY KEY (ledger_account_id, account_version),
    UNIQUE (entry_id, ledger_account_id),
    FOREIGN KEY (ledger_account_id, asset) REFERENCES ledger_accounts (id, asset)
);

CREATE TABLE idempotency_keys (
    account_id      UUID        NOT NULL REFERENCES accounts (id),
    idempotency_key TEXT        NOT NULL,
    request_hash    BYTEA       NOT NULL,
    entry_id        BIGINT      REFERENCES journal_entries (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, idempotency_key)
);

-- +goose StatementBegin
CREATE FUNCTION journal_entry_must_balance() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    unbalanced_asset TEXT;
BEGIN
    SELECT asset INTO unbalanced_asset
    FROM journal_lines
    WHERE entry_id = NEW.entry_id
    GROUP BY asset
    HAVING SUM(amount) <> 0
    LIMIT 1;

    IF unbalanced_asset IS NOT NULL THEN
        RAISE EXCEPTION 'journal entry % does not balance for %', NEW.entry_id, unbalanced_asset
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER journal_lines_balanced
    AFTER INSERT ON journal_lines
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION journal_entry_must_balance();

-- +goose StatementBegin
CREATE FUNCTION journal_entry_must_have_lines() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM journal_lines WHERE entry_id = NEW.id) THEN
        RAISE EXCEPTION 'journal entry % has no lines', NEW.id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER journal_entries_have_lines
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION journal_entry_must_have_lines();

-- +goose StatementBegin
CREATE FUNCTION reject_journal_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER journal_entries_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_journal_change();

CREATE TRIGGER journal_lines_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON journal_lines
    FOR EACH STATEMENT EXECUTE FUNCTION reject_journal_change();

-- +goose Down
DROP TABLE idempotency_keys;
DROP TABLE journal_lines;
DROP TABLE journal_entries;
DROP TABLE ledger_accounts;
DROP TABLE assets;
DROP TABLE accounts;
DROP FUNCTION reject_journal_change();
DROP FUNCTION journal_entry_must_have_lines();
DROP FUNCTION journal_entry_must_balance();
