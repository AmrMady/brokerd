-- +goose Up
-- Keys are only stored hashed: a key is shown once, when it's created.
CREATE TABLE api_keys (
    key_hash   BYTEA       PRIMARY KEY CHECK (length(key_hash) = 32),
    account_id UUID        NOT NULL REFERENCES accounts (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX api_keys_account_idx ON api_keys (account_id);

-- +goose Down
DROP TABLE api_keys;
