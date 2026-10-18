-- +goose Up
-- Requests sent with an Idempotency-Key, and their responses once done, so
-- a retry gets the same answer for a day. A row with no status is a request
-- still running. Rows are partitioned by day, and old days are dropped
-- whole; the app creates and drops the partitions.
CREATE TABLE idempotent_requests (
    scope       TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    fingerprint BYTEA       NOT NULL,
    status      SMALLINT,
    body        BYTEA,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status IS NULL) = (body IS NULL))
) PARTITION BY RANGE (created_at);

CREATE INDEX idempotent_requests_key_idx ON idempotent_requests (scope, key);

-- +goose Down
DROP TABLE idempotent_requests;
