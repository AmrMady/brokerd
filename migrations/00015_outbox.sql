-- +goose Up
-- Events to publish, written in the same transaction as what they describe,
-- so one is published if and only if the other happened. They're published
-- in seq order.
CREATE TABLE outbox (
    seq          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id           UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    type         TEXT        NOT NULL CHECK (type <> ''),
    key          TEXT        NOT NULL CHECK (key <> ''),
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (seq) WHERE published_at IS NULL;
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- The events each consumer has handled, so it can skip the ones delivered
-- again.
CREATE TABLE consumed_events (
    consumer    TEXT        NOT NULL,
    event_id    UUID        NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE INDEX consumed_events_age_idx ON consumed_events (consumed_at);

-- +goose Down
DROP TABLE consumed_events;
DROP TABLE outbox;
