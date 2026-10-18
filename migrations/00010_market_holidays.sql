-- +goose Up
-- Days the market is closed, or closes early when closes_at is set. Weekends
-- are closed without being listed.
CREATE TABLE market_holidays (
    day       DATE        PRIMARY KEY,
    name      TEXT        NOT NULL CHECK (name <> ''),
    closes_at TIME,
    CHECK (closes_at > '09:30' AND closes_at < '16:00')
);

-- +goose Down
DROP TABLE market_holidays;
