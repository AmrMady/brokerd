-- +goose Up
CREATE TABLE tax_lots (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ledger_account_id BIGINT        NOT NULL REFERENCES ledger_accounts (id),
    entry_id          BIGINT        UNIQUE REFERENCES journal_entries (id), -- the buy; NULL for backfilled lots
    quantity          NUMERIC(38,9) NOT NULL CHECK (quantity > 0),
    remaining         NUMERIC(38,9) NOT NULL CHECK (remaining >= 0 AND remaining <= quantity),
    cost              NUMERIC(38,9) CHECK (cost >= 0), -- NULL when unknown
    remaining_cost    NUMERIC(38,9) CHECK (remaining_cost >= 0 AND remaining_cost <= cost),
    acquired_on       DATE          NOT NULL,
    reversed_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK ((cost IS NULL) = (remaining_cost IS NULL))
);

CREATE INDEX tax_lots_open_idx ON tax_lots (ledger_account_id, acquired_on, id) WHERE remaining > 0;

CREATE TABLE lot_sales (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lot_id      BIGINT        NOT NULL REFERENCES tax_lots (id),
    entry_id    BIGINT        NOT NULL REFERENCES journal_entries (id), -- the sell
    quantity    NUMERIC(38,9) NOT NULL CHECK (quantity > 0),
    proceeds    NUMERIC(38,9) NOT NULL,
    cost        NUMERIC(38,9),
    gain        NUMERIC(38,9),
    sold_on     DATE          NOT NULL,
    long_term   BOOLEAN       NOT NULL,
    reversed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (entry_id, lot_id),
    CHECK ((cost IS NULL) = (gain IS NULL)),
    CHECK (gain = proceeds - cost)
);

CREATE INDEX lot_sales_lot_idx ON lot_sales (lot_id);

-- Positions from before lots were tracked get one lot each, with an unknown
-- cost.
INSERT INTO tax_lots (ledger_account_id, quantity, remaining, acquired_on)
SELECT id, balance, balance, created_at::date
FROM ledger_accounts
WHERE kind = 'customer' AND asset <> 'USD' AND balance > 0;

-- +goose Down
DROP TABLE lot_sales;
DROP TABLE tax_lots;
