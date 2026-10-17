-- +goose Up
INSERT INTO accounts (id, kind, system_name) VALUES
    ('00000000-0000-0000-0000-000000000005', 'system', 'sec_fee_payable'),
    ('00000000-0000-0000-0000-000000000006', 'system', 'taf_payable')
ON CONFLICT (id) DO NOTHING;

-- The SEC fee is per dollar sold. FINRA's trading activity fee (TAF) is per
-- share sold, up to a maximum per trade.
CREATE TABLE fee_rates (
    fee           TEXT          NOT NULL CHECK (fee IN ('sec', 'taf')),
    effective_on  DATE          NOT NULL,
    rate          NUMERIC(38,9) NOT NULL CHECK (rate >= 0),
    max_per_trade NUMERIC(38,9) CHECK (max_per_trade >= 0),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (fee, effective_on),
    CHECK ((fee = 'taf') = (max_per_trade IS NOT NULL))
);

-- +goose Down
DROP TABLE fee_rates;
