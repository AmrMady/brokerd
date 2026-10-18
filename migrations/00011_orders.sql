-- +goose Up
CREATE TABLE orders (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID          NOT NULL REFERENCES accounts (id),
    client_order_id TEXT          CHECK (client_order_id <> ''),
    symbol          TEXT          NOT NULL REFERENCES assets (code),
    side            TEXT          NOT NULL CHECK (side IN ('buy', 'sell')),
    type            TEXT          NOT NULL CHECK (type IN ('market', 'limit')),
    time_in_force   TEXT          NOT NULL CHECK (time_in_force IN ('day', 'gtc')),
    quantity        NUMERIC(38,9) NOT NULL CHECK (quantity > 0),
    limit_price     NUMERIC(38,9) CHECK (limit_price > 0),
    status          TEXT          NOT NULL DEFAULT 'new'
                    CHECK (status IN ('new', 'accepted', 'filled', 'canceled', 'rejected', 'expired')),
    reject_reason   TEXT,
    hold_id         BIGINT        REFERENCES holds (id),
    expires_at      TIMESTAMPTZ,
    fill_price      NUMERIC(38,9),
    fill_entry_id   BIGINT        REFERENCES journal_entries (id),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    CHECK ((type = 'limit') = (limit_price IS NOT NULL)),
    CHECK ((status = 'rejected') = (reject_reason IS NOT NULL)),
    CHECK ((status IN ('new', 'rejected')) = (hold_id IS NULL)),
    CHECK ((status = 'filled') = (fill_price IS NOT NULL AND fill_entry_id IS NOT NULL)),
    CHECK ((status IN ('new', 'accepted')) = (closed_at IS NULL)),
    -- Day orders expire at their session's close once accepted; GTC orders
    -- never expire.
    CHECK (CASE WHEN time_in_force = 'gtc' THEN expires_at IS NULL
                WHEN status IN ('new', 'rejected') THEN true
                ELSE expires_at IS NOT NULL END)
);

CREATE UNIQUE INDEX orders_client_order_id_idx ON orders (account_id, client_order_id)
    WHERE client_order_id IS NOT NULL;
CREATE INDEX orders_account_idx ON orders (account_id, created_at DESC, id DESC);
CREATE INDEX orders_open_idx ON orders (symbol) WHERE status = 'accepted';
CREATE INDEX orders_expiring_idx ON orders (expires_at) WHERE status = 'accepted';

-- Orders start new, and move along new -> accepted | rejected, then
-- accepted -> filled | canceled | expired. Nothing else changes, and
-- nothing is deleted.
-- +goose StatementBegin
CREATE FUNCTION check_order_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'new' THEN
            RAISE EXCEPTION 'orders start as new, not %', NEW.status;
        END IF;
        RETURN NEW;
    END IF;
    IF NOT ((OLD.status = 'new' AND NEW.status IN ('accepted', 'rejected'))
         OR (OLD.status = 'accepted' AND NEW.status IN ('filled', 'canceled', 'expired'))) THEN
        RAISE EXCEPTION 'order % can''t go from % to %', OLD.id, OLD.status, NEW.status;
    END IF;
    IF (NEW.account_id, NEW.client_order_id, NEW.symbol, NEW.side, NEW.type, NEW.time_in_force,
        NEW.quantity, NEW.limit_price, NEW.created_at)
       IS DISTINCT FROM
       (OLD.account_id, OLD.client_order_id, OLD.symbol, OLD.side, OLD.type, OLD.time_in_force,
        OLD.quantity, OLD.limit_price, OLD.created_at) THEN
        RAISE EXCEPTION 'order % can only change its status', OLD.id;
    END IF;
    IF OLD.status = 'accepted' AND (NEW.hold_id, NEW.expires_at) IS DISTINCT FROM (OLD.hold_id, OLD.expires_at) THEN
        RAISE EXCEPTION 'order % can only change its status', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER orders_change
    BEFORE INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION check_order_change();

CREATE TRIGGER orders_no_delete
    BEFORE DELETE OR TRUNCATE ON orders
    FOR EACH STATEMENT EXECUTE FUNCTION reject_journal_change();

-- +goose Down
DROP TABLE orders;
DROP FUNCTION check_order_change();
