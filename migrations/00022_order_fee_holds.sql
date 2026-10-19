-- +goose Up
-- A limit sell whose fees would be more than it brings in at its limit
-- holds the difference in cash, so its fill can't leave the account short.
ALTER TABLE orders
    ADD COLUMN fee_hold_id BIGINT REFERENCES holds (id),
    ADD CHECK (fee_hold_id IS NULL OR (side = 'sell' AND hold_id IS NOT NULL));

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION check_order_change() RETURNS trigger
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
    IF OLD.status = 'accepted' AND (NEW.hold_id, NEW.fee_hold_id, NEW.expires_at, NEW.commission)
       IS DISTINCT FROM (OLD.hold_id, OLD.fee_hold_id, OLD.expires_at, OLD.commission) THEN
        RAISE EXCEPTION 'order % can only change its status', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION check_order_change() RETURNS trigger
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
    IF OLD.status = 'accepted' AND (NEW.hold_id, NEW.expires_at, NEW.commission)
       IS DISTINCT FROM (OLD.hold_id, OLD.expires_at, OLD.commission) THEN
        RAISE EXCEPTION 'order % can only change its status', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
ALTER TABLE orders DROP COLUMN fee_hold_id;
