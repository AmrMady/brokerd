-- +goose Up
-- 00016 gave the orders resting at the time a commission of 0, but their
-- holds were sized with the commission configured then, and a fill charges
-- the order's own commission. Take it back from the hold where it's there:
-- a limit buy holds its cost at the limit, rounded up to the cent, plus the
-- commission. A sell holds shares, so its commission can't be recovered; it
-- stays 0, and that fill is charged none, which can't leave its hold short.
ALTER TABLE orders DISABLE TRIGGER orders_change;

UPDATE orders o SET commission = h.amount - ceil(o.quantity * o.limit_price * 100) / 100
FROM holds h
WHERE o.status = 'accepted' AND o.side = 'buy' AND o.type = 'limit' AND o.commission = 0
  AND h.id = o.hold_id AND h.amount > ceil(o.quantity * o.limit_price * 100) / 100;

ALTER TABLE orders ENABLE TRIGGER orders_change;

-- +goose Down
-- The commissions stay: they're what the holds were sized for.
