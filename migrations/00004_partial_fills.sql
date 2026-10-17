-- +goose Up
ALTER TABLE holds ADD COLUMN remaining NUMERIC(38,9);

UPDATE holds SET remaining = CASE WHEN status = 'settled' THEN 0 ELSE amount END;

ALTER TABLE holds
    ALTER COLUMN remaining SET NOT NULL,
    ADD CHECK (remaining >= 0 AND remaining <= amount),
    ADD CHECK ((status = 'settled') = (remaining = 0));

-- +goose Down
ALTER TABLE holds DROP COLUMN remaining;
