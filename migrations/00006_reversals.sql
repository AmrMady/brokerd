-- +goose Up
ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'deposit_return', 'withdrawal_return', 'trade_fill', 'reversal')),
    ADD COLUMN reverses_entry_id BIGINT UNIQUE REFERENCES journal_entries (id),
    ADD CONSTRAINT journal_entries_reversal_check CHECK ((kind = 'reversal') = (reverses_entry_id IS NOT NULL));

-- +goose StatementBegin
CREATE FUNCTION reversal_must_mirror() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        (SELECT ledger_account_id, asset, -amount FROM journal_lines WHERE entry_id = NEW.reverses_entry_id
         EXCEPT
         SELECT ledger_account_id, asset, amount FROM journal_lines WHERE entry_id = NEW.id)
        UNION ALL
        (SELECT ledger_account_id, asset, amount FROM journal_lines WHERE entry_id = NEW.id
         EXCEPT
         SELECT ledger_account_id, asset, -amount FROM journal_lines WHERE entry_id = NEW.reverses_entry_id)
    ) THEN
        RAISE EXCEPTION 'reversal % does not mirror entry %', NEW.id, NEW.reverses_entry_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER journal_entries_reversal_mirrors
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    WHEN (NEW.kind = 'reversal')
    EXECUTE FUNCTION reversal_must_mirror();

-- +goose Down
DROP TRIGGER journal_entries_reversal_mirrors ON journal_entries;
DROP FUNCTION reversal_must_mirror();

ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_reversal_check,
    DROP COLUMN reverses_entry_id,
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'deposit_return', 'withdrawal_return', 'trade_fill'));
