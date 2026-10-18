-- +goose Up
-- A deficit entry moves cash from the deficits account to a customer who
-- can't pay for something that has to be booked anyway, like a trade that
-- already happened.
ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'deposit_return', 'withdrawal_return', 'trade_fill', 'reversal', 'deficit'));

-- +goose Down
ALTER TABLE journal_entries
    DROP CONSTRAINT journal_entries_kind_check,
    ADD CONSTRAINT journal_entries_kind_check
        CHECK (kind IN ('deposit', 'withdrawal', 'deposit_return', 'withdrawal_return', 'trade_fill', 'reversal'));
