-- +goose Up
-- An account's activity is read newest entry first.
CREATE INDEX journal_lines_entry_idx ON journal_lines (ledger_account_id, entry_id DESC);

-- +goose Down
DROP INDEX journal_lines_entry_idx;
