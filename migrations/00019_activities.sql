-- +goose Up
-- Each customer numbers the entries that change it, in commit order, so its
-- activity can be paged, and polled for what's new, without missing an
-- entry that committed late. An entry takes the next number from its
-- account's row, which every write holds until it commits.
ALTER TABLE accounts ADD COLUMN last_activity BIGINT NOT NULL DEFAULT 0;

CREATE TABLE activities (
    account_id UUID   NOT NULL REFERENCES accounts (id),
    seq        BIGINT NOT NULL,
    entry_id   BIGINT NOT NULL REFERENCES journal_entries (id),
    PRIMARY KEY (account_id, seq),
    UNIQUE (account_id, entry_id)
);

CREATE TRIGGER activities_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON activities
    FOR EACH STATEMENT EXECUTE FUNCTION reject_journal_change();

INSERT INTO activities (account_id, seq, entry_id)
SELECT account_id, row_number() OVER (PARTITION BY account_id ORDER BY entry_id), entry_id
FROM (
    SELECT DISTINCT la.account_id, l.entry_id
    FROM journal_lines l
    JOIN ledger_accounts la ON la.id = l.ledger_account_id
    WHERE la.kind = 'customer'
) AS e;

UPDATE accounts a SET last_activity = n.last
FROM (SELECT account_id, max(seq) AS last FROM activities GROUP BY account_id) AS n
WHERE a.id = n.account_id;

DROP INDEX journal_lines_entry_idx;

-- +goose Down
CREATE INDEX journal_lines_entry_idx ON journal_lines (ledger_account_id, entry_id DESC);
DROP TABLE activities;
ALTER TABLE accounts DROP COLUMN last_activity;
