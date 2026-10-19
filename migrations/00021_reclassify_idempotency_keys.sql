-- +goose Up
-- 00018 took every key that pointed only at a journal entry to be a fill's,
-- but deposits and withdrawals booked before transfers existed (00005)
-- kept their keys that way too. Go by the entry's kind instead. A key
-- that's since been used again for a transfer is left as it is, since one
-- key can't belong to an operation twice.
UPDATE idempotency_keys k SET operation = 'transfer'
FROM journal_entries e
WHERE k.operation = 'fill' AND e.id = k.entry_id AND e.kind IN ('deposit', 'withdrawal')
  AND NOT EXISTS (
      SELECT 1 FROM idempotency_keys t
      WHERE t.account_id = k.account_id AND t.operation = 'transfer' AND t.idempotency_key = k.idempotency_key
  );

-- +goose Down
-- The keys stay classified by what they were used for.
