# 1. Double-entry ledger with row locks

Status: accepted

## Context

Balances have to be right under concurrent writes, and every change to them
has to be explainable after the fact. Some rows are contended by nature: a
busy customer's cash, and the clearing account every fill posts to.

## Decision

Every change is a journal entry whose lines sum to zero for each asset. A
deferred trigger checks the sum at commit, and the journal is append-only.
Each account has one `ledger_accounts` row per asset, holding its balance,
held amount and version, updated in the same transaction as the entry.

Writes lock what they touch and check the available balance under the lock.
Every write locks its account's row first (`FOR NO KEY UPDATE`), so one
account's writes go one at a time. Ledger rows are then locked with `SELECT
... FOR NO KEY UPDATE`, customer rows before system rows, each by id, so
writes to different accounts can't deadlock on the system rows they share.
A `CHECK` keeps customer rows from going below what's held.

## Alternatives

- Optimistic writes with a version check, retried on conflict. On rows this
  contended most attempts would fail and retry, and every caller would need
  a retry loop.
- `SERIALIZABLE` transactions. The same retries, everywhere, and harder to
  reason about than explicit locks.
- Summing journal lines to get a balance. Reads get slower with history,
  and checking a balance before spending it still needs a lock.

## Consequences

Writers queue on hot rows instead of failing. The clearing account is the
limit: the load test tops out at about 450 fills a second, all waiting on
its rows. Splitting it into several accounts, or batching fills into one
entry, would raise that. `brokerd reconcile` rebuilds every balance from the
journal, so a bug in the balance updates shows up as a mismatch.
