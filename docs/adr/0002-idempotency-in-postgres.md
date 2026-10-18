# 2. Idempotency in Postgres

Status: accepted

## Context

Clients retry after timeouts, and the engine retries fills. A retry must not
move money twice, and a retry of a request that's still running must not
run it again at the same time.

## Decision

Keep idempotency in Postgres, in the same transaction as the effect, at two
levels.

- The ledger takes an idempotency key for transfers, holds and fills,
  scoped to the account and the operation. The key is claimed with `INSERT
  ... ON CONFLICT DO NOTHING` in the transaction that makes the change, so
  the key and the change commit together or not at all. A duplicate running
  at the same time waits on the unique index, then replays the result. Each
  key stores a hash of its request, so reusing it for a different request is
  an error.
- The API stores responses to requests with an `Idempotency-Key`, scoped to
  the account in the path. A request claims its key in a short transaction
  under an advisory lock with a one-second `lock_timeout`, so a duplicate
  still running gets 409 rather than a wait. A claim has a one-minute lease
  for crashed servers, and handlers get half of it. Server errors and
  requests whose client went away give the key back. Keys are kept for a day
  in a table partitioned by day.

## Alternatives

Redis for keys and stored responses. That's a second store written next to
Postgres: a crash between the two leaves a key without its effect or an
effect without its key, which is the bug idempotency is there to prevent.

## Consequences

Keys cost Postgres storage and writes, kept in check by dropping a day's
partition at a time. Opening an account can't take a key, since there's no
account to scope it to. Even when the API runs a request again, the ledger
key or the order's client order ID it passes down stops the effect from
happening twice.
