# brokerd

A brokerage backend in Go, built as a paper-trading system: a double-entry
ledger, market data, and an order engine. The ledger is done; the rest is
in progress.

## Status

Done:

- Ledger: brokerage accounts holding USD cash and fractional share positions
  in Postgres. Deposits and withdrawals that go through a bank transfer
  lifecycle, holds for open orders, trade fills (including partial fills)
  with fees, T+1 settlement of sale proceeds, tax lots with realized gains
  and losses, reversals, balances, account history and a reconcile command.

Planned:

- Market data: simulated and CSV replay feeds, latest quotes, OHLCV bars,
  WebSocket streaming
- Paper trading: market and limit orders filled against live quotes
- REST API, metrics, OpenAPI spec
- Transactional outbox to Kafka
- Later: order book and matching engine, risk checks, corporate actions

## Ledger

Every change is a journal entry whose lines sum to zero for each asset. Each
account has one ledger row per asset it holds, carrying the balance. Customer
accounts sit on one side; the system accounts `external_bank`, `clearing`,
`fees` and `deficits` (rows in the same `accounts` table) sit on the other.

| Operation         | Lines                                                                              |
|-------------------|------------------------------------------------------------------------------------|
| Deposit X         | external_bank -X, customer +X                                                      |
| Withdraw X        | customer -X, external_bank +X                                                      |
| Deposit returned  | external_bank +X, customer -a, deficits -(X-a)                                     |
| Buy q @ p, fee f  | customer USD -(n+f), clearing USD +n, fees +f, clearing SYM -q, customer SYM +q    |
| Sell q @ p, fee f | customer SYM -q, clearing SYM +q, clearing USD -n, customer USD +(n-f), fees +f    |
| Reversal          | every line of the reversed entry, negated                                          |

`n` is `q * p` rounded half-to-even to the cent. `a` is what the customer
still has available when a deposit comes back.

Some notes on how it works:

- Money is `shopspring/decimal` in Go and `NUMERIC(38,9)` in Postgres. Each
  asset has a scale: 2 for USD, 9 for fractional shares, 0 for symbols that
  only trade in whole shares.
- Writes lock the ledger rows they touch with `SELECT ... FOR NO KEY UPDATE`
  and check the available balance under the lock. Rows are always locked in
  the same order (customer rows, then system rows, by id), so concurrent
  writes don't deadlock.
- Accounts are active, restricted or closed. A restricted account can still
  receive deposits and settle orders it already placed, but can't withdraw or
  place new holds. Closing needs every balance and hold at zero, and every
  status change is recorded with a reason. Writes take a shared lock on the
  account row first, so a close waits for postings in flight.
- A hold reserves cash or shares for an open order (`available = balance -
  held`). Each fill takes its cost out of the hold with a conditional update,
  so concurrent fills queue on the hold and can't use more than is left. What
  the fills don't use is released when the order is done or canceled.
- Deposits and withdrawals are transfers. They stay pending until the bank
  confirms them, and a pending withdrawal holds its cash. Bank events
  (completed, canceled, returned) are compare-and-sets on the transfer's
  status, so a repeated event changes nothing and a cancel and a completion
  can't both win. If a returned deposit was already spent, the `deficits`
  account covers the difference and the account is restricted.
- Transfer requests, holds and fills take an idempotency key, scoped to
  the account. Retrying a request returns the original result; reusing a key
  for a different request is an error.
- Fills are booked on the trade date. A sell's proceeds can be traded with
  right away but stay unsettled, and can't be withdrawn, until their
  settlement date. `brokerd settle` settles everything due; each settlement is
  a compare-and-set on its status, so runs can repeat or overlap safely. The
  settlement date comes with the fill from whoever executed the trade.
- Every buy opens a tax lot with its cost (fee included). Sells take shares
  from the oldest lots first (FIFO) and record the gain or loss on each lot,
  short- or long-term by holding period. A sell locks the position's open
  lots in that same order, so concurrent sells queue instead of taking the
  same shares. Cost and proceeds are split to the cent with nothing lost to
  rounding.
- Mistakes are corrected with a reversal, never an edit. An entry can only be
  reversed once (`reverses_entry_id` is unique), and a trigger checks at
  commit that the reversal mirrors it exactly. Only trade fills are reversed
  this way; transfers are undone through their own lifecycle. Reversing a
  sell puts its shares back in the lots it took them from; a buy can't be
  reversed once any of its shares were sold.
- Postgres checks the invariants too: a deferred trigger rejects entries that
  don't balance, the journal is append-only, and customer balances can't go
  negative.
- `brokerd reconcile` rebuilds every balance from the journal, checks that
  each position matches its open lots, and exits non-zero if anything is
  off.
- Ledger functions take a `pgx.Tx`, so a caller can post to the ledger and
  write its own rows in one transaction.

## Running

```bash
make db-up      # Postgres in Docker
make migrate
make reconcile
make settle     # settle trades due today
```

## Tests

```bash
make test       # needs Docker, the tests start Postgres with testcontainers
make test-short # unit tests only
make lint
```

Set `BROKERD_TEST_DATABASE_URL` to run the tests against an existing
Postgres instead of a container.

## License

MIT
