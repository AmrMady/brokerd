# brokerd

A brokerage backend in Go, built as a paper-trading system: a double-entry
ledger, market data, and an order engine. The ledger is done; the rest is
in progress.

## Status

Done:

- Ledger: brokerage accounts holding USD cash and fractional share positions
  in Postgres. Deposits, withdrawals, holds for open orders, settlement of
  trade fills with fees, balances, account history and a reconcile command.

Planned:

- Market data: simulated and CSV replay feeds, latest quotes, OHLCV bars,
  WebSocket streaming
- Paper trading: market and limit orders filled against live quotes
- REST API, metrics, OpenAPI spec
- Transactional outbox to Kafka
- Later: order book and matching engine, risk checks, T+1 settlement

## Ledger

Every change is a journal entry whose lines sum to zero for each asset. Each
account has one ledger row per asset it holds, carrying the balance. Customer
accounts sit on one side; the system accounts `external_bank`, `clearing` and
`fees` (rows in the same `accounts` table) sit on the other.

| Operation         | Lines                                                                              |
|-------------------|------------------------------------------------------------------------------------|
| Deposit X         | external_bank -X, customer +X                                                      |
| Withdraw X        | customer -X, external_bank +X                                                      |
| Buy q @ p, fee f  | customer USD -(n+f), clearing USD +n, fees +f, clearing SYM -q, customer SYM +q    |
| Sell q @ p, fee f | customer SYM -q, clearing SYM +q, clearing USD -n, customer USD +(n-f), fees +f    |

`n` is `q * p` rounded half-to-even to the cent.

Some notes on how it works:

- Money is `shopspring/decimal` in Go and `NUMERIC(38,9)` in Postgres. Each
  asset has a scale: 2 for USD, 9 for fractional shares, 0 for symbols that
  only trade in whole shares.
- Writes lock the ledger rows they touch with `SELECT ... FOR NO KEY UPDATE`
  and check the available balance under the lock. Rows are always locked in
  the same order (customer rows, then system rows, by id), so concurrent
  writes don't deadlock.
- A hold reserves cash or shares for an open order (`available = balance -
  held`). Releasing and settling a hold are both conditional updates on
  `status = 'active'`, so a cancel and a fill can't both go through.
- Deposits, withdrawals, holds and fills take an idempotency key, scoped to
  the account. Retrying a request returns the original result; reusing a key
  for a different request is an error.
- Postgres checks the invariants too: a deferred trigger rejects entries that
  don't balance, the journal is append-only, and customer balances can't go
  negative.
- `brokerd reconcile` rebuilds every balance from the journal and exits
  non-zero if anything is off.
- Ledger functions take a `pgx.Tx`, so a caller can post to the ledger and
  write its own rows in one transaction.

## Running

```bash
make db-up      # Postgres in Docker
make migrate
make reconcile
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
