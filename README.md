# brokerd

A brokerage backend in Go, built as a paper-trading system: a double-entry
ledger, market data, and an order engine. The ledger and market data are
done; the order engine is next.

## Status

Done:

- Ledger: brokerage accounts holding USD cash and fractional share positions
  in Postgres. Deposits and withdrawals that go through a bank transfer
  lifecycle, holds for open orders, trade fills (including partial fills)
  with fees, SEC and FINRA regulatory fees on sells, T+1 settlement of sale
  proceeds, tax lots with realized gains and losses, reversals, balances,
  account history and a reconcile command.
- Market data: a seeded random-walk simulator and CSV replay, per-symbol
  sequence checks, a lock-free latest-quote cache, 1s and 1m OHLCV bars by
  event time, a WebSocket stream that drops and then disconnects slow
  clients, and a relay that streams from another brokerd, all run by
  `brokerd serve`.

Planned:

- Paper trading: market and limit orders filled against live quotes
- REST API, metrics, OpenAPI spec
- Transactional outbox to Kafka
- Later: order book and matching engine, risk checks, corporate actions

## Ledger

Every change is a journal entry whose lines sum to zero for each asset. Each
account has one ledger row per asset it holds, carrying the balance. Customer
accounts sit on one side; the system accounts `external_bank`, `clearing`,
`fees`, `deficits`, `sec_fee_payable` and `taf_payable` (rows in the same
`accounts` table) sit on the other.

| Operation         | Lines                                                                                   |
|-------------------|-----------------------------------------------------------------------------------------|
| Deposit X         | external_bank -X, customer +X                                                           |
| Withdraw X        | customer -X, external_bank +X                                                           |
| Deposit returned  | external_bank +X, customer -a, deficits -(X-a)                                          |
| Buy q @ p, fee f  | customer USD -(n+f), clearing USD +n, fees +f, clearing SYM -q, customer SYM +q         |
| Sell q @ p, fee f | customer SYM -q, clearing SYM +q, clearing USD -n, customer USD +(n-f-s-t), fees +f,    |
|                   | sec_fee_payable +s, taf_payable +t                                                      |
| Reversal          | every line of the reversed entry, negated                                               |

`n` is `q * p` rounded half-to-even to the cent. `s` and `t` are the SEC fee
and FINRA's trading activity fee (TAF). `a` is what the customer still has
available when a deposit comes back.

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
- Sells pay the SEC fee (per dollar sold) and TAF (per share sold, capped
  per trade) at the rates in effect on the trade date, each rounded up to
  the cent. Rates are loaded with `brokerd fee-rate` from the published
  schedules, and a sell with no rate in effect fails rather than charging
  nothing. Each entry records the rates it was charged, and the idempotency
  key covers the fill as requested, so a retry returns the original entry
  even if the schedule changed since.
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

## Market data

A feed sends trades and quotes, in order, to a handler on the feed's own
goroutine. Each symbol's events carry a sequence number.

- The simulator moves each symbol's midpoint by at most a cent per quote,
  with a spread of one to four cents, and prints trades at the bid or ask.
  Prices are whole cents internally and event times are spaced `1/rate`
  apart, so a seed always gives the same events. It runs as fast as it can
  (about 10M events/s on an Apple M4 Pro) or paced in real time.
- CSV replay reads `type,symbol,seq,timestamp,price,size,bid,bid_size,ask,ask_size`
  and can keep the original spacing between events, sped up or not. Bad
  rows fail with their line number.
- The sequencer passes on events in order. A symbol's first event starts its
  sequence; an event that skips ahead is passed on and counted as a gap,
  and one at or behind the last is dropped and counted as stale. Late
  events aren't held back to fill a gap: for prices, the newest one wins.
- The latest-quote cache is read far more than it's written, so reads take
  no locks. Each symbol has an `atomic.Pointer` to an immutable quote, and
  an update stores a pointer to a new one. The pointers live in a `sync.Map`,
  which is written only when a symbol first appears. I benchmarked it
  against the alternatives (1,000 symbols, Apple M4 Pro, time per read with
  reads spread over all cores, so lower is better):

  | Design                                  | 1 core  | 14 cores | 14 cores + writer | Writer's rate | Add 10k symbols |
  |-----------------------------------------|---------|----------|-------------------|---------------|-----------------|
  | One RWMutex                             | 21.5 ns | 125 ns   | 132 ns            | 0.17M/s       | 0.87 ms         |
  | 64 RWMutex shards                       | 25.0 ns | 14.2 ns  | 19.2 ns           | 0.16M/s       | 1.0 ms          |
  | `atomic.Pointer` to a copy-on-write map | 16.4 ns | 3.2 ns   | 3.7 ns            | 1.73M/s       | 1.5 s           |
  | `sync.Map` of `atomic.Pointer` (used)   | 22.7 ns | 3.4 ns   | 3.0 ns            | 2.23M/s       | 1.5 ms          |

  A single RWMutex gets slower as cores are added, because every `RLock`
  writes the same counter. Sharding spreads that out but still slows the
  writer, which is the feed, to under 0.2M quotes/s. A copy-on-write map
  reads as fast as the `sync.Map`, but copies itself for every new symbol,
  which is quadratic. Run them with
  `go test -run '^$' -bench QuoteCache -cpu 1,14 ./internal/marketdata`.
- Bars are built from trades by event time, one aggregator per interval.
  The watermark is the latest event time seen from any symbol, quotes
  included, so bars of a quiet symbol still close. A window closes once the
  watermark passes its end plus a grace period, and its bars go out sorted
  by symbol. Trades that arrive out of order within the grace period are
  placed by their own time (open and close follow event time, not arrival);
  trades for a closed window are dropped and counted. A 1s aggregator
  handles about 13M trades/s on one core.

## Streaming

The hub streams trades, quotes and bars over a WebSocket. A client sends

```json
{"action": "subscribe", "trades": ["AAPL"], "quotes": ["AAPL", "MSFT"], "bars": ["AAPL"]}
```

(or `"unsubscribe"`), and gets frames that are JSON arrays of messages:

```json
[{"type":"subscription","trades":["AAPL"],"quotes":["AAPL","MSFT"],"bars":["AAPL"]},
 {"type":"trade","symbol":"AAPL","price":"227.16","size":"100","timestamp":"2026-10-16T13:30:00Z","seq":2}]
```

Prices and sizes are strings, so they keep their exact decimal value. A
subscription's confirmation is always queued before its first event. The
handshake's `Stream-Session` header names the hub's session: events keep
their feed's sequence numbers, which start over when the hub restarts.

The hub runs on the feed's goroutine and must never block it, so a slow
client can only hurt itself:

- Each event is encoded once, then queued for every client subscribed to
  it. Each client's queue is a fixed-size ring buffer; when it's full, the
  oldest message is dropped and counted. A writer goroutine per connection
  sends up to 256 queued messages per frame.
- A client is disconnected once it has kept dropping messages for `MaxLag`
  (5s) without emptying its queue in between, when a single write blocks
  for `MaxLag`, or when it doesn't answer a ping within `MaxLag`. A client
  that's cut off for being slow gets no close handshake, which can take
  10s and which it couldn't keep up with anyway.
- Shutting down sends every client a "going away" close and waits for each
  connection to finish. The tests check for leaked goroutines with goleak.

A relay is a feed that subscribes to another brokerd's stream, so hubs can
be chained: one reads the real feed and relays closer to the clients fan
it out further. Events keep their upstream sequence numbers, so what the
relay misses while reconnecting, or what the upstream hub drops because
the relay fell behind, shows up downstream as sequence gaps. When the
upstream session changes, the relay shifts the new numbers past the ones it
already passed on. It reconnects with backoff (1s doubling to a minute,
reset after a connection that streamed) and pings to catch a dead
connection.

Publishing a trade to 100 subscribers (1,000 clients each subscribed to 10
of 100 symbols) takes about 29 µs, or 3.4M messages/s, on an Apple M4 Pro.
With 50 of the clients reading only one message a millisecond, and so
losing about 2.8 messages for every trade published, it takes about 31 µs:
slow clients barely touch the publisher. Run it with
`go test -run '^$' -bench HubFanOut ./internal/stream`.

## Running

`brokerd serve` runs the market data pipeline and streams it at
`ws://localhost:8080/v1/stream`:

```text
feed -> sequencer -> 1s and 1m bars -> hub -> WebSocket clients
                  -> hub
```

```bash
make serve      # simulated feed
go run ./cmd/brokerd serve -feed replay -file events.csv -speed 10
go run ./cmd/brokerd serve -feed relay -upstream ws://localhost:8080/v1/stream -addr :8081
```

Connect with any WebSocket client, for example
`websocat ws://localhost:8080/v1/stream`, and send a subscribe request. A
replay serves until its file runs out, flushing its last bars. On SIGINT or
SIGTERM, serve stops the feed and the HTTP server, sends WebSocket clients
a going-away close, waits for them, and logs sequence gaps, late trades and
dropped messages.

The ledger needs Postgres:

```bash
make db-up      # Postgres in Docker
make migrate
make reconcile
make settle     # settle trades due today
```

Sells need regulatory fee rates. Add each rate the SEC and FINRA publish,
with the date it takes effect. The SEC rate is per dollar sold, so $27.80
per million is 0.0000278. TAF is per share sold, with a maximum per trade.

```bash
go run ./cmd/brokerd fee-rate sec YYYY-MM-DD RATE
go run ./cmd/brokerd fee-rate taf YYYY-MM-DD RATE MAX
```

Trades settle on the next trading day, and day orders expire at the close.
Weekends are closed; add each exchange holiday, and each early close in New
York time:

```bash
go run ./cmd/brokerd holiday 2026-11-26 "Thanksgiving Day"
go run ./cmd/brokerd holiday 2026-11-27 "Day after Thanksgiving" 13:00
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
