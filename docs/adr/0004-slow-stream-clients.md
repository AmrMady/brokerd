# 4. Slow stream clients

Status: accepted

## Context

The hub runs on the feed's goroutine. One slow WebSocket client must not
slow the feed, the engine behind it, or the other clients.

## Decision

Each client gets a fixed-size ring buffer. When it's full, the oldest
message is dropped and counted. A client that keeps dropping for `MaxLag`
(5s) without catching up, whose write blocks for `MaxLag`, or that doesn't
answer a ping in time, is disconnected. Publishing takes no lock: each
topic's subscribers are an immutable slice that subscription changes copy
and swap in, and each connection's subscription requests are rate limited.
Events keep their sequence numbers, so a client can see what it lost.

## Alternatives

- Blocking the feed until every client has its message. One slow client
  would stall everyone, the engine included.
- Unbounded queues. Memory grows with the slowest client.
- Dropping the newest message instead of the oldest. For prices the newest
  one is the one that matters.

## Consequences

A slow client loses data, and sees it as sequence gaps. One that can't keep
up at all is cut off and has to reconnect and subscribe again. The feed's
cost per event is encoding it once and queueing it for each subscriber.
