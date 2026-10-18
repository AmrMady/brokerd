# 5. Transactional outbox

Status: accepted

## Context

Every posting and fill has to reach Kafka. Writing to Postgres and then to
Kafka can lose an event if the process dies in between, and writing to
Kafka first can publish one for a change that then rolls back.

## Decision

Write each event to an `outbox` table in the same transaction as the change
it describes. A relay publishes the table in `seq` order. Only one relay
publishes at a time: the one holding a Postgres advisory lock on its own
connection, which Postgres releases if that connection drops, so a standby
takes over. The leader publishes a batch, waits for every event to be
acknowledged, then marks the batch published. Events are keyed by account,
so an account's events share a partition.

Consumers record each event's ID in the same transaction as what they do
with it, and skip IDs they've seen. They commit the Kafka offset only after
that transaction.

## Alternatives

- Publishing after the commit. A crash in between loses the event.
- Reading Postgres's logical replication stream. No table to keep, but
  more infrastructure to run, and the events would follow the table layout
  rather than a format of their own.

## Consequences

Delivery is at least once: a relay that dies after publishing but before
marking publishes the batch again, so consumers have to skip duplicates.
Order holds per account within a topic, not across topics. Published events
are purged after a week.
