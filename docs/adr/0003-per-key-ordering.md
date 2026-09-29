# ADR 0003: Per-key ordering by claiming only the head of each key

- Status: accepted
- Date: 2026-09

## Context

Events about one aggregate must usually be applied in order: `order.paid` after
`order.created`. Events about different aggregates have no order relation and
should be processed in parallel. A single global FIFO would be correct and slow;
full parallelism would be fast and wrong.

Ordering must hold across many workers, across retries (a failed message may
not be overtaken while it backs off), and across worker crashes.

Options considered:

1. **Partition keys onto workers** (hash(key) mod N, like Kafka partitions).
   Needs membership and rebalancing, which is exactly the coordination SKIP
   LOCKED lets us avoid.
2. **Advisory lock per key.** Serialises processing but not *order*: after a
   failure, a later message of the key can grab the lock first.
3. **Claim only the head of each key.** A keyed message is claimable only when
   no older message with the same key is unfinished.

## Decision

Option 3, as one extra predicate in the claim query:

```sql
AND (m.key IS NULL OR NOT EXISTS (
    SELECT 1 FROM outbox e
    WHERE e.key = m.key AND e.id < m.id AND e.status IN ('pending', 'dead')))
```

"Unfinished" includes pending rows that are leased, waiting for `run_at`, or
backing off after a failure. So the head of a key is the only candidate, two
workers can never hold messages of one key at once, and a retrying message
holds back its successors. A partial index on `(key, id)` covering unfinished
rows keeps the check to an index probe.

**Dead heads are configurable** (`BLOCK_KEY_ON_DEAD_LETTER`, default on). On,
a dead-lettered message blocks its key until an operator requeues it: strict
order, at the price of a stalled key. Off, dead rows are ignored and later
messages proceed: availability over order. The right answer depends on whether
a skipped event corrupts the consumer's state, so it is the user's call.

Unkeyed messages (`key=None`) are never ordered and never blocked.

## What "insertion order" means

Order is the `id` sequence, assigned at `INSERT` time. If two *concurrent*
transactions publish for the same key, their ids and their commit order can
disagree, and a worker may process whichever commits first. That is not a
violation: concurrent transactions have no order to preserve. When order
matters, the transactions are causally ordered already, typically because
both lock the aggregate row (`select_for_update()`), and then ids follow that
order. Publish after taking the lock.

## Consequences

- Throughput per key is one message at a time, end to end (claim, run, ack).
  A hot key is a sequential bottleneck by design; spread load across keys.
- A batch never holds more than one message of a key, so a key with a deep
  backlog drains at one message per claim round trip.
- The ordering test runs 8 workers over 40 interleaved keys with injected
  first-attempt failures and checks every key's sequence.
