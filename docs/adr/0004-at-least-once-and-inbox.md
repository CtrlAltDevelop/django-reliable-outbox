# ADR 0004: At-least-once delivery, and an inbox for effectively-once effects

- Status: accepted
- Date: 2026-09

## Context

Between "the destination accepted the message" and "the outbox row says
delivered" there is always a gap. If the worker dies inside it, the message
will be sent again. No design closes that gap for an external system that
isn't part of our transaction. Exactly-once *delivery* across a network
boundary is not something we can honestly promise.

## Decision

1. **Promise at-least-once, say so plainly.** The README's guarantees table
   states it in the first row. A message is never lost once its transaction
   commits, and may be delivered more than once.
2. **Make the common case exactly-once for database effects.** Local handlers
   and jobs run in the same transaction as the fenced acknowledgement
   (ADR 0002). Their own writes to the outbox's database commit if and only if
   the message is marked delivered, so for work that only touches that
   database, duplicates cannot produce duplicate effects.
3. **Give consumers an inbox.** Everything else (a remote consumer of a
   webhook, a service reading Redis or Kafka, a handler that also writes
   elsewhere) deduplicates on `message_id`. The library ships the pattern for
   Django consumers:

   ```python
   @handler("order.created")
   @inbox.idempotent("billing")
   def charge(envelope): ...
   ```

   `inbox.claim()` does `INSERT ... ON CONFLICT DO NOTHING` on
   `(consumer, message_id)` in the same transaction as the consumer's work.
   It refuses to run in autocommit, where the record would commit before the
   work and a crash between them would drop the message.

## Why not two-phase commit / transactional producers?

XA/2PC with a broker would close the gap for brokers that support it, at the
cost of in-doubt transactions, a coordinator, and a much larger failure
surface. Kafka's transactional producer gives exactly-once *within* Kafka, not
between PostgreSQL and Kafka. Neither is worth it next to a unique index on
the consumer side.

## Consequences

- Handlers with external side effects must be idempotent or guarded by an
  idempotency key (most payment and email APIs accept one; pass `message_id`).
- Inbox rows accumulate. `outbox_cleanup --inbox` removes them past the
  retention age; a duplicate that arrives after its record was deleted is no
  longer recognised, so retention must exceed the longest redelivery window.
- `PermanentError` exists so handlers can say "don't retry this" and send a
  message straight to the dead-letter state, without burning attempts on input
  that will never succeed.
