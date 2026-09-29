# ADR 0005: Wake workers with a NOTIFY trigger; keep polling underneath

- Status: accepted
- Date: 2026-09

## Context

A polling worker delivers a message, on average, half a poll interval after it
commits. Polling faster costs a query per worker per interval even when the
queue is empty. PostgreSQL's `LISTEN/NOTIFY` can wake idle workers the moment a
row commits, but it has sharp edges: notifications are not stored for
listeners that are disconnected, a listener needs a dedicated connection (it
cannot sit behind a transaction pooler), and nothing notifies when a message
becomes due because time passed (a backoff ends, a lease expires, `run_at`
arrives).

## Decision

- **NOTIFY is only a hint.** Workers always wake at least every
  `POLL_INTERVAL`. A lost notification costs at most one interval of latency;
  correctness never depends on it.
- **Send it from a trigger**, `AFTER INSERT ... FOR EACH STATEMENT`, created by
  a migration. Compared with calling `pg_notify` from `publish()`, it costs no
  extra round trip, fires once per statement (a `bulk_create` of 10,000 rows is
  one notification, not 10,000), and covers rows written by any client. Like
  every `NOTIFY`, it is delivered only if the transaction commits, and
  PostgreSQL folds duplicates within one transaction.
- **Requeue notifies explicitly**, since it is an `UPDATE`. An update trigger
  would also fire on every acknowledgement and wake every worker for nothing.
- **One listener connection per worker**, opened lazily with psycopg 3, kept in
  autocommit, reopened after errors. The wait is sliced into short timeouts so
  a stop request is noticed promptly. Without psycopg 3 the worker logs a
  warning and polls.

## Consequences

- Measured on the benchmark machine (see `bench/`), NOTIFY brings
  commit-to-handler latency from about half the poll interval down to tens of
  milliseconds, without polling faster.
- A burst of inserts wakes every idle worker (a thundering herd). `SKIP
  LOCKED` makes that harmless: the losers get empty batches and go back to
  sleep.
- The channel name is fixed (`reliable_outbox`). NOTIFY is scoped to one
  database, so projects sharing a server don't collide unless they share a
  database *and* a table.
- Each worker thread holds two connections (work and listen). Size
  `max_connections` for that.
