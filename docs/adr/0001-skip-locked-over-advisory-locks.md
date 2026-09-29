# ADR 0001: Claim work with `FOR UPDATE SKIP LOCKED`, not advisory locks

- Status: accepted
- Date: 2026-09

## Context

Several workers, possibly on several machines, poll one table. Two of them must
never run the same message at the same time, and a busy row should not make
anyone wait. There is no broker and we do not want a coordinator process.

The options PostgreSQL gives us:

1. **`SELECT ... FOR UPDATE`** (no `SKIP LOCKED`). Correct, but every worker
   queues behind the first one's row locks, so N workers behave like one.
2. **Advisory locks** (`pg_try_advisory_lock(id)`). Workers select candidates
   and try to lock each id. Locks are held per *session*, not per transaction,
   so a pooled connection (PgBouncer in transaction mode) can hand a lock to
   the wrong client or keep it forever. Filtering with `pg_try_advisory_lock`
   in a `WHERE` clause also locks rows the planner looks at but `LIMIT` then
   discards, a well-known footgun. And the lock tells nobody else what is
   happening: it lives in `pg_locks`, not in the row.
3. **`FOR UPDATE SKIP LOCKED`** (PostgreSQL 9.5+). Rows another transaction
   has locked are skipped, not waited on. Transaction-scoped, so it is
   compatible with transaction pooling, and the lock disappears with the
   transaction if the client dies.

## Decision

Claim with one statement:

```sql
WITH claimable AS (
    SELECT id FROM outbox WHERE <due, unleased, head of its key>
    ORDER BY id LIMIT :batch
    FOR UPDATE OF m SKIP LOCKED
)
UPDATE outbox SET locked_until = now() + :lease, locked_by = :worker, attempts = attempts + 1
FROM claimable WHERE outbox.id = claimable.id
RETURNING ...
```

The row lock only lives for this short claim transaction. Ownership after that
is recorded in the row itself as a lease (ADR 0002), which is what makes the
state visible in the admin and recoverable after a crash.

## Consequences

- Workers scale out without configuration; they coordinate through row locks
  alone. The 10-workers × 10,000-messages test checks that no handler is ever
  started twice while a lease is live.
- `SKIP LOCKED` gives an inconsistent view *on purpose*: a worker may see an
  empty batch while rows exist that others hold. Fine for a queue; it would be
  wrong for anything that needs an exact count.
- A partial index `(id) WHERE status = 'pending'` keeps the claim cheap
  even when the table holds millions of delivered rows awaiting cleanup.
- PostgreSQL-only. MySQL 8 has `SKIP LOCKED` too but no `LISTEN/NOTIFY` and
  different locking semantics around the subquery; supporting it is out of
  scope rather than impossible.
