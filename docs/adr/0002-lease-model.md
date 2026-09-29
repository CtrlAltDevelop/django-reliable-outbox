# ADR 0002: Leases with a fencing token instead of long-held locks

- Status: accepted
- Date: 2026-09

## Context

A worker that claims a message can die at any point: OOM-killed, `SIGKILL`ed
by a deploy, its node lost. Its message must come back. The obvious way to hold
a message while working on it, keeping the claim transaction open with the row
locked, would free the row automatically when the connection dies, but it
means one open transaction per in-flight message, for as long as the handler
runs. That blocks vacuum, pins old snapshots, and ties an HTTP call's latency to
a database transaction.

## Decision

Ownership is a **lease** stored in the row: `locked_until` (and `locked_by` for
humans). The claim sets `locked_until = now() + LEASE_SECONDS` and commits at
once. A pending row with `locked_until` in the past is claimable again, so a
dead worker's messages return by themselves once their lease runs out.

Leases can expire under a worker that is merely slow, so every write-back is
**fenced**:

```sql
UPDATE outbox SET status = 'delivered' ...
WHERE id = :id AND attempts = :attempts AND locked_by = :worker AND status = 'pending'
```

`attempts` is incremented by every claim, so it works as a fencing token: if
another worker has re-claimed the row, the stale worker's update matches
nothing. Because the handler runs in the *same transaction* as that update, the
stale worker then rolls back, and its database writes vanish with it. Only
the current owner's run can commit.

Further details:

- **The database clock only.** `now()` sets and checks every lease and every
  `run_at`, so skew between app servers can't make a lease look longer or
  shorter than it is.
- **Attempts are counted at claim time**, not on failure. A message that
  crashes its worker every time still runs out of attempts and is
  dead-lettered, instead of looping forever.
- **Don't start what can't finish.** A worker holding a batch checks, before
  each message, that more than half the lease is left. If not, it releases the
  rest of the batch (refunding their attempt) rather than run on a lease that
  may lapse mid-handler.
- **No heartbeat.** Extending the lease from a background thread would let
  handlers run arbitrarily long, but it also keeps a hung handler's message
  locked forever, and adds a thread per worker. We chose a fixed lease that
  must exceed the slowest handler; set `LEASE_SECONDS` accordingly.

## Consequences

- Crash recovery needs no reaper process and no coordination: it is a `WHERE`
  clause. The subprocess crash test kills a worker with `os._exit` mid-handler
  and checks that the message is untouchable until its lease expires and is
  delivered, once, after.
- Recovery latency after a crash is up to `LEASE_SECONDS`. Short leases recover
  quickly but risk expiring under slow handlers (which is safe thanks to the
  fence, but wastes work); long leases do the reverse.
- For side effects **outside** the database (an email, an HTTP call), a lost
  lease means they happen twice. This is inherent to at-least-once delivery;
  see ADR 0004.
