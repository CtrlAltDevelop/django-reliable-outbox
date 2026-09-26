"""The SQL the worker runs. Kept in one place because the guarantees live here.

Raw SQL rather than the ORM for two reasons: the claim has to be a single
``UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)`` round trip, and every
timestamp must come from the database clock (``now()``), not from Python.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from django.db import connections

from .envelope import Envelope
from .models import OutboxMessage

TABLE = OutboxMessage._meta.db_table


@dataclass(frozen=True, slots=True)
class Claim:
    """A message leased to one worker. ``attempts`` doubles as the fencing token."""

    id: int
    attempts: int
    kind: str
    destination: str
    backoff: str
    envelope: Envelope


_CLAIM = f"""
WITH claimable AS (
    SELECT m.id
    FROM {TABLE} AS m
    WHERE m.status = 'pending'
      AND m.run_at <= now()
      AND (m.locked_until IS NULL OR m.locked_until < now())
    ORDER BY m.id
    LIMIT %(limit)s
    FOR UPDATE OF m SKIP LOCKED
)
UPDATE {TABLE} AS u
SET locked_until = now() + make_interval(secs => %(lease)s),
    locked_by = %(worker)s,
    attempts = u.attempts + 1
FROM claimable
WHERE u.id = claimable.id
RETURNING u.id, u.attempts, u.kind, u.destination, u.backoff, u.message_id, u.name,
          u.payload::text, u.headers::text, u.key, u.max_attempts, u.created_at
"""


def claim(alias: str, *, worker: str, limit: int, lease_seconds: float) -> list[Claim]:
    """Lease up to ``limit`` due messages to ``worker``, oldest first.

    ``SKIP LOCKED`` lets concurrent workers run this at the same time without
    waiting on, or double-claiming, each other's rows. The attempt counter goes
    up at claim time, not on failure, so a message that kills its worker every
    time still runs out of attempts instead of looping forever.
    """
    with connections[alias].cursor() as cursor:
        cursor.execute(_CLAIM, {"limit": limit, "lease": lease_seconds, "worker": worker})
        rows = cursor.fetchall()
    claims = [
        Claim(
            id=row[0],
            attempts=row[1],
            kind=row[2],
            destination=row[3],
            backoff=row[4],
            envelope=Envelope(
                message_id=row[5],
                kind=row[2],
                name=row[6],
                payload=json.loads(row[7]),
                headers=json.loads(row[8]),
                key=row[9],
                attempt=row[1],
                max_attempts=row[10],
                created_at=row[11],
            ),
        )
        for row in rows
    ]
    # RETURNING has no defined order; hand them back in queue order.
    return sorted(claims, key=lambda c: c.id)


# Every write-back is fenced on (id, attempts, locked_by). If this worker's
# lease expired and another worker re-claimed the row, attempts has moved on,
# the UPDATE matches nothing, and the stale worker learns it no longer owns it.
_FENCE = "id = %(id)s AND attempts = %(attempts)s AND locked_by = %(worker)s AND status = 'pending'"


def _fence(claim: Claim, worker: str) -> dict[str, int | str]:
    return {"id": claim.id, "attempts": claim.attempts, "worker": worker}


def mark_delivered(alias: str, claim: Claim, *, worker: str) -> bool:
    """Acknowledge a message. False means the lease was lost to another worker."""
    with connections[alias].cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {TABLE}
            SET status = 'delivered', completed_at = now(), locked_until = NULL
            WHERE {_FENCE}
            """,
            _fence(claim, worker),
        )
        return bool(cursor.rowcount == 1)


def record_failure(
    alias: str, claim: Claim, *, worker: str, error: str, traceback: str, dead: bool
) -> bool:
    """Release a failed message for another attempt, or park it as dead."""
    with connections[alias].cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {TABLE}
            SET status = CASE WHEN %(dead)s THEN 'dead' ELSE 'pending' END,
                completed_at = CASE WHEN %(dead)s THEN now() END,
                locked_until = NULL,
                locked_by = '',
                last_error = %(error)s,
                last_traceback = %(traceback)s
            WHERE {_FENCE}
            """,
            {"dead": dead, "error": error, "traceback": traceback, **_fence(claim, worker)},
        )
        return bool(cursor.rowcount == 1)


def release(alias: str, claims: list[Claim], *, worker: str) -> int:
    """Hand back claimed messages that were never started, refunding the attempt."""
    if not claims:
        return 0
    with connections[alias].cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {TABLE} AS u
            SET locked_until = NULL, locked_by = '', attempts = u.attempts - 1
            FROM unnest(%(ids)s::bigint[], %(attempts)s::integer[]) AS r(id, attempts)
            WHERE u.id = r.id AND u.attempts = r.attempts
              AND u.locked_by = %(worker)s AND u.status = 'pending'
            """,
            {
                "ids": [c.id for c in claims],
                "attempts": [c.attempts for c in claims],
                "worker": worker,
            },
        )
        return int(cursor.rowcount)
