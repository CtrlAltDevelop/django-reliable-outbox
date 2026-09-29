"""Operator actions shared by the management commands and the admin."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.db import connections
from django.db.models import QuerySet
from django.db.models.functions import Now

from .conf import get_settings
from .models import InboxMessage, OutboxMessage, Status
from .waiting import notify


def requeue(messages: QuerySet[OutboxMessage] | Iterable[int], *, using: str | None = None) -> int:
    """Give dead messages a fresh set of attempts, due immediately.

    Only dead rows are touched, so requeueing a selection that also holds
    pending or delivered messages is safe. ``last_error`` is kept so the reason
    for the previous failure is still visible after the retry.
    """
    alias = using or get_settings().DATABASE
    if isinstance(messages, QuerySet):
        rows = messages.using(alias)
    else:
        rows = OutboxMessage.objects.using(alias).filter(pk__in=list(messages))
    count = rows.filter(status=Status.DEAD).update(
        status=Status.PENDING,
        attempts=0,
        run_at=Now(),
        locked_until=None,
        locked_by="",
        completed_at=None,
    )
    if count:
        notify(alias)
    return count


@dataclass(frozen=True, slots=True)
class OutboxStats:
    ready: int
    """Due now and waiting for a worker. A number that keeps growing is backlog."""
    in_flight: int
    scheduled: int
    """Pending but not due yet: scheduled with ``run_at``/``delay`` or backing off."""
    retrying: int
    """Pending messages that have failed at least once."""
    dead: int
    delivered: int
    oldest_ready_seconds: float | None
    """How long the oldest due message has been waiting: the queue's lag."""


def stats(*, using: str | None = None) -> OutboxStats:
    """A snapshot of the queue in one scan, cheap enough for a metrics scrape."""
    alias = using or get_settings().DATABASE
    unleased = "(locked_until IS NULL OR locked_until < now())"
    with connections[alias].cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE status = 'pending' AND run_at <= now() AND {unleased}),
                count(*) FILTER (WHERE status = 'pending' AND locked_until >= now()),
                count(*) FILTER (WHERE status = 'pending' AND run_at > now() AND {unleased}),
                count(*) FILTER (WHERE status = 'pending' AND last_error <> ''),
                count(*) FILTER (WHERE status = 'dead'),
                count(*) FILTER (WHERE status = 'delivered'),
                extract(epoch FROM now() - min(run_at)
                        FILTER (WHERE status = 'pending' AND run_at <= now() AND {unleased}))
            FROM {OutboxMessage._meta.db_table}
            """
        )
        # An aggregate without GROUP BY always returns exactly one row.
        ready, in_flight, scheduled, retrying, dead, delivered, lag = cursor.fetchone()
    return OutboxStats(
        ready=ready,
        in_flight=in_flight,
        scheduled=scheduled,
        retrying=retrying,
        dead=dead,
        delivered=delivered,
        oldest_ready_seconds=None if lag is None else float(lag),
    )


def cleanup(
    *,
    older_than: timedelta,
    include_dead: bool = False,
    inbox: bool = False,
    batch_size: int = 5_000,
    using: str | None = None,
) -> int:
    """Delete finished rows older than ``older_than``, in batches.

    Small batches keep each DELETE's locks and WAL burst short, so cleanup can
    run next to live workers. Dead rows are kept unless asked for: they are
    the evidence someone still has to look at. With ``inbox``, dedup records
    past the same age go too; a duplicate arriving after that is no longer
    recognised, so pick an age well beyond your longest redelivery window.
    """
    alias = using or get_settings().DATABASE
    statuses = ["delivered", "dead"] if include_dead else ["delivered"]
    deleted = _delete_in_batches(
        alias,
        OutboxMessage._meta.db_table,
        "status = ANY(%s) AND completed_at < now() - %s",
        [statuses, older_than],
        batch_size,
    )
    if inbox:
        deleted += _delete_in_batches(
            alias,
            InboxMessage._meta.db_table,
            "received_at < now() - %s",
            [older_than],
            batch_size,
        )
    return deleted


def _delete_in_batches(
    alias: str, table: str, condition: str, params: list[Any], batch_size: int
) -> int:
    deleted: int = 0
    with connections[alias].cursor() as cursor:
        while True:
            cursor.execute(
                f"DELETE FROM {table} WHERE id IN "
                f"(SELECT id FROM {table} WHERE {condition} LIMIT %s)",
                [*params, batch_size],
            )
            batch = int(cursor.rowcount)
            deleted += batch
            if batch < batch_size:
                return deleted
