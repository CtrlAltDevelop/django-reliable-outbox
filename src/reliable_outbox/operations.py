"""Operator actions shared by the management commands and the admin."""

from __future__ import annotations

from collections.abc import Iterable

from django.db.models import QuerySet
from django.db.models.functions import Now

from .conf import get_settings
from .models import OutboxMessage, Status


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
    return rows.filter(status=Status.DEAD).update(
        status=Status.PENDING,
        attempts=0,
        run_at=Now(),
        locked_until=None,
        locked_by="",
        completed_at=None,
    )
