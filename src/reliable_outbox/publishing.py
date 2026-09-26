"""Writing messages into the outbox.

There is no clever machinery here, and that is the point: the row is inserted
through the same connection, inside the same transaction, as the caller's own
writes. If that transaction rolls back, the message never existed. If it
commits, the message is durable before ``COMMIT`` returns, and a worker will
pick it up even if this process dies on the next line.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from fnmatch import fnmatchcase
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.db import models, transaction
from django.db.models.functions import Now

from .conf import Backoff, get_settings
from .models import Kind, OutboxMessage


class NotInTransaction(RuntimeError):
    """Raised when ``REQUIRE_TRANSACTION`` is on and there is no transaction to join."""


def resolve_destination(event_type: str) -> str:
    """Pick a destination for an event type from ``ROUTES``; first match wins."""
    for pattern, destination in get_settings().ROUTES:
        if fnmatchcase(event_type, pattern):
            return destination
    return "default"


def write_message(
    *,
    kind: Kind,
    name: str,
    payload: Any,
    key: str | None,
    destination: str,
    headers: Mapping[str, Any] | None,
    run_at: datetime | None,
    delay: timedelta | None,
    max_attempts: int,
    backoff: Backoff,
    using: str | None,
) -> uuid.UUID:
    """Insert one row on the caller's connection. Shared by ``publish`` and jobs."""
    config = get_settings()
    alias = using or config.DATABASE
    if config.REQUIRE_TRANSACTION and not transaction.get_connection(alias).in_atomic_block:
        raise NotInTransaction(
            f"{name!r} was sent outside transaction.atomic() while REQUIRE_TRANSACTION is on."
        )
    if run_at is not None and delay is not None:
        raise ValueError("Pass run_at or delay, not both.")
    if run_at is not None and run_at.tzinfo is None:
        raise ValueError("run_at must be timezone-aware.")
    if key == "":
        raise ValueError("key must be a non-empty string or None.")

    # A relative delay is added to the database clock, like every other time
    # the worker compares against, so app-server skew can't shift it.
    scheduled: datetime | models.Expression | None = run_at
    if delay is not None:
        scheduled = models.ExpressionWrapper(
            Now() + models.Value(delay), output_field=models.DateTimeField()
        )

    message_id = uuid.uuid4()
    fields: dict[str, Any] = {
        "message_id": message_id,
        "kind": kind,
        "name": name,
        "payload": payload,
        "headers": dict(headers or {}),
        "key": key,
        "destination": destination,
        "max_attempts": max_attempts,
        "backoff": backoff,
    }
    if scheduled is not None:
        fields["run_at"] = scheduled
    OutboxMessage.objects.using(alias).create(**fields)
    return message_id


def publish(
    event_type: str,
    payload: Any,
    *,
    key: str | None = None,
    destination: str | None = None,
    headers: Mapping[str, Any] | None = None,
    run_at: datetime | None = None,
    delay: timedelta | None = None,
    max_attempts: int | None = None,
    using: str | None = None,
) -> uuid.UUID:
    """Record an event, to be delivered only if the surrounding transaction commits.

    ``payload`` must be JSON-serialisable (Django's encoder, so dates, UUIDs and
    decimals are accepted and arrive as strings). Messages that share a ``key``
    are delivered one at a time in insertion order. Returns the ``message_id``
    consumers can deduplicate on.
    """
    if not event_type:
        raise ValueError("event_type must be a non-empty string.")
    config = get_settings()
    target = destination or resolve_destination(event_type)
    if target not in config.DESTINATIONS:
        raise ImproperlyConfigured(f"Unknown outbox destination {target!r}.")
    if max_attempts is not None and max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")
    return write_message(
        kind=Kind.EVENT,
        name=event_type,
        payload=payload,
        key=key,
        destination=target,
        headers=headers,
        run_at=run_at,
        delay=delay,
        max_attempts=max_attempts or config.MAX_ATTEMPTS,
        backoff=config.BACKOFF,
        using=using,
    )
