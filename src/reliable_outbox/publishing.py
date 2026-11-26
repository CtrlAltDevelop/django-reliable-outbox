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

from .conf import ON_CONFLICT_CHOICES, Backoff, OnConflict, check_priority, get_settings
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
    priority: int = 0,
    unique_key: str | None = None,
    on_conflict: OnConflict = "skip",
) -> uuid.UUID:
    """Insert one row on the caller's connection. Shared by ``publish`` and jobs.

    With ``unique_key``, a pending row of the same ``name`` and key is reused
    instead: its id is returned and, for ``on_conflict="replace"``, its payload
    and schedule are overwritten (unless a worker has it leased).
    """
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
    if unique_key == "":
        raise ValueError("unique_key must be a non-empty string or None.")
    if on_conflict not in ON_CONFLICT_CHOICES:
        raise ValueError(f"on_conflict must be one of {ON_CONFLICT_CHOICES}, got {on_conflict!r}.")
    check_priority(priority)

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
        "priority": priority,
        "unique_key": unique_key,
    }
    if scheduled is not None:
        fields["run_at"] = scheduled
    manager = OutboxMessage.objects.using(alias)
    if unique_key is None:
        manager.create(**fields)
        return message_id
    return _write_unique(manager, fields, on_conflict=on_conflict)


_UNIQUE_ATTEMPTS = 3


def _write_unique(
    manager: models.QuerySet[OutboxMessage], fields: dict[str, Any], *, on_conflict: OnConflict
) -> uuid.UUID:
    """Insert unless a pending twin exists (``ON CONFLICT DO NOTHING``), then settle which won."""
    message_id: uuid.UUID = fields["message_id"]
    twin = {"name": fields["name"], "unique_key": fields["unique_key"], "status": "pending"}
    # The twin can be delivered between our failed insert and the lookup, so
    # try again; the next insert then succeeds.
    for _ in range(_UNIQUE_ATTEMPTS):
        manager.bulk_create([OutboxMessage(**fields)], ignore_conflicts=True)
        existing = manager.filter(**twin).values_list("message_id", flat=True).first()
        if existing is None:
            continue
        if existing != message_id and on_conflict == "replace":
            # Only a still-pending, unleased row may be rewritten. `key` stays:
            # the row keeps its id, so moving it to another key would reorder it.
            replaced = (
                manager.filter(message_id=existing, status="pending")
                .filter(models.Q(locked_until__isnull=True) | models.Q(locked_until__lt=Now()))
                .update(
                    payload=fields["payload"],
                    headers=fields["headers"],
                    priority=fields["priority"],
                    run_at=fields.get("run_at", Now()),
                )
            )
            if not replaced and not manager.filter(message_id=existing, status="pending").exists():
                continue  # delivered in the meantime: insert a fresh row
        return existing
    raise RuntimeError(f"Could not settle unique message {fields['name']!r}; try again.")


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
    priority: int = 0,
    using: str | None = None,
) -> uuid.UUID:
    """Record an event, to be delivered only if the surrounding transaction commits.

    ``payload`` must be JSON-serialisable (Django's encoder, so dates, UUIDs and
    decimals are accepted and arrive as strings). Messages that share a ``key``
    are delivered one at a time in insertion order; a higher ``priority`` is
    claimed first, but never ahead of an older message of its own key. Returns the ``message_id``
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
        priority=priority,
        using=using,
    )
