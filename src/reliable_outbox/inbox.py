"""Consumer-side deduplication: the Inbox pattern.

The outbox promises at-least-once delivery. The inbox is the other half of the
bargain: before doing the work, the consumer records the ``message_id`` in the
same transaction as its own writes. A duplicate delivery then finds the row
already there and skips, and a crash rolls both back together.

    with transaction.atomic():
        if not inbox.claim(envelope.message_id, consumer="billing"):
            return
        charge_customer(...)
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from uuid import UUID

from django.db import connections, transaction

from .conf import get_settings
from .envelope import Envelope
from .models import InboxMessage

_TABLE = InboxMessage._meta.db_table


def claim(message_id: UUID | str, *, consumer: str, using: str | None = None) -> bool:
    """Record ``message_id`` for ``consumer``. True the first time, False for a duplicate.

    Must run inside ``transaction.atomic()``: in autocommit the record would
    commit before the work, and a crash in between would drop the message for
    good. A concurrent duplicate blocks on the unique index until the first
    transaction finishes, then sees its row (or takes over if it rolled back).
    """
    alias = using or get_settings().DATABASE
    connection = connections[alias]
    if not connection.in_atomic_block:
        raise transaction.TransactionManagementError(
            "inbox.claim() must run inside transaction.atomic() with the work it guards."
        )
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {_TABLE} (consumer, message_id, received_at)
            VALUES (%s, %s, now())
            ON CONFLICT (consumer, message_id) DO NOTHING
            RETURNING 1
            """,
            [consumer, str(message_id)],
        )
        return cursor.fetchone() is not None


def idempotent[R](
    consumer: str, *, using: str | None = None
) -> Callable[[Callable[[Envelope], R]], Callable[[Envelope], R | None]]:
    """Wrap a handler so each ``message_id`` is processed at most once per ``consumer``.

    Duplicates return None without calling the handler.
    """

    def decorate(fn: Callable[[Envelope], R]) -> Callable[[Envelope], R | None]:
        @functools.wraps(fn)
        def wrapper(envelope: Envelope) -> R | None:
            alias = using or get_settings().DATABASE
            with transaction.atomic(using=alias):
                if not claim(envelope.message_id, consumer=consumer, using=alias):
                    return None
                return fn(envelope)

        return wrapper

    return decorate
