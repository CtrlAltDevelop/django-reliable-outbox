from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from reliable_outbox.models import OutboxMessage
from reliable_outbox.worker import Worker


def make_messages(
    count: int,
    *,
    name: str = "test.record",
    key: Callable[[int], str | None] = lambda i: None,
    payload: Callable[[int], Any] = lambda i: {"seq": i},
    max_attempts: int = 5,
) -> list[OutboxMessage]:
    """Insert ``count`` due messages in one statement, ids in ``i`` order."""
    return OutboxMessage.objects.bulk_create(
        OutboxMessage(
            name=name,
            payload=payload(i),
            key=key(i),
            destination="default",
            max_attempts=max_attempts,
            backoff="exponential",
        )
        for i in range(count)
    )


def drain(**worker_options: Any) -> Worker:
    worker = Worker(**worker_options)
    worker.run(burst=True)
    return worker


def statuses(messages: Iterable[OutboxMessage] | None = None) -> dict[str, int]:
    rows = OutboxMessage.objects.all()
    if messages is not None:
        rows = rows.filter(pk__in=[m.pk for m in messages])
    counts: dict[str, int] = {}
    for status in rows.values_list("status", flat=True):
        counts[status] = counts.get(status, 0) + 1
    return counts
