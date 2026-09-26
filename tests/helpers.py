from __future__ import annotations

import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from django.db.models import Q
from django.db.models.functions import Now

from reliable_outbox.models import OutboxMessage, Status
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


def due_soon() -> bool:
    """Is anything unleased going to become claimable within the next second?"""
    return (
        OutboxMessage.objects.filter(
            status=Status.PENDING, run_at__lte=Now() + timedelta(seconds=1)
        )
        .filter(Q(locked_until__isnull=True) | Q(locked_until__lt=Now()))
        .exists()
    )


def drain(timeout: float = 15.0, **worker_options: Any) -> Worker:
    """Run a worker until nothing is due soon, riding out the tests' short backoffs."""
    worker = Worker(**worker_options)
    deadline = time.monotonic() + timeout
    while True:
        worker.run(burst=True)
        if not due_soon() or time.monotonic() > deadline:
            return worker
        time.sleep(0.02)


def statuses() -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in OutboxMessage.objects.values_list("status", flat=True):
        counts[status] = counts.get(status, 0) + 1
    return counts
