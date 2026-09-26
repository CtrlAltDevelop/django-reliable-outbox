import random
from datetime import timedelta

import pytest
from django.db import connection
from django.db.models.functions import Now
from django.test import override_settings

from reliable_outbox import publish
from reliable_outbox.backoff import backoff_ceiling, retry_delay
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.worker import Worker


def test_ceiling_doubles_until_the_cap() -> None:
    schedule = [backoff_ceiling(attempt, base=1, cap=60) for attempt in range(1, 10)]
    assert schedule == [1, 2, 4, 8, 16, 32, 60, 60, 60]


def test_ceiling_survives_absurd_attempt_counts() -> None:
    assert backoff_ceiling(10_000, base=1, cap=3600) == 3600
    assert backoff_ceiling(0, base=2, cap=10) == 2


def test_full_jitter_stays_within_the_ceiling_and_uses_the_whole_range() -> None:
    rng = random.Random(1234)
    for attempt in range(1, 12):
        ceiling = backoff_ceiling(attempt, base=0.5, cap=30)
        samples = [
            retry_delay("exponential", attempt, base=0.5, cap=30, rng=rng) for _ in range(500)
        ]
        assert all(0 <= s <= ceiling for s in samples)
        # Full jitter, not "ceiling plus a bit": both ends of the range get used.
        assert min(samples) < ceiling * 0.1
        assert max(samples) > ceiling * 0.9


def test_seeded_schedule_is_reproducible() -> None:
    def draw(seed: int) -> list[float]:
        rng = random.Random(seed)
        return [retry_delay("exponential", a, base=1, cap=60, rng=rng) for a in range(1, 8)]

    assert draw(7) == draw(7)
    assert draw(7) != draw(8)


def test_fixed_backoff_waits_the_base() -> None:
    rng = random.Random(0)
    assert {retry_delay("fixed", a, base=5, cap=60, rng=rng) for a in range(1, 6)} == {5}


@pytest.mark.django_db(transaction=True)
@override_settings(RELIABLE_OUTBOX={"BACKOFF_BASE_SECONDS": 30, "BACKOFF_MAX_SECONDS": 30})
def test_failed_message_is_rescheduled_by_the_database_clock() -> None:
    publish("test.record", {"fail_attempts": 1}, max_attempts=3)
    worker = Worker()
    assert worker.run_once() == 1

    message = OutboxMessage.objects.annotate(db_now=Now()).get()
    assert message.status == Status.PENDING
    assert message.attempts == 1
    assert message.locked_until is None
    assert message.last_error == "RuntimeError: planned failure on attempt 1"
    assert message.created_at <= message.run_at <= message.db_now + timedelta(seconds=30)

    # Nothing is claimable until run_at; pull it forward and it succeeds.
    if message.run_at > message.db_now:
        assert worker.run_once() == 0
    with connection.cursor() as cursor:
        cursor.execute("UPDATE reliable_outbox_outboxmessage SET run_at = now()")
    assert worker.run_once() == 1
    message.refresh_from_db()
    assert (message.status, message.attempts) == (Status.DELIVERED, 2)
