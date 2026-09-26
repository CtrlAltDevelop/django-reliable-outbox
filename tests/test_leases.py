"""Leases: a claimed row belongs to its worker only until ``locked_until``."""

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from django.db import connection

from reliable_outbox import publish, queries
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.worker import Worker
from tests.helpers import make_messages
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)

ROOT = Path(__file__).resolve().parent.parent


def run_worker_process(*args: str) -> subprocess.CompletedProcess[str]:
    """Run ``manage.py outbox_worker`` in a separate interpreter against the test database."""
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "OUTBOX_DB_NAME": connection.settings_dict["NAME"],
        "DJANGO_SETTINGS_MODULE": "tests.settings",
    }
    return subprocess.run(  # noqa: S603 - fixed argv, our own interpreter
        [sys.executable, "-m", "django", "outbox_worker", "--burst", *args],
        env=env,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.slow
def test_worker_killed_mid_handler_is_retried_after_the_lease_expires() -> None:
    publish("test.record", {"seq": 1, "crash_on_attempt": 1})

    crashed = run_worker_process("--lease", "2")
    assert crashed.returncode == 3, crashed.stderr

    # The dead worker still holds the lease, so nobody may touch the row yet.
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (Status.PENDING, 1)
    assert message.locked_until is not None
    assert Worker(lease_seconds=2).run_once() == 0

    time.sleep(2.2)
    assert Worker(lease_seconds=2).run_once() == 1
    message.refresh_from_db()
    assert (message.status, message.attempts) == (Status.DELIVERED, 2)
    assert list(Handled.objects.values_list("attempt", flat=True)) == [2]


def test_expired_lease_can_be_reclaimed_and_the_late_ack_is_refused() -> None:
    publish("test.record", {"seq": 1})
    (stale,) = queries.claim("default", worker="w1", limit=1, lease_seconds=0.3)

    assert queries.claim("default", worker="w2", limit=1, lease_seconds=5) == []
    time.sleep(0.4)
    (fresh,) = queries.claim("default", worker="w2", limit=1, lease_seconds=5)
    assert (fresh.id, fresh.attempts) == (stale.id, 2)

    # w1 wakes up and tries to finish: the fence rejects both outcomes.
    assert not queries.mark_delivered("default", stale, worker="w1")
    assert not queries.record_failure(
        "default", stale, worker="w1", error="x", traceback="", dead=True
    )
    assert queries.mark_delivered("default", fresh, worker="w2")


def test_a_slow_worker_that_lost_its_lease_rolls_back_its_writes() -> None:
    publish("test.record", {"seq": 1, "sleep": 1.0})
    slow = Worker(name="slow", lease_seconds=0.5)
    thread = threading.Thread(target=slow.run_once)
    thread.start()

    time.sleep(0.7)  # slow's lease has lapsed while its handler still sleeps
    Worker(name="fast", lease_seconds=10).run_once()
    thread.join()

    # Two runs happened, but only the owner's writes survived.
    assert list(Handled.objects.values_list("attempt", flat=True)) == [2]
    message = OutboxMessage.objects.get()
    assert (message.status, message.locked_by) == (Status.DELIVERED, "fast")


def test_worker_gives_back_what_it_cannot_start_within_the_lease() -> None:
    make_messages(3, payload=lambda i: {"seq": i, "sleep": 0.6})
    worker = Worker(lease_seconds=1, batch_size=3)

    # The first message eats more than half the lease; the other two are
    # released untouched, with their attempt refunded.
    assert worker.run_once() == 1
    rows = list(OutboxMessage.objects.order_by("id").values_list("status", "attempts", "locked_by"))
    assert rows == [
        (Status.DELIVERED, 1, worker.name),
        (Status.PENDING, 0, ""),
        (Status.PENDING, 0, ""),
    ]


def test_release_only_touches_our_own_claims() -> None:
    make_messages(2)
    claims = queries.claim("default", worker="w1", limit=2, lease_seconds=30)
    assert queries.release("default", claims, worker="someone-else") == 0
    assert queries.release("default", claims, worker="w1") == 2
    assert queries.release("default", [], worker="w1") == 0
