"""LISTEN/NOTIFY: idle workers wake on commit instead of on the next poll."""

import threading
import time
from collections.abc import Iterator

import psycopg
import pytest
from django.db import connection, transaction

from reliable_outbox import publish
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.operations import requeue
from reliable_outbox.waiting import CHANNEL, NotifyWaiter, PollingWaiter, make_waiter
from reliable_outbox.worker import Worker
from tests.helpers import drain
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def listener() -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    params = connection.get_connection_params()
    params.pop("cursor_factory", None)
    params.pop("context", None)
    conn = psycopg.connect(**params, autocommit=True)
    conn.execute(f"LISTEN {CHANNEL}")
    yield conn
    conn.close()


def received(conn: psycopg.Connection[tuple[object, ...]], timeout: float = 0.3) -> int:
    return sum(1 for _ in conn.notifies(timeout=timeout))


def test_notification_is_sent_only_when_the_insert_commits(
    listener: psycopg.Connection[tuple[object, ...]],
) -> None:
    with transaction.atomic():
        publish("test.record", {"seq": 1})
        publish("test.record", {"seq": 2})
        assert received(listener) == 0  # not while the transaction is open
    # Postgres folds identical notifications from one transaction into one.
    assert received(listener) == 1

    with pytest.raises(ZeroDivisionError), transaction.atomic():
        publish("test.record", {"seq": 3})
        _ = 1 / 0
    assert received(listener) == 0


def test_requeue_wakes_workers(listener: psycopg.Connection[tuple[object, ...]]) -> None:
    publish("test.permanent", {})
    received(listener)
    drain()
    assert requeue(OutboxMessage.objects.all()) == 1
    assert received(listener) == 1


def run_in_background(worker: Worker) -> threading.Thread:
    thread = threading.Thread(target=worker.run)
    thread.start()
    return thread


def wait_for_delivery(timeout: float) -> float | None:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if OutboxMessage.objects.filter(status=Status.DELIVERED).exists():
            return time.monotonic() - started
        time.sleep(0.01)
    return None


@pytest.mark.parametrize("notify", [True, False])
def test_idle_worker_latency_with_and_without_notify(notify: bool) -> None:
    # A 30s poll interval: only NOTIFY can make delivery fast.
    worker = Worker(poll_interval=30, notify=notify)
    thread = run_in_background(worker)
    try:
        time.sleep(0.5)  # let it go idle
        with transaction.atomic():
            publish("test.record", {"seq": 1})
        latency = wait_for_delivery(timeout=3)
    finally:
        worker.stop()
        thread.join(timeout=5)
    assert not thread.is_alive(), "stop() must interrupt the wait"
    if notify:
        assert latency is not None
        assert latency < 1.0
    else:
        assert latency is None
    assert Handled.objects.count() == (1 if notify else 0)


def test_listener_failure_falls_back_to_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    waiter = NotifyWaiter("default", threading.Event())

    def broken() -> None:
        raise psycopg.OperationalError("listener down")

    monkeypatch.setattr(waiter, "_connect", broken)
    started = time.monotonic()
    waiter.wait(0.3)
    assert 0.25 < time.monotonic() - started < 2
    waiter.close()


def test_waiter_selection() -> None:
    stop = threading.Event()
    assert isinstance(make_waiter("default", stop, notify=True), NotifyWaiter)
    assert isinstance(make_waiter("default", stop, notify=False), PollingWaiter)
