import json
import uuid
from datetime import timedelta
from io import StringIO
from typing import Any

import pytest
from django.core.management import call_command
from django.db import connection, transaction

from reliable_outbox import inbox, publish, queries
from reliable_outbox.models import InboxMessage, OutboxMessage
from reliable_outbox.operations import cleanup, stats
from reliable_outbox.signals import message_dead_lettered, message_delivered, message_failed
from tests.helpers import drain, statuses

pytestmark = pytest.mark.django_db(transaction=True)


def age_everything(hours: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE reliable_outbox_outboxmessage "
            "SET completed_at = completed_at - make_interval(hours => %s)",
            [hours],
        )
        cursor.execute(
            "UPDATE reliable_outbox_inboxmessage "
            "SET received_at = received_at - make_interval(hours => %s)",
            [hours],
        )


def test_stats_snapshot() -> None:
    assert stats().ready == 0
    assert stats().oldest_ready_seconds is None

    publish("test.record", {"seq": 1})  # will be leased
    publish("test.record", {"seq": 2})  # ready
    publish("reminder.due", {}, delay=timedelta(hours=1))  # scheduled
    publish("test.permanent", {})  # will be dead
    queries.claim("default", worker="w", limit=1, lease_seconds=60)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE reliable_outbox_outboxmessage SET status = 'dead' WHERE name = 'test.permanent'"
        )

    snapshot = stats()
    assert (snapshot.ready, snapshot.in_flight, snapshot.scheduled) == (1, 1, 1)
    assert (snapshot.dead, snapshot.delivered, snapshot.retrying) == (1, 0, 0)
    assert snapshot.oldest_ready_seconds is not None
    assert snapshot.oldest_ready_seconds >= 0

    out = StringIO()
    call_command("outbox_stats", "--json", stdout=out)
    assert json.loads(out.getvalue())["in_flight"] == 1
    out = StringIO()
    call_command("outbox_stats", stdout=out)
    assert "oldest_ready_seconds" in out.getvalue()


def test_cleanup_keeps_recent_pending_and_dead_rows() -> None:
    publish("test.record", {"seq": 1})
    publish("test.permanent", {})
    drain()
    publish("test.record", {"seq": 2}, delay=timedelta(hours=1))  # still pending
    with transaction.atomic():
        inbox.claim(uuid.uuid4(), consumer="billing")

    assert cleanup(older_than=timedelta(hours=1)) == 0  # nothing old enough yet
    age_everything(hours=2)

    assert cleanup(older_than=timedelta(hours=1), batch_size=1) == 1
    assert statuses() == {"dead": 1, "pending": 1}
    assert cleanup(older_than=timedelta(hours=1), include_dead=True, inbox=True) == 2
    assert statuses() == {"pending": 1}
    assert not InboxMessage.objects.exists()


def test_cleanup_command() -> None:
    for seq in range(3):
        publish("test.record", {"seq": seq})
    drain()
    age_everything(hours=24 * 8)
    out = StringIO()
    call_command("outbox_cleanup", "--batch-size", "2", stdout=out)
    assert "Deleted 3 row(s) older than 168h." in out.getvalue()
    assert not OutboxMessage.objects.exists()


def test_metrics_signals() -> None:
    seen: list[tuple[str, dict[str, Any]]] = []

    def listener(name: str) -> Any:
        def receive(sender: object, **kwargs: Any) -> None:
            seen.append((name, kwargs))

        return receive

    receivers = {
        message_delivered: listener("delivered"),
        message_failed: listener("failed"),
        message_dead_lettered: listener("dead"),
    }
    for signal, receiver in receivers.items():
        signal.connect(receiver)
    try:
        publish("test.record", {"seq": 1, "fail_attempts": 1})
        publish("test.permanent", {})
        drain()
    finally:
        for signal, receiver in receivers.items():
            signal.disconnect(receiver)

    kinds = sorted(name for name, _ in seen)
    assert kinds == ["dead", "delivered", "failed"]
    delivered = next(kwargs for name, kwargs in seen if name == "delivered")
    assert delivered["envelope"].attempt == 2
    assert delivered["duration"] >= 0
    assert delivered["queued_for"] >= 0
    failed = next(kwargs for name, kwargs in seen if name == "failed")
    assert failed["retry_in"] >= 0
    assert isinstance(failed["error"], RuntimeError)
