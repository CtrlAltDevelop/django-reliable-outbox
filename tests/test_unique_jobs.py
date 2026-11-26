from datetime import timedelta

import pytest
from django.db.models import F
from django.db.models.functions import Now

from reliable_outbox import queries
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.operations import requeue
from tests.helpers import drain
from tests.testapp.jobs import noted, record

pytestmark = pytest.mark.django_db(transaction=True)


def test_a_repeat_while_pending_is_a_no_op_that_returns_the_first_id() -> None:
    first = noted.enqueue_with(args=[1], unique_key="u")
    second = noted.enqueue_with(args=[2], unique_key="u")

    assert second == first
    message = OutboxMessage.objects.get()
    assert message.payload["args"] == [1]


def test_unique_keys_are_scoped_to_the_job_name() -> None:
    noted.enqueue_with(args=[1], unique_key="u")
    noted.enqueue_with(args=[1], unique_key="v")
    record.enqueue_with(args=[1], unique_key="u")
    noted.enqueue(1)
    noted.enqueue(1)  # no unique_key: never deduplicated

    assert OutboxMessage.objects.count() == 5


def test_replace_swaps_in_the_new_call_and_schedule() -> None:
    first = noted.enqueue_with(args=[1], unique_key="u", delay=timedelta(hours=1))
    second = noted.enqueue_with(
        args=[2], unique_key="u", on_conflict="replace", delay=timedelta(hours=2), priority=7
    )

    assert second == first
    message = OutboxMessage.objects.get()
    assert (message.payload["args"], message.priority) == ([2], 7)
    later = OutboxMessage.objects.filter(pk=message.pk, run_at__gt=Now() + timedelta(minutes=90))
    assert later.exists()


def test_replace_leaves_a_message_a_worker_is_running_alone() -> None:
    first = noted.enqueue_with(args=[1], unique_key="u")
    OutboxMessage.objects.update(locked_until=Now() + timedelta(minutes=1), locked_by="w")

    again = noted.enqueue_with(args=[2], unique_key="u", on_conflict="replace")

    assert again == first
    assert OutboxMessage.objects.get().payload["args"] == [1]


def test_a_new_message_is_accepted_once_the_first_is_delivered() -> None:
    first = noted.enqueue_with(args=[1], unique_key="u")
    drain()
    assert OutboxMessage.objects.get().status == Status.DELIVERED

    second = noted.enqueue_with(args=[2], unique_key="u")

    assert second != first
    assert OutboxMessage.objects.filter(status=Status.PENDING).count() == 1


def test_a_dead_message_does_not_block_its_unique_key() -> None:
    OutboxMessage.objects.all().delete()
    noted.enqueue_with(args=[1], unique_key="u")
    OutboxMessage.objects.update(status=Status.DEAD, attempts=F("max_attempts"))

    noted.enqueue_with(args=[2], unique_key="u")

    assert OutboxMessage.objects.count() == 2


def test_arguments_are_validated() -> None:
    with pytest.raises(ValueError, match="unique_key"):
        noted.enqueue_with(args=[1], unique_key="")
    with pytest.raises(ValueError, match="on_conflict"):
        noted.enqueue_with(args=[1], unique_key="u", on_conflict="merge")  # type: ignore[arg-type]


def test_a_dead_message_frees_its_unique_key_and_can_be_requeued() -> None:
    noted.enqueue_with(args=[1], unique_key="u")
    dead = OutboxMessage.objects.get()
    claim = queries.claim("default", worker="w", limit=1, lease_seconds=60)[0]
    assert queries.mark_dead("default", claim, worker="w", error="x", traceback="")
    dead.refresh_from_db()
    assert dead.unique_key is None

    noted.enqueue_with(args=[2], unique_key="u")  # accepted: the dead row let go
    assert requeue([dead.pk]) == 1  # and requeueing the old one cannot collide
    assert OutboxMessage.objects.filter(status=Status.PENDING).count() == 2


def test_replace_rewrites_a_delivered_twin_never() -> None:
    first = noted.enqueue_with(args=[1], unique_key="u")
    drain()
    # The twin is gone from the pending set, so replace inserts a new row.
    second = noted.enqueue_with(args=[2], unique_key="u", on_conflict="replace")
    assert second != first
    delivered = OutboxMessage.objects.get(message_id=first)
    assert delivered.payload["args"] == [1]
