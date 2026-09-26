from datetime import timedelta

import pytest
from django.core.management import call_command
from django.db import transaction
from django.test import override_settings

from reliable_outbox import handler, handlers, publish
from reliable_outbox.handlers import handlers_for
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.worker import Worker, default_worker_name
from tests.helpers import drain, make_messages, statuses
from tests.testapp import outbox_handlers
from tests.testapp.jobs import record as record_job
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


def test_committed_event_reaches_its_handler() -> None:
    with transaction.atomic():
        message_id = publish("test.record", {"seq": 1}, key="k")

    assert drain().run_once() == 0
    handled = Handled.objects.get()
    assert (handled.message_id, handled.key, handled.seq, handled.attempt) == (
        message_id,
        "k",
        1,
        1,
    )
    message = OutboxMessage.objects.get()
    assert message.status == Status.DELIVERED
    assert message.completed_at is not None
    assert message.locked_until is None


def test_enqueued_job_runs() -> None:
    with transaction.atomic():
        record_job.enqueue(42, label="job")
    drain()
    assert Handled.objects.get().seq == 42
    assert statuses() == {"delivered": 1}


def test_pattern_handlers_receive_matching_events() -> None:
    publish("audit.login", {})
    drain()
    assert Handled.objects.get().name == "audit:audit.login"


def test_handler_writes_roll_back_with_a_failed_attempt() -> None:
    # The handler writes a row and then raises on attempts 1 and 2: those rows
    # must vanish with the failed transaction, leaving only attempt 3's.
    publish("test.record", {"seq": 1, "fail_attempts": 2})
    drain()
    assert list(Handled.objects.values_list("attempt", flat=True)) == [3]
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (Status.DELIVERED, 3)


def test_exhausted_attempts_park_the_message_as_dead() -> None:
    publish("test.record", {"fail_attempts": 99}, max_attempts=3)
    drain()
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (Status.DEAD, 3)
    assert message.last_error == "RuntimeError: planned failure on attempt 3"
    assert "planned failure" in message.last_traceback
    assert not Handled.objects.exists()


def test_event_without_a_handler_is_not_marked_delivered() -> None:
    publish("nobody.listens", {}, max_attempts=1)
    drain()
    message = OutboxMessage.objects.get()
    assert message.status == Status.DEAD
    assert message.last_error.startswith("NoHandler")


def test_future_messages_wait_for_their_time() -> None:
    publish("test.record", {"seq": 1}, delay=timedelta(hours=1))
    assert drain().run_once() == 0
    assert statuses() == {"pending": 1}


def test_batches_are_claimed_in_queue_order() -> None:
    make_messages(25)
    worker = Worker(batch_size=10)
    assert worker.run_once() == 10
    assert list(Handled.objects.order_by("id").values_list("seq", flat=True)) == list(range(10))
    drain()
    assert Handled.objects.count() == 25


def test_command_drains_in_burst_mode() -> None:
    make_messages(3)
    call_command("outbox_worker", "--burst", "--batch-size", "2")
    assert statuses() == {"delivered": 3}


@override_settings(
    RELIABLE_OUTBOX={"DESTINATIONS": {"default": {"BACKEND": "tests.test_worker.NotADestination"}}}
)
def test_destination_must_implement_the_protocol() -> None:
    publish("test.record", {}, max_attempts=1)
    drain()
    assert "does not implement" in OutboxMessage.objects.get().last_error


class NotADestination:
    pass


def test_registry_is_idempotent_and_ordered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(handlers._exact, "test.record", list(handlers._exact["test.record"]))

    def extra(envelope: object) -> None: ...

    handler("test.record")(extra)
    handler("test.record")(extra)
    assert handlers_for("test.record") == [outbox_handlers.record, extra]


def test_worker_names_identify_host_process_and_slot() -> None:
    assert default_worker_name(3).endswith(":3")
