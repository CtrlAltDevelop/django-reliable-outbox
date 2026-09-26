from io import StringIO

import pytest
from django.core.management import CommandError, call_command

from reliable_outbox import publish
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.operations import requeue
from tests.helpers import drain, statuses
from tests.testapp.jobs import flaky
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


def requeue_command(*args: str) -> str:
    out = StringIO()
    call_command("outbox_requeue", *args, stdout=out)
    return out.getvalue()


def test_exhausted_job_is_dead_lettered_with_its_traceback() -> None:
    flaky.enqueue(7)
    drain()
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (Status.DEAD, flaky.max_attempts)
    assert message.last_error == "RuntimeError: flaky job failed for 7"
    assert "Traceback (most recent call last)" in message.last_traceback
    assert "tests/testapp/jobs.py" in message.last_traceback.replace("\\", "/")
    assert message.completed_at is not None


def test_permanent_error_skips_the_remaining_attempts() -> None:
    publish("test.permanent", {}, max_attempts=10)
    drain()
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (Status.DEAD, 1)
    assert message.last_error == "PermanentError: customer 42 no longer exists"


def test_requeued_message_gets_fresh_attempts_and_succeeds() -> None:
    publish("test.record", {"seq": 1, "fail_attempts": 2}, max_attempts=2)
    drain()
    message = OutboxMessage.objects.get()
    assert message.status == Status.DEAD

    assert "Requeued 1 dead message(s)." in requeue_command(str(message.pk))
    message.refresh_from_db()
    assert (message.status, message.attempts, message.completed_at) == (Status.PENDING, 0, None)
    assert message.last_error  # history is kept

    # Attempt counting restarts, so the handler's "fail on attempts 1-2" rule
    # bites again and the message ends up dead a second time. The point here is
    # that it was genuinely retried.
    drain()
    message.refresh_from_db()
    assert (message.status, message.attempts) == (Status.DEAD, 2)


def test_requeue_ignores_rows_that_are_not_dead() -> None:
    publish("test.record", {"seq": 1})
    publish("test.permanent", {})
    drain()
    assert statuses() == {"delivered": 1, "dead": 1}
    assert requeue(OutboxMessage.objects.all()) == 1
    assert statuses() == {"delivered": 1, "pending": 1}
    assert Handled.objects.count() == 1


def test_command_filters_and_dry_run() -> None:
    for _ in range(2):
        publish("test.permanent", {}, key="a")
    publish("test.permanent", {}, key="b")
    drain()

    assert "Would requeue 2" in requeue_command("--key", "a", "--dry-run")
    assert statuses() == {"dead": 3}
    assert "Requeued 2" in requeue_command("--key", "a", "--name", "test.permanent")
    assert "Requeued 1" in requeue_command("--all")


def test_command_refuses_to_guess() -> None:
    with pytest.raises(CommandError, match="Say what to requeue"):
        requeue_command()
