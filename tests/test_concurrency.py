"""The claim is the whole ballgame: many workers, one queue, no double success."""

import threading

import pytest
from django.db.models import Count

from reliable_outbox.models import OutboxMessage
from reliable_outbox.worker import Worker
from tests.helpers import make_messages, statuses
from tests.testapp import outbox_handlers
from tests.testapp.models import Handled

pytestmark = [pytest.mark.django_db(transaction=True), pytest.mark.slow]


def run_workers(count: int, **options: object) -> None:
    errors: list[BaseException] = []

    def target(index: int) -> None:
        try:
            Worker(name=f"test:{index}", **options).run(burst=True)  # type: ignore[arg-type]
        except BaseException as exc:  # surfaced below, not swallowed
            errors.append(exc)

    threads = [threading.Thread(target=target, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_ten_workers_ten_thousand_messages_each_handled_once() -> None:
    outbox_handlers.invocations.clear()
    messages = make_messages(10_000)

    run_workers(10, batch_size=10)

    assert statuses() == {"delivered": 10_000}
    # Every success is a row written in the same transaction as the ack.
    assert Handled.objects.count() == 10_000
    assert not Handled.objects.values("message_id").annotate(n=Count("id")).filter(n__gt=1)
    # And no handler was even *started* twice: SKIP LOCKED never hands one row
    # to two workers while its lease is live.
    assert len(outbox_handlers.invocations) == 10_000
    assert set(outbox_handlers.invocations.values()) == {1}
    assert set(OutboxMessage.objects.values_list("attempts", flat=True)) == {1}
    assert len(set(OutboxMessage.objects.values_list("locked_by", flat=True))) == 10
    assert len(messages) == 10_000
