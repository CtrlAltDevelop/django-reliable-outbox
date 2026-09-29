import threading
import time
import uuid

import pytest
from django.db import connection, transaction

from reliable_outbox import Envelope, inbox
from reliable_outbox.models import InboxMessage
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


def make_envelope(message_id: uuid.UUID) -> Envelope:
    from django.utils import timezone  # noqa: PLC0415

    return Envelope(
        message_id=message_id,
        kind="event",
        name="order.created",
        payload={},
        key=None,
        headers={},
        attempt=1,
        max_attempts=3,
        created_at=timezone.now(),
    )


def test_first_claim_wins_and_duplicates_are_refused() -> None:
    message_id = uuid.uuid4()
    with transaction.atomic():
        assert inbox.claim(message_id, consumer="billing")
    with transaction.atomic():
        assert not inbox.claim(message_id, consumer="billing")
        assert not inbox.claim(str(message_id), consumer="billing")
        # Each consumer keeps its own ledger.
        assert inbox.claim(message_id, consumer="shipping")
    assert InboxMessage.objects.count() == 2


def test_claim_rolls_back_with_the_work() -> None:
    message_id = uuid.uuid4()
    with pytest.raises(RuntimeError), transaction.atomic():
        inbox.claim(message_id, consumer="billing")
        raise RuntimeError("work failed")
    with transaction.atomic():
        assert inbox.claim(message_id, consumer="billing")


def test_claim_outside_a_transaction_is_refused() -> None:
    with pytest.raises(transaction.TransactionManagementError):
        inbox.claim(uuid.uuid4(), consumer="billing")


def test_idempotent_handler_runs_once_per_message() -> None:
    calls: list[uuid.UUID] = []

    @inbox.idempotent("billing")
    def charge(envelope: Envelope) -> str:
        calls.append(envelope.message_id)
        Handled.objects.create(name="charge", message_id=envelope.message_id)
        return "charged"

    first, second = uuid.uuid4(), uuid.uuid4()
    assert charge(make_envelope(first)) == "charged"
    assert charge(make_envelope(first)) is None
    assert charge(make_envelope(second)) == "charged"
    assert calls == [first, second]
    assert Handled.objects.count() == 2
    assert charge.__name__ == "charge"


def test_concurrent_duplicates_process_once() -> None:
    message_id = uuid.uuid4()
    results: list[bool] = []
    barrier = threading.Barrier(2)

    def consume() -> None:
        try:
            with transaction.atomic():
                barrier.wait()
                won = inbox.claim(message_id, consumer="billing")
                results.append(won)
                time.sleep(0.2)  # the loser is blocked on the unique index meanwhile
        finally:
            connection.close()

    threads = [threading.Thread(target=consume) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == [False, True]
