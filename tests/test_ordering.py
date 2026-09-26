"""Per-key ordering: one key is a single-file line, different keys run side by side."""

import threading
from collections import defaultdict

import pytest
from django.db import connection
from django.test import override_settings

from reliable_outbox import publish
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.operations import requeue
from reliable_outbox.worker import Worker
from tests.helpers import drain, due_soon, make_messages, statuses
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


def run_workers(count: int, **options: object) -> None:
    def target(index: int) -> None:
        worker = Worker(name=f"order:{index}", **options)  # type: ignore[arg-type]
        try:
            while True:  # ride out the short test backoffs
                worker.run(burst=True)
                if not due_soon():
                    return
        finally:
            connection.close()  # due_soon() opened this thread's own connection

    threads = [threading.Thread(target=target, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def handled_order() -> dict[str, list[int]]:
    """Sequence numbers per key, in the order their successful runs committed."""
    order: dict[str, list[int]] = defaultdict(list)
    for key, seq in Handled.objects.order_by("id").values_list("key", "seq"):
        assert seq is not None
        order[key].append(seq)
    return order


@pytest.mark.slow
def test_same_key_runs_in_insertion_order_across_many_workers() -> None:
    keys, per_key = 40, 25
    # Interleaved inserts: key-0 #0, key-1 #0, ..., key-0 #1, ... and every
    # fifth message fails its first attempt, so retries have to be waited for.
    make_messages(
        keys * per_key,
        key=lambda i: f"key-{i % keys}",
        payload=lambda i: {"seq": i // keys, "fail_attempts": 1 if i % 5 == 0 else 0},
    )

    run_workers(8, batch_size=5)

    assert statuses() == {"delivered": keys * per_key}
    order = handled_order()
    assert len(order) == keys
    for key, seqs in order.items():
        assert seqs == list(range(per_key)), key
    # And it was genuinely parallel, not one worker doing everything.
    assert len(set(OutboxMessage.objects.values_list("locked_by", flat=True))) > 1


def test_only_the_head_of_a_key_is_claimable() -> None:
    make_messages(3, key=lambda i: "k")
    make_messages(2)  # unkeyed
    claimed = Worker(batch_size=10).run_once()
    # key "k" contributes only its head; both unkeyed messages go alongside it.
    assert claimed == 3


@override_settings(RELIABLE_OUTBOX={"BACKOFF_BASE_SECONDS": 60, "BACKOFF_MAX_SECONDS": 60})
def test_a_head_waiting_to_retry_holds_back_its_key_but_not_others() -> None:
    publish("test.record", {"seq": 0, "fail_attempts": 1}, key="k")
    publish("test.record", {"seq": 1}, key="k")
    publish("test.record", {"seq": 0}, key="other")
    publish("test.record", {"seq": 0})

    worker = Worker()
    while worker.run_once():
        pass
    # k#0 failed and waits up to a minute; k#1 must wait with it.
    assert sorted(Handled.objects.values_list("key", flat=True)) == ["", "other"]


def test_dead_head_blocks_its_key_until_requeued() -> None:
    publish("test.record", {"seq": 0, "fail_attempts": 1}, key="k", max_attempts=1)
    publish("test.record", {"seq": 1}, key="k")
    publish("test.record", {"seq": 0}, key="free")
    drain()

    head = OutboxMessage.objects.order_by("id").first()
    assert head is not None
    assert head.status == Status.DEAD
    assert handled_order() == {"free": [0]}

    # The operator fixes the cause, then requeues; the key drains in order.
    OutboxMessage.objects.filter(pk=head.pk).update(payload={"seq": 0})
    requeue([head.pk])
    drain()
    assert handled_order() == {"free": [0], "k": [0, 1]}


@override_settings(RELIABLE_OUTBOX={"BLOCK_KEY_ON_DEAD_LETTER": False})
def test_dead_head_can_be_skipped_when_configured() -> None:
    publish("test.record", {"seq": 0, "fail_attempts": 1}, key="k", max_attempts=1)
    publish("test.record", {"seq": 1}, key="k")
    drain()
    assert handled_order() == {"k": [1]}
    assert statuses() == {"dead": 1, "delivered": 1}
