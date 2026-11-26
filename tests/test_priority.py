import pytest
from django.db import transaction

from reliable_outbox import publish
from reliable_outbox.worker import Worker
from tests.testapp.jobs import noted
from tests.testapp.models import Handled

pytestmark = pytest.mark.django_db(transaction=True)


def run_one_at_a_time(count: int) -> list[int | None]:
    worker = Worker(batch_size=1)
    for _ in range(count):
        assert worker.run_once() == 1
    return list(Handled.objects.order_by("id").values_list("seq", flat=True))


def test_higher_priority_is_claimed_first_then_oldest() -> None:
    with transaction.atomic():
        publish("test.record", {"seq": 1})
        publish("test.record", {"seq": 2}, priority=10)
        publish("test.record", {"seq": 3}, priority=10)
        publish("test.record", {"seq": 4}, priority=-1)
    assert run_one_at_a_time(4) == [2, 3, 1, 4]


def test_priority_never_overtakes_an_older_message_of_the_same_key() -> None:
    with transaction.atomic():
        publish("test.record", {"seq": 1}, key="k")
        publish("test.record", {"seq": 2}, key="k", priority=100)
        publish("test.record", {"seq": 3})
    # 2 outranks everything but is held behind 1, the older message of its key.
    assert run_one_at_a_time(3) == [1, 2, 3]


def test_a_jobs_default_priority_and_an_override() -> None:
    with transaction.atomic():
        noted.enqueue(1)  # the job's own priority is 5
        noted.enqueue_with(args=[2], priority=9)
        noted.enqueue_with(args=[3], priority=1)
    assert run_one_at_a_time(3) == [2, 1, 3]


def test_priority_is_validated() -> None:
    for bad in (True, 1.5, 2**15):
        with pytest.raises(ValueError, match="priority"):
            publish("test.record", {}, priority=bad)  # type: ignore[arg-type]


def test_priority_order_holds_inside_one_batch() -> None:
    with transaction.atomic():
        publish("test.record", {"seq": 1})
        publish("test.record", {"seq": 2})
        publish("test.record", {"seq": 3}, priority=10)
    Worker(batch_size=3).run_once()
    assert list(Handled.objects.order_by("id").values_list("seq", flat=True)) == [3, 1, 2]
