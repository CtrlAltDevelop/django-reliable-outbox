import time
from typing import Any

import pytest

from reliable_outbox import PermanentError
from reliable_outbox.exceptions import JobTimeout
from reliable_outbox.jobs import Job
from reliable_outbox.models import OutboxMessage, Status
from tests.helpers import drain
from tests.testapp.jobs import never_key, picky, slow


def test_retry_on_limits_retries_to_the_listed_exceptions() -> None:
    assert not picky.is_permanent(ConnectionError("down"))
    assert not picky.is_permanent(ConnectionResetError("subclass"))
    assert picky.is_permanent(ValueError("bad input"))
    assert picky.is_permanent(PermanentError("always"))


def test_a_timeout_is_retried_even_when_retry_on_does_not_list_it() -> None:
    assert not picky.is_permanent(JobTimeout("slow"))


def test_dont_retry_on_wins_and_the_rest_retry() -> None:
    assert never_key.is_permanent(KeyError("gone"))
    assert not never_key.is_permanent(ValueError("transient"))


def test_exception_lists_must_be_exception_classes() -> None:
    with pytest.raises(ValueError, match="retry_on"):
        Job(lambda: None, name="x", retries=1, backoff="fixed", retry_on=("nope",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout"):
        Job(lambda: None, name="x", retries=1, backoff="fixed", timeout=0)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    ("job_under_test", "error", "attempts", "status"),
    [
        (picky, "value", 1, Status.DEAD),  # not in retry_on: dead at once
        (picky, "connection", 4, Status.DEAD),  # retried until exhausted
        (never_key, "key", 1, Status.DEAD),
        (never_key, "value", 4, Status.DEAD),
    ],
)
def test_worker_applies_the_policy(
    job_under_test: Job[..., Any], error: str, attempts: int, status: str
) -> None:
    job_under_test.enqueue(error)
    drain()
    message = OutboxMessage.objects.get()
    assert (message.status, message.attempts) == (status, attempts)


@pytest.mark.django_db(transaction=True)
def test_a_job_past_its_timeout_fails_with_job_timeout() -> None:
    slow.enqueue(5)
    started = time.monotonic()
    drain()
    assert time.monotonic() - started < 3
    message = OutboxMessage.objects.get()
    assert message.status == Status.DEAD
    assert message.last_error.startswith("JobTimeout:")
    assert issubclass(JobTimeout, TimeoutError)


@pytest.mark.django_db(transaction=True)
def test_a_job_inside_its_timeout_is_delivered() -> None:
    slow.enqueue(0.01)
    drain()
    assert OutboxMessage.objects.get().status == Status.DELIVERED
