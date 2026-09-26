from datetime import timedelta

import pytest
from django.db import transaction

from reliable_outbox import job
from reliable_outbox.jobs import Job, UnknownJob, get_job
from reliable_outbox.models import Kind, OutboxMessage
from tests.testapp.jobs import flaky, record
from tests.testapp.models import Handled


class Boom(Exception):
    pass


def test_autodiscovery_registered_the_test_jobs() -> None:
    assert get_job("tests.testapp.jobs.record") is record
    assert get_job("tests.flaky") is flaky
    with pytest.raises(UnknownJob):
        get_job("nope")


def test_retry_policy_is_kept_on_the_job() -> None:
    assert record.max_attempts == 4
    assert (flaky.retries, flaky.max_attempts, flaky.backoff) == (2, 3, "fixed")
    assert repr(flaky) == "<Job tests.flaky retries=2 backoff=fixed>"


def test_decorator_keeps_the_function_metadata() -> None:
    assert record.__name__ == "record"  # type: ignore[attr-defined]
    assert record.__wrapped__ is record.fn  # type: ignore[attr-defined]


@pytest.mark.django_db
def test_calling_a_job_runs_it_inline() -> None:
    record(5, label="inline")
    assert Handled.objects.get().seq == 5
    assert not OutboxMessage.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_enqueue_in_a_rolled_back_transaction_leaves_nothing() -> None:
    with pytest.raises(Boom), transaction.atomic():
        record.enqueue(1)
        raise Boom
    assert not OutboxMessage.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_enqueue_stores_the_call() -> None:
    with transaction.atomic():
        record.enqueue(3, label="x")

    message = OutboxMessage.objects.get()
    assert message.kind == Kind.JOB
    assert message.name == "tests.testapp.jobs.record"
    assert message.payload == {"args": [3], "kwargs": {"label": "x"}}
    assert message.max_attempts == 4
    assert message.backoff == "exponential"
    assert get_job(message.name).run(message.payload) is None
    assert Handled.objects.get().key == "x"


@pytest.mark.django_db(transaction=True)
def test_enqueue_with_options() -> None:
    record.enqueue_with(args=[1], key="user-9", delay=timedelta(minutes=5))
    message = OutboxMessage.objects.get()
    assert message.key == "user-9"
    assert message.run_at - message.created_at == timedelta(minutes=5)


@pytest.mark.django_db(transaction=True)
def test_arguments_must_be_json_serialisable() -> None:
    with pytest.raises(TypeError):
        record.enqueue(object())  # type: ignore[arg-type]


def test_invalid_policies_are_rejected() -> None:
    with pytest.raises(ValueError, match="retries"):
        job(retries=-1)(lambda: None)
    with pytest.raises(ValueError, match="backoff"):
        job(backoff="linear")(lambda: None)  # type: ignore[call-overload]


def test_one_name_cannot_belong_to_two_functions() -> None:
    def first() -> None: ...

    def second() -> None: ...

    registered = job(name="tests.clash")(first)
    assert isinstance(registered, Job)
    assert job(name="tests.clash")(first).fn is first  # re-import is fine
    with pytest.raises(ValueError, match="already taken"):
        job(name="tests.clash")(second)
