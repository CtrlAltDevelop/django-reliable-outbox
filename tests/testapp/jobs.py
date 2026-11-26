import time

from reliable_outbox.jobs import job

from .models import Handled


@job
def record(value: int, *, label: str = "") -> None:
    Handled.objects.create(name="record", key=label, seq=value)


@job(retries=2, backoff="fixed", name="tests.flaky")
def flaky(value: int) -> None:
    raise RuntimeError(f"flaky job failed for {value}")


@job(retries=0, timeout=0.2, name="tests.slow")
def slow(seconds: float) -> None:
    time.sleep(seconds)


@job(retries=3, backoff="fixed", retry_on=(ConnectionError,), name="tests.picky")
def picky(error: str) -> None:
    raise {"connection": ConnectionError, "value": ValueError}[error](error)


@job(retries=3, backoff="fixed", dont_retry_on=(KeyError,), name="tests.never_key")
def never_key(error: str) -> None:
    raise {"key": KeyError, "value": ValueError}[error](error)


@job(name="tests.noted", priority=5)
def noted(value: int) -> None:
    Handled.objects.create(name="noted", seq=value)
