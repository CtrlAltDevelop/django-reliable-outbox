from reliable_outbox.jobs import job

from .models import Handled


@job
def record(value: int, *, label: str = "") -> None:
    Handled.objects.create(name="record", key=label, seq=value)


@job(retries=2, backoff="fixed", name="tests.flaky")
def flaky(value: int) -> None:
    raise RuntimeError(f"flaky job failed for {value}")
