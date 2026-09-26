"""Background jobs: functions whose calls are recorded in the outbox.

A job is just a message whose payload is the call's arguments. That gives jobs
the same guarantee as events for free: ``enqueue`` inside a transaction that
rolls back leaves nothing behind, so a job can never run against data that was
never committed.
"""

from __future__ import annotations

import functools
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, overload

from .conf import BACKOFF_STRATEGIES, Backoff


class UnknownJob(LookupError):
    """The worker found a job name nothing has registered, usually a deploy in progress."""


_registry: dict[str, Job[..., Any]] = {}


class Job[**P, R]:
    """A registered job. Calling it runs the function inline; ``enqueue`` defers it."""

    def __init__(self, fn: Callable[P, R], *, name: str, retries: int, backoff: Backoff) -> None:
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise ValueError(f"retries must be a non-negative integer, got {retries!r}.")
        if backoff not in BACKOFF_STRATEGIES:
            raise ValueError(f"backoff must be one of {BACKOFF_STRATEGIES}, got {backoff!r}.")
        self.fn = fn
        self.name = name
        self.retries = retries
        self.backoff: Backoff = backoff
        functools.update_wrapper(self, fn)

    @property
    def max_attempts(self) -> int:
        return self.retries + 1

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<Job {self.name} retries={self.retries} backoff={self.backoff}>"

    def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> uuid.UUID:
        """Run later, once the surrounding transaction commits. Arguments must be JSON-able."""
        return self.enqueue_with(args=args, kwargs=kwargs)

    def enqueue_with(
        self,
        *,
        args: Sequence[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        key: str | None = None,
        run_at: datetime | None = None,
        delay: timedelta | None = None,
        using: str | None = None,
    ) -> uuid.UUID:
        """``enqueue`` with scheduling and ordering options that can't share its signature."""
        from .models import Kind  # noqa: PLC0415 - models load after app registry
        from .publishing import write_message  # noqa: PLC0415

        return write_message(
            kind=Kind.JOB,
            name=self.name,
            payload={"args": list(args), "kwargs": dict(kwargs or {})},
            key=key,
            destination="",
            headers=None,
            run_at=run_at,
            delay=delay,
            max_attempts=self.max_attempts,
            backoff=self.backoff,
            using=using,
        )

    def run(self, payload: Mapping[str, Any]) -> R:
        """Invoke the function with the arguments stored by ``enqueue``."""
        return self.fn(*payload.get("args", ()), **payload.get("kwargs", {}))


def _register[**P, R](entry: Job[P, R]) -> Job[P, R]:
    existing = _registry.get(entry.name)
    # Importing the same module twice re-registers the same function, which is
    # harmless. Two different functions claiming one name is a bug.
    if existing is not None and (
        (existing.fn.__module__, existing.fn.__qualname__)
        != (entry.fn.__module__, entry.fn.__qualname__)
    ):
        raise ValueError(
            f"Job name {entry.name!r} is already taken by "
            f"{existing.fn.__module__}.{existing.fn.__qualname__}."
        )
    _registry[entry.name] = entry
    return entry


@overload
def job[**P, R](fn: Callable[P, R], /) -> Job[P, R]: ...


@overload
def job[**P, R](
    *, name: str | None = None, retries: int = 3, backoff: Backoff = "exponential"
) -> Callable[[Callable[P, R]], Job[P, R]]: ...


def job[**P, R](
    fn: Callable[P, R] | None = None,
    /,
    *,
    name: str | None = None,
    retries: int = 3,
    backoff: Backoff = "exponential",
) -> Job[P, R] | Callable[[Callable[P, R]], Job[P, R]]:
    """Register a function as a background job.

    ``retries`` counts retries after the first attempt, so ``retries=5`` means
    up to six runs before the message is dead-lettered. The registered name
    defaults to ``module.qualname``; set ``name`` explicitly if you might move
    the function while messages for it are still queued.
    """

    def decorate(func: Callable[P, R]) -> Job[P, R]:
        job_name = name or f"{func.__module__}.{func.__qualname__}"
        return _register(Job(func, name=job_name, retries=retries, backoff=backoff))

    return decorate(fn) if fn is not None else decorate


def get_job(name: str) -> Job[..., Any]:
    try:
        return _registry[name]
    except KeyError:
        raise UnknownJob(f"No job registered under {name!r}.") from None
