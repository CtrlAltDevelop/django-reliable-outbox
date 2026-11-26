"""Background jobs: functions whose calls are recorded in the outbox.

A job is just a message whose payload is the call's arguments. That gives jobs
the same guarantee as events for free: ``enqueue`` inside a transaction that
rolls back leaves nothing behind, so a job can never run against data that was
never committed.
"""

from __future__ import annotations

import functools
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, overload

from .conf import BACKOFF_STRATEGIES, Backoff, OnConflict, check_priority
from .exceptions import JobTimeout, PermanentError


class UnknownJob(LookupError):
    """The worker found a job name nothing has registered, usually a deploy in progress."""


_registry: dict[str, Job[..., Any]] = {}


class Job[**P, R]:
    """A registered job. Calling it runs the function inline; ``enqueue`` defers it."""

    def __init__(
        self,
        fn: Callable[P, R],
        *,
        name: str,
        retries: int,
        backoff: Backoff,
        timeout: float | None = None,
        retry_on: tuple[type[BaseException], ...] = (),
        dont_retry_on: tuple[type[BaseException], ...] = (),
        priority: int = 0,
    ) -> None:
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise ValueError(f"retries must be a non-negative integer, got {retries!r}.")
        if backoff not in BACKOFF_STRATEGIES:
            raise ValueError(f"backoff must be one of {BACKOFF_STRATEGIES}, got {backoff!r}.")
        if timeout is not None and (
            isinstance(timeout, bool) or not isinstance(timeout, int | float) or timeout <= 0
        ):
            raise ValueError(f"timeout must be a positive number of seconds, got {timeout!r}.")
        self.fn = fn
        self.name = name
        self.retries = retries
        self.backoff: Backoff = backoff
        self.timeout = timeout
        self.retry_on = _exception_types("retry_on", retry_on)
        self.dont_retry_on = _exception_types("dont_retry_on", dont_retry_on)
        self.priority = check_priority(priority)
        functools.update_wrapper(self, fn)

    @property
    def max_attempts(self) -> int:
        return self.retries + 1

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)

    def is_permanent(self, exc: BaseException) -> bool:
        """True when retrying ``exc`` cannot help, so the message goes straight to dead.

        ``PermanentError`` and ``dont_retry_on`` always win; ``JobTimeout`` is
        always retried; otherwise, if ``retry_on`` is set, anything outside it
        is permanent too.
        """
        if isinstance(exc, (PermanentError, *self.dont_retry_on)):
            return True
        if isinstance(exc, JobTimeout):
            return False  # a timeout is an outage, whatever retry_on lists
        return bool(self.retry_on) and not isinstance(exc, self.retry_on)

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
        priority: int | None = None,
        unique_key: str | None = None,
        on_conflict: OnConflict = "skip",
        using: str | None = None,
    ) -> uuid.UUID:
        """``enqueue`` with scheduling and ordering options that can't share its signature.

        ``unique_key`` allows one pending copy of this job per key. A second
        call while one is pending (or running) returns the existing message id
        and does nothing; with ``on_conflict="replace"`` it instead swaps in the
        new arguments and schedule, unless the first is already running.
        """
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
            priority=self.priority if priority is None else priority,
            unique_key=unique_key,
            on_conflict=on_conflict,
            using=using,
        )

    def run(self, payload: Mapping[str, Any]) -> R:
        """Invoke the function with the arguments stored by ``enqueue``."""
        return self.fn(*payload.get("args", ()), **payload.get("kwargs", {}))


def _exception_types(
    name: str, value: type[BaseException] | Iterable[type[BaseException]]
) -> tuple[type[BaseException], ...]:
    types = (value,) if isinstance(value, type) else tuple(value)
    if not all(isinstance(t, type) and issubclass(t, BaseException) for t in types):
        raise ValueError(f"{name} must be exception classes, got {value!r}.")
    return types


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
    *,
    name: str | None = None,
    retries: int = 3,
    backoff: Backoff = "exponential",
    timeout: float | None = None,
    retry_on: tuple[type[BaseException], ...] = (),
    dont_retry_on: tuple[type[BaseException], ...] = (),
    priority: int = 0,
) -> Callable[[Callable[P, R]], Job[P, R]]: ...


def job[**P, R](
    fn: Callable[P, R] | None = None,
    /,
    *,
    name: str | None = None,
    retries: int = 3,
    backoff: Backoff = "exponential",
    timeout: float | None = None,
    retry_on: tuple[type[BaseException], ...] = (),
    dont_retry_on: tuple[type[BaseException], ...] = (),
    priority: int = 0,
) -> Job[P, R] | Callable[[Callable[P, R]], Job[P, R]]:
    """Register a function as a background job.

    ``retries`` counts retries after the first attempt, so ``retries=5`` means
    up to six runs before the message is dead-lettered. The registered name
    defaults to ``module.qualname``; set ``name`` explicitly if you might move
    the function while messages for it are still queued.

    ``timeout`` (seconds) raises ``JobTimeout`` in a run that overstays; it is
    enforced on a worker's main thread only. ``retry_on`` limits retries to the
    listed exceptions and ``dont_retry_on`` dead-letters on them at once, like
    ``PermanentError``. ``priority`` (higher runs first) is the default for
    this job's messages.
    """

    def decorate(func: Callable[P, R]) -> Job[P, R]:
        job_name = name or f"{func.__module__}.{func.__qualname__}"
        return _register(
            Job(
                func,
                name=job_name,
                retries=retries,
                backoff=backoff,
                timeout=timeout,
                retry_on=retry_on,
                dont_retry_on=dont_retry_on,
                priority=priority,
            )
        )

    return decorate(fn) if fn is not None else decorate


def get_job(name: str) -> Job[..., Any]:
    try:
        return _registry[name]
    except KeyError:
        raise UnknownJob(f"No job registered under {name!r}.") from None
