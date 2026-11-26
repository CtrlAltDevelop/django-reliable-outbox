"""Wall-clock limit for a job run, via ``SIGALRM``."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from types import FrameType

from .exceptions import JobTimeout

logger = logging.getLogger(__name__)


@cache
def _warn_unenforced() -> None:
    logger.warning(
        "Job timeouts are only enforced on a worker's main thread (--concurrency 1); "
        "ignoring them here."
    )


@contextmanager
def deadline(seconds: float | None, *, label: str) -> Iterator[None]:
    """Raise ``JobTimeout`` in the running code after ``seconds``.

    # ponytail: signals only fire on the main thread, so with --concurrency > 1
    # (worker threads) and on Windows the limit is skipped with a warning. Run
    # timed jobs on a --concurrency 1 worker, or add a subprocess runner.
    # The query in flight is not cancelled, only abandoned (its connection is
    # dropped); keep `timeout` below LEASE_SECONDS.
    """
    if seconds is None:
        yield
        return
    if threading.current_thread() is not threading.main_thread() or not hasattr(
        signal, "setitimer"
    ):
        _warn_unenforced()
        yield
        return

    def expire(signum: int, frame: FrameType | None) -> None:
        raise JobTimeout(f"{label} exceeded its {seconds:g}s timeout.")

    previous = signal.signal(signal.SIGALRM, expire)
    outer_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        try:
            # One call both cancels ours and re-arms a timer the host had running.
            signal.setitimer(signal.ITIMER_REAL, *outer_timer)
        finally:
            # An alarm landing in the call above must not leave `expire` installed.
            signal.signal(signal.SIGALRM, signal.SIG_DFL if previous is None else previous)
