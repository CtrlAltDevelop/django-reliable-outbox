"""How an idle worker waits for work: LISTEN/NOTIFY, with polling underneath.

NOTIFY is a latency optimisation, never a correctness mechanism. Notifications
are not queued for listeners that aren't connected, and messages also become
due without any INSERT (a retry's backoff ends, a lease expires). So the
worker always wakes at least every ``POLL_INTERVAL`` and a lost notification
costs at most one interval.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Protocol

from django.db import connections

if TYPE_CHECKING:
    import psycopg

logger = logging.getLogger(__name__)

CHANNEL = "reliable_outbox"

# The listener checks the stop flag this often, so shutdown stays prompt.
_SLICE = 0.25


class Waiter(Protocol):
    def wait(self, timeout: float) -> None: ...
    def close(self) -> None: ...


class PollingWaiter:
    def __init__(self, stop_event: threading.Event) -> None:
        self._stop = stop_event

    def wait(self, timeout: float) -> None:
        self._stop.wait(timeout)

    def close(self) -> None:
        pass


class NotifyWaiter:
    """Sleeps on a dedicated LISTEN connection until a NOTIFY, a timeout, or stop.

    The connection is separate from Django's because it must stay in autocommit
    and idle between waits; it is opened lazily and re-opened after an error.
    """

    def __init__(self, alias: str, stop_event: threading.Event) -> None:
        self._alias = alias
        self._stop = stop_event
        self._conn: psycopg.Connection[tuple[object, ...]] | None = None

    def _connect(self) -> psycopg.Connection[tuple[object, ...]]:
        import psycopg  # noqa: PLC0415 - optional dependency

        params = connections[self._alias].get_connection_params()
        # Django-specific adapters; a bare LISTEN connection needs neither.
        params.pop("cursor_factory", None)
        params.pop("context", None)
        conn = psycopg.connect(**params, autocommit=True)
        conn.execute(f"LISTEN {CHANNEL}")
        return conn

    def wait(self, timeout: float) -> None:
        import psycopg  # noqa: PLC0415

        deadline = time.monotonic() + timeout
        try:
            if self._conn is None:
                self._conn = self._connect()
            while not self._stop.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                # list() runs the generator to completion, releasing its lock.
                woken = list(self._conn.notifies(timeout=min(remaining, _SLICE), stop_after=1))
                if woken:
                    # One wake-up is enough: drop whatever else queued up while
                    # we were busy, or each stale notification costs a claim.
                    # (Only after the first generator is closed: it holds the
                    # connection lock, and a nested notifies() would deadlock.)
                    for _ in self._conn.notifies(timeout=0):
                        pass
                    return
        except psycopg.Error:
            logger.warning("LISTEN connection failed; polling until it recovers", exc_info=True)
            self.close()
            self._stop.wait(max(deadline - time.monotonic(), 0))

    def close(self) -> None:
        if self._conn is not None:
            conn, self._conn = self._conn, None
            try:
                conn.close()
            except Exception:  # closing a broken connection may itself fail
                logger.debug("Error closing LISTEN connection", exc_info=True)


def make_waiter(alias: str, stop_event: threading.Event, *, notify: bool) -> Waiter:
    if notify:
        if connections[alias].vendor != "postgresql":
            logger.warning("NOTIFY needs PostgreSQL; falling back to polling")
        else:
            try:
                import psycopg  # noqa: F401, PLC0415
            except ImportError:
                logger.warning("NOTIFY needs psycopg 3; falling back to polling")
            else:
                return NotifyWaiter(alias, stop_event)
    return PollingWaiter(stop_event)


def notify(alias: str) -> None:
    """Wake idle workers for rows that became due without an INSERT (e.g. a requeue)."""
    with connections[alias].cursor() as cursor:
        cursor.execute("SELECT pg_notify(%s, '')", [CHANNEL])
