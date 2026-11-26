"""One queue snapshot per scrape, shared by the exporters."""

from __future__ import annotations

import logging
import threading
import time

from django.db import Error, close_old_connections

from reliable_outbox.operations import OutboxStats, stats

logger = logging.getLogger(__name__)

_TTL_SECONDS = 1.0
_lock = threading.Lock()
_cache: dict[str | None, tuple[float, OutboxStats]] = {}


def queue_stats(using: str | None) -> OutboxStats | None:
    """``stats()`` reused for a second (one collection reads many gauges); None if DB is down."""
    with _lock:
        cached = _cache.get(using)
        if cached is not None and time.monotonic() - cached[0] < _TTL_SECONDS:
            return cached[1]
        try:
            snapshot = stats(using=using)
        except Error:
            logger.exception("Could not read outbox queue stats for metrics")
            return None
        finally:
            close_old_connections()  # a scrape thread must not pin a connection
        _cache[using] = (time.monotonic(), snapshot)
        return snapshot
