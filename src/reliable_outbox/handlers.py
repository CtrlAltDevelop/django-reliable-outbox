"""Registry of in-process event handlers, used by the ``LocalHandlers`` destination."""

from __future__ import annotations

from collections.abc import Callable
from fnmatch import fnmatchcase

from .envelope import Envelope

Handler = Callable[[Envelope], object]

_exact: dict[str, list[Handler]] = {}
_patterns: list[tuple[str, Handler]] = []

_WILDCARDS = frozenset("*?[")


def handler[H: Handler](event_type: str) -> Callable[[H], H]:
    """Register ``fn(envelope)`` for an event type, or an ``fnmatch`` pattern like ``"order.*"``.

    Several handlers may share an event; they run in registration order inside
    one transaction, and if any of them raises, the message is retried as a whole.
    """

    def register(fn: H) -> H:
        if _WILDCARDS.isdisjoint(event_type):
            bucket = _exact.setdefault(event_type, [])
            if fn not in bucket:
                bucket.append(fn)
        elif (event_type, fn) not in _patterns:
            _patterns.append((event_type, fn))
        return fn

    return register


def handlers_for(event_type: str) -> list[Handler]:
    found = list(_exact.get(event_type, ()))
    found.extend(fn for pattern, fn in _patterns if fnmatchcase(event_type, pattern))
    return found
