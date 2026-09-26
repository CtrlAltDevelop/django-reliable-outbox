from __future__ import annotations

from typing import Any

from django.core.checks import CheckMessage, Error, register
from django.db import connections

from .conf import get_settings


@register()
def check_postgresql(**kwargs: Any) -> list[CheckMessage]:
    """The worker is built on SKIP LOCKED and LISTEN/NOTIFY; nothing else will do."""
    alias = get_settings().DATABASE
    vendor = connections[alias].vendor
    if vendor == "postgresql":
        return []
    return [
        Error(
            f"django-reliable-outbox needs PostgreSQL, but database {alias!r} is {vendor}.",
            hint="Point RELIABLE_OUTBOX['DATABASE'] at a PostgreSQL alias.",
            id="reliable_outbox.E001",
        )
    ]
