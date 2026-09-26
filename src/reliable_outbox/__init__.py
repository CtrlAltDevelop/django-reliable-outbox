"""Transactional outbox and reliable background jobs for Django on PostgreSQL."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

# Django imports this package while it is still loading apps, before models
# may be touched, so the public names are resolved on first use instead.
_EXPORTS = {
    "Envelope": "reliable_outbox.envelope",
    "PermanentError": "reliable_outbox.exceptions",
    "handler": "reliable_outbox.handlers",
    "job": "reliable_outbox.jobs",
    "publish": "reliable_outbox.publishing",
}

__all__ = ["Envelope", "PermanentError", "__version__", "handler", "job", "publish"]

if TYPE_CHECKING:
    from .envelope import Envelope
    from .exceptions import PermanentError
    from .handlers import handler
    from .jobs import job
    from .publishing import publish


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return getattr(import_module(_EXPORTS[name]), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
