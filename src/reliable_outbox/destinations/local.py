from __future__ import annotations

from reliable_outbox.envelope import Envelope
from reliable_outbox.handlers import handlers_for


class NoHandler(LookupError):
    """No handler is registered for the event. Retried, since a deploy may be adding one."""


class LocalHandlers:
    """Deliver to functions registered with ``@handler`` in this codebase.

    Handlers run inside the same transaction that marks the message delivered,
    so their own database writes and the acknowledgement commit or roll back
    together.
    """

    def deliver(self, envelope: Envelope) -> None:
        found = handlers_for(envelope.name)
        if not found:
            raise NoHandler(f"No handler registered for {envelope.name!r}.")
        for fn in found:
            fn(envelope)
