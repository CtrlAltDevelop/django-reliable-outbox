from __future__ import annotations

from typing import Protocol, runtime_checkable

from reliable_outbox.envelope import Envelope


@runtime_checkable
class Destination(Protocol):
    """Where a published event goes once its transaction has committed.

    ``deliver`` must return only once the message is safely handed over (acked
    by the broker, 2xx from the webhook, handlers finished) and raise otherwise.
    The worker marks the message delivered after ``deliver`` returns, so a crash
    in between means one more delivery, never a lost one.

    Implementations are built once per worker process from the ``OPTIONS`` of
    their ``DESTINATIONS`` entry and are shared between worker threads.
    """

    def deliver(self, envelope: Envelope) -> None: ...
