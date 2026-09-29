"""Graceful shutdown: finish the message in hand, claim nothing new.

The first SIGTERM/SIGINT (or SIGBREAK, Ctrl+Break on Windows) sets the stop
event. Workers notice it between messages, hand back the unstarted rest of
their batch, and exit. A second signal means "now": the default handler is
restored, so Ctrl+C twice raises KeyboardInterrupt and the leases of anything
still in flight simply expire, which is exactly how a crash is recovered.
"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable
from types import FrameType

logger = logging.getLogger(__name__)

_Handler = Callable[[int, FrameType | None], object] | int | None


def shutdown_signals() -> list[signal.Signals]:
    """The signals that mean "please stop" on this platform."""
    names = ("SIGTERM", "SIGINT", "SIGBREAK")
    return [getattr(signal, name) for name in names if hasattr(signal, name)]


def install_signal_handlers(stop_event: threading.Event) -> Callable[[], None]:
    """Route shutdown signals to ``stop_event``. Returns a function that undoes it.

    Only the main thread may install handlers; anywhere else this is a no-op
    and the caller is expected to set the event itself.
    """
    if threading.current_thread() is not threading.main_thread():
        logger.debug("Not in the main thread; signal handlers not installed")
        return lambda: None

    previous: dict[signal.Signals, _Handler] = {}

    def restore() -> None:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    def on_signal(signum: int, frame: FrameType | None) -> None:
        name = signal.Signals(signum).name
        if stop_event.is_set():
            logger.warning("Second %s: stopping immediately", name)
            restore()
            raise KeyboardInterrupt
        logger.warning("%s received: finishing current messages, then stopping", name)
        stop_event.set()

    for signum in shutdown_signals():
        previous[signum] = signal.signal(signum, on_signal)
    return restore
