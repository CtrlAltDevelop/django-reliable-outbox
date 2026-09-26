"""Handlers the test suite drives through the payload.

Payload switches: ``fail_attempts`` (raise on attempts up to N, after writing a
row that the rollback must discard), ``crash_on_attempt`` (kill the process
outright, like a SIGKILL or an OOM), ``sleep`` (seconds).
"""

import os
import threading
import time
from collections import Counter

from reliable_outbox import Envelope, handler

from .models import Handled

invocations: Counter[str] = Counter()
_lock = threading.Lock()


@handler("test.record")
def record(envelope: Envelope) -> None:
    with _lock:
        invocations[str(envelope.message_id)] += 1
    options = envelope.payload if isinstance(envelope.payload, dict) else {}
    if options.get("crash_on_attempt") == envelope.attempt:
        os._exit(3)
    if seconds := options.get("sleep"):
        time.sleep(seconds)
    Handled.objects.create(
        message_id=envelope.message_id,
        name=envelope.name,
        key=envelope.key or "",
        seq=options.get("seq"),
        attempt=envelope.attempt,
    )
    if envelope.attempt <= options.get("fail_attempts", 0):
        raise RuntimeError(f"planned failure on attempt {envelope.attempt}")


@handler("audit.*")
def audit(envelope: Envelope) -> None:
    Handled.objects.create(message_id=envelope.message_id, name=f"audit:{envelope.name}")
