"""Metrics hooks. Connect a receiver to feed Prometheus, StatsD, OpenTelemetry...

All three are sent by the worker after the outcome is committed, with
``sender`` the ``Worker``. Receivers run on the worker's thread, so keep them
cheap; an exception in one is logged and does not affect the message.

* ``message_delivered``: ``envelope``, ``duration`` (seconds spent running it),
  ``queued_for`` (seconds from the row's creation to the start of this run).
* ``message_failed``: ``envelope``, ``error``, ``retry_in`` (seconds).
* ``message_dead_lettered``: ``envelope``, ``error``.
"""

from django.dispatch import Signal

message_delivered = Signal()
message_failed = Signal()
message_dead_lettered = Signal()
