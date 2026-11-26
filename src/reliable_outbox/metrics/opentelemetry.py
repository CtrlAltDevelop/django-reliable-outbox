"""OpenTelemetry metrics: the same instruments as the Prometheus exporter, via the Metrics API.

Configure a ``MeterProvider`` with your exporter as usual, then call
``install()`` once per process. The queue gauges are observed from the
database at each collection.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation

from reliable_outbox.metrics._snapshot import queue_stats
from reliable_outbox.signals import message_dead_lettered, message_delivered, message_failed

_UID = "reliable_outbox.metrics.opentelemetry"
# Keeps each provider alive, so a freed provider's id can't be reused and mistaken for installed.
_installed: dict[int, MeterProvider | None] = {}
_GAUGES = ("ready", "in_flight", "scheduled", "retrying", "dead", "delivered")


def install(
    meter_provider: MeterProvider | None = None, *, prefix: str = "outbox", using: str | None = None
) -> None:
    """Create the instruments and connect the signal receivers. Safe to call twice."""
    meter = (
        meter_provider.get_meter("reliable_outbox")
        if meter_provider is not None
        else metrics.get_meter("reliable_outbox")
    )
    key = id(meter_provider)
    if key in _installed:
        return
    delivered = meter.create_counter(
        f"{prefix}.messages.delivered", description="Messages delivered."
    )
    failed = meter.create_counter(
        f"{prefix}.messages.failed", description="Failed attempts to retry."
    )
    dead = meter.create_counter(
        f"{prefix}.messages.dead_lettered", description="Messages dead-lettered."
    )
    run_time = meter.create_histogram(
        f"{prefix}.run.duration", unit="s", description="Time spent running a message."
    )
    queued_time = meter.create_histogram(
        f"{prefix}.queued.duration", unit="s", description="Creation to start of the run."
    )

    def attributes(envelope: Any) -> dict[str, str]:
        return {"kind": envelope.kind, "name": envelope.name}

    def on_delivered(*, envelope: Any, duration: float, queued_for: float, **_: Any) -> None:
        delivered.add(1, attributes(envelope))
        run_time.record(duration, attributes(envelope))
        queued_time.record(queued_for, attributes(envelope))

    def on_failed(*, envelope: Any, **_: Any) -> None:
        failed.add(1, attributes(envelope))

    def on_dead(*, envelope: Any, **_: Any) -> None:
        dead.add(1, attributes(envelope))

    message_delivered.connect(on_delivered, weak=False, dispatch_uid=f"{_UID}.delivered.{key}")
    message_failed.connect(on_failed, weak=False, dispatch_uid=f"{_UID}.failed.{key}")
    message_dead_lettered.connect(on_dead, weak=False, dispatch_uid=f"{_UID}.dead.{key}")

    def observe(name: str) -> Any:
        def callback(options: CallbackOptions) -> Iterable[Observation]:
            snapshot = queue_stats(using)
            return [] if snapshot is None else [Observation(getattr(snapshot, name))]

        return callback

    for name in _GAUGES:
        meter.create_observable_gauge(f"{prefix}.queue.{name}", callbacks=[observe(name)])

    def observe_lag(options: CallbackOptions) -> Iterable[Observation]:
        snapshot = queue_stats(using)
        lag = None if snapshot is None else snapshot.oldest_ready_seconds
        return [] if lag is None else [Observation(lag)]

    meter.create_observable_gauge(
        f"{prefix}.queue.oldest_ready",
        unit="s",
        callbacks=[observe_lag],
        description="Age of the oldest due message: the queue's lag.",
    )
    _installed[key] = meter_provider
