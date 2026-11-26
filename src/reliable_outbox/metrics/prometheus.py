"""Prometheus metrics: counters and histograms from the worker, queue gauges from the database.

Call ``install()`` once in each process that should report. Counters and
histograms fill in the process that runs the worker (expose them there, e.g.
with ``prometheus_client.start_http_server``). The queue gauges are read from
the database at scrape time, so any process that serves a scrape can export them.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from reliable_outbox.metrics._snapshot import queue_stats
from reliable_outbox.signals import message_dead_lettered, message_delivered, message_failed

_LABELS = ("kind", "name")
_UID = "reliable_outbox.metrics.prometheus"
# Keeps each registry alive, so a freed registry's id can't be reused and mistaken for installed.
_installed: dict[int, CollectorRegistry] = {}


class _QueueCollector(Collector):
    def __init__(self, prefix: str, using: str | None) -> None:
        self._prefix = prefix
        self._using = using

    def collect(self) -> Iterator[Metric]:
        snapshot = queue_stats(self._using)
        if snapshot is None:
            return  # database down: still serve the counters
        gauges: dict[str, tuple[str, float]] = {
            "ready": ("Messages due now and waiting for a worker.", snapshot.ready),
            "in_flight": ("Messages leased to a worker.", snapshot.in_flight),
            "scheduled": ("Pending messages not due yet.", snapshot.scheduled),
            "retrying": ("Pending messages that have failed at least once.", snapshot.retrying),
            "dead": ("Dead-lettered messages.", snapshot.dead),
            "delivered": ("Delivered messages not yet cleaned up.", snapshot.delivered),
        }
        if snapshot.oldest_ready_seconds is not None:
            gauges["oldest_ready_seconds"] = (
                "Age of the oldest due message: the queue's lag.",
                snapshot.oldest_ready_seconds,
            )
        for name, (documentation, value) in gauges.items():
            gauge = GaugeMetricFamily(f"{self._prefix}_queue_{name}", documentation)
            gauge.add_metric([], value)
            yield gauge


def install(
    registry: CollectorRegistry = REGISTRY, *, prefix: str = "outbox", using: str | None = None
) -> None:
    """Register the metrics and connect the signal receivers. Safe to call twice."""
    if id(registry) in _installed:
        return
    delivered = Counter(
        f"{prefix}_delivered_total", "Messages delivered.", _LABELS, registry=registry
    )
    failed = Counter(
        f"{prefix}_failed_total",
        "Failed attempts that will be retried.",
        _LABELS,
        registry=registry,
    )
    dead = Counter(
        f"{prefix}_dead_lettered_total", "Messages dead-lettered.", _LABELS, registry=registry
    )
    run_hist = Histogram(
        f"{prefix}_run_seconds", "Time spent running a message.", _LABELS, registry=registry
    )
    queued_hist = Histogram(
        f"{prefix}_queued_seconds",
        "Time from creation to the start of the delivering run.",
        _LABELS,
        registry=registry,
    )

    def on_delivered(*, envelope: Any, duration: float, queued_for: float, **_: Any) -> None:
        labels = (envelope.kind, envelope.name)
        delivered.labels(*labels).inc()
        run_hist.labels(*labels).observe(duration)
        queued_hist.labels(*labels).observe(queued_for)

    def on_failed(*, envelope: Any, **_: Any) -> None:
        failed.labels(envelope.kind, envelope.name).inc()

    def on_dead(*, envelope: Any, **_: Any) -> None:
        dead.labels(envelope.kind, envelope.name).inc()

    # weak=False: these closures have no other owner.
    message_delivered.connect(
        on_delivered, weak=False, dispatch_uid=f"{_UID}.delivered.{id(registry)}"
    )
    message_failed.connect(on_failed, weak=False, dispatch_uid=f"{_UID}.failed.{id(registry)}")
    message_dead_lettered.connect(on_dead, weak=False, dispatch_uid=f"{_UID}.dead.{id(registry)}")
    registry.register(_QueueCollector(prefix, using))
    _installed[id(registry)] = registry
