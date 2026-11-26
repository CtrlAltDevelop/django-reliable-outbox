from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from prometheus_client import CollectorRegistry

from reliable_outbox import publish
from reliable_outbox.envelope import Envelope
from reliable_outbox.metrics import _snapshot as snapshot
from reliable_outbox.metrics import opentelemetry as otel_metrics
from reliable_outbox.metrics import prometheus as prom_metrics
from reliable_outbox.signals import message_dead_lettered, message_delivered, message_failed
from tests.helpers import drain

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def fresh_snapshots() -> None:
    snapshot._cache.clear()  # the exporters reuse a snapshot for a second


def envelope() -> Envelope:
    return Envelope(
        message_id=uuid4(),
        kind="job",
        name="demo",
        payload={},
        key=None,
        headers={},
        attempt=1,
        max_attempts=3,
        created_at=datetime.now(UTC),
    )


def counter_value(points: Sequence[Any]) -> float:
    return float(points[0].value)


def test_prometheus_counts_signals_and_exports_queue_gauges() -> None:
    registry = CollectorRegistry()
    prom_metrics.install(registry)
    prom_metrics.install(registry)  # idempotent
    publish("test.record", {"seq": 1})
    sample = {"envelope": envelope(), "sender": None}

    message_delivered.send(duration=0.5, queued_for=2.0, **sample)
    message_failed.send(error=RuntimeError(), retry_in=1.0, **sample)
    message_dead_lettered.send(error=RuntimeError(), **sample)

    def value(name: str) -> float | None:
        return registry.get_sample_value(name, {"kind": "job", "name": "demo"})

    assert value("outbox_delivered_total") == 1
    assert value("outbox_failed_total") == 1
    assert value("outbox_dead_lettered_total") == 1
    assert value("outbox_run_seconds_sum") == 0.5
    assert value("outbox_queued_seconds_sum") == 2.0
    assert registry.get_sample_value("outbox_queue_ready") == 1
    assert registry.get_sample_value("outbox_queue_oldest_ready_seconds") is not None


def test_prometheus_records_real_deliveries() -> None:
    registry = CollectorRegistry()
    prom_metrics.install(registry, prefix="real")
    publish("test.record", {"seq": 1})
    drain()
    labels = {"kind": "event", "name": "test.record"}
    assert registry.get_sample_value("real_delivered_total", labels) == 1
    assert registry.get_sample_value("real_queue_delivered", None) == 1
    assert registry.get_sample_value("real_queue_ready") == 0


def test_opentelemetry_records_signals_and_observes_queue_gauges() -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    otel_metrics.install(provider)
    otel_metrics.install(provider)  # idempotent
    publish("test.record", {"seq": 1})

    message_delivered.send(sender=None, envelope=envelope(), duration=0.5, queued_for=2.0)

    data = reader.get_metrics_data()
    assert data is not None
    points = {
        metric.name: metric.data.data_points
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert counter_value(points["outbox.messages.delivered"]) == 1
    assert counter_value(points["outbox.queue.ready"]) == 1
    assert "outbox.queue.oldest_ready" in points
