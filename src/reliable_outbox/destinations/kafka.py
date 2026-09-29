"""Produce each event to Kafka. Needs the ``kafka`` extra (confluent-kafka)."""

from __future__ import annotations

import json
from typing import Any

from reliable_outbox.envelope import Envelope


class KafkaDeliveryError(Exception):
    """The broker did not acknowledge the record; it will be retried."""


class KafkaDestination:
    """``OPTIONS``: ``topic`` (``{name}`` is replaced by the event type), ``config``
    (passed to ``confluent_kafka.Producer``), optional ``flush_timeout``.

    The message key becomes the Kafka record key, so per-key order carries over
    into partition order. Delivery is synchronous: ``deliver`` flushes and
    waits for the broker's ack before the outbox row is marked delivered. That
    is slower than fire-and-forget batching but it is the only way to know.
    Set ``enable.idempotence`` in ``config`` to stop producer retries from
    duplicating records within a session.
    """

    def __init__(
        self,
        *,
        topic: str = "{name}",
        config: dict[str, Any] | None = None,
        flush_timeout: float = 10.0,
        producer: Any = None,
    ) -> None:
        if producer is None:
            from confluent_kafka import Producer  # noqa: PLC0415 - optional dependency

            producer = Producer({"enable.idempotence": True, **(config or {})})
        self._producer = producer
        self.topic = topic
        self.flush_timeout = flush_timeout

    def deliver(self, envelope: Envelope) -> None:
        errors: list[object] = []

        def on_delivery(error: object, _record: object) -> None:
            if error is not None:
                errors.append(error)

        self._producer.produce(
            self.topic.format(name=envelope.name),
            key=envelope.key.encode() if envelope.key else None,
            value=json.dumps(envelope.to_dict(), separators=(",", ":")).encode(),
            headers={"message_id": str(envelope.message_id)},
            on_delivery=on_delivery,
        )
        remaining = self._producer.flush(self.flush_timeout)
        if errors:
            raise KafkaDeliveryError(f"Kafka rejected {envelope.message_id}: {errors[0]}")
        if remaining:
            raise KafkaDeliveryError(f"Kafka did not ack {envelope.message_id} in time")
