import json
import os
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest
from django.test import override_settings

from reliable_outbox import Envelope, PermanentError, publish
from reliable_outbox.destinations import get_destination
from reliable_outbox.destinations.kafka import KafkaDeliveryError, KafkaDestination
from reliable_outbox.destinations.redis import RedisStreamDestination
from reliable_outbox.destinations.webhook import (
    MESSAGE_ID_HEADER,
    SIGNATURE_HEADER,
    DeliveryError,
    WebhookDestination,
    sign,
    verify_signature,
)
from reliable_outbox.models import OutboxMessage, Status
from tests.helpers import drain

SECRET = "whsec-test-only"


def envelope(**overrides: Any) -> Envelope:
    fields: dict[str, Any] = {
        "message_id": uuid.uuid4(),
        "kind": "event",
        "name": "order.created",
        "payload": {"id": 7},
        "key": "order-7",
        "headers": {"trace": "t1"},
        "attempt": 1,
        "max_attempts": 5,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    return Envelope(**{**fields, **overrides})


# --- signatures ------------------------------------------------------------


def test_signature_round_trip() -> None:
    body = b'{"id":7}'
    header = sign(body, SECRET, timestamp=1_700_000_000)
    assert header.startswith("t=1700000000,v1=")
    assert verify_signature(body, header, SECRET, now=1_700_000_100)


@pytest.mark.parametrize(
    ("body", "header_secret", "now"),
    [
        (b'{"id":8}', SECRET, 1_700_000_000),  # tampered body
        (b'{"id":7}', "other-secret", 1_700_000_000),  # wrong key
        (b'{"id":7}', SECRET, 1_700_000_301),  # replayed after the window
    ],
)
def test_signature_rejections(body: bytes, header_secret: str, now: int) -> None:
    header = sign(b'{"id":7}', header_secret, timestamp=1_700_000_000)
    assert not verify_signature(body, header, SECRET, now=now)


@pytest.mark.parametrize("header", ["", "garbage", "t=abc,v1=00", "v1=00"])
def test_malformed_signature_headers_are_rejected(header: str) -> None:
    assert not verify_signature(b"{}", header, SECRET)


def test_bytes_and_str_secrets_agree() -> None:
    assert sign(b"x", SECRET, timestamp=1) == sign(b"x", SECRET.encode(), timestamp=1)


# --- webhook delivery ------------------------------------------------------


class Receiver(BaseHTTPRequestHandler):
    status = 204
    requests: list[tuple[dict[str, str], bytes]] = []

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        Receiver.requests.append((dict(self.headers), body))
        self.send_response(Receiver.status)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def receiver() -> Iterator[str]:
    Receiver.status = 204
    Receiver.requests = []
    server = HTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/hook"
    server.shutdown()
    server.server_close()


def test_webhook_posts_a_signed_body(receiver: str) -> None:
    sent = envelope()
    WebhookDestination(url=receiver, secret=SECRET, headers={"X-Env": "test"}).deliver(sent)

    ((headers, body),) = Receiver.requests
    assert verify_signature(body, headers[SIGNATURE_HEADER], SECRET)
    assert headers[MESSAGE_ID_HEADER] == str(sent.message_id)
    assert headers["X-Env"] == "test"
    assert json.loads(body) == sent.to_dict()


@pytest.mark.parametrize(
    ("status", "error"),
    [(400, PermanentError), (410, PermanentError), (429, DeliveryError), (503, DeliveryError)],
)
def test_webhook_status_codes(receiver: str, status: int, error: type[Exception]) -> None:
    Receiver.status = status
    with pytest.raises(error):
        WebhookDestination(url=receiver, secret=SECRET).deliver(envelope())


def test_webhook_unreachable_is_retryable() -> None:
    destination = WebhookDestination(url="http://127.0.0.1:9/", secret=SECRET, timeout=2)
    with pytest.raises(DeliveryError):
        destination.deliver(envelope())


def test_webhook_validates_its_options() -> None:
    with pytest.raises(ValueError, match="http"):
        WebhookDestination(url="ftp://example.com", secret=SECRET)
    with pytest.raises(ValueError, match="secret"):
        WebhookDestination(url="https://example.com", secret="")
    assert SECRET not in repr(WebhookDestination(url="https://example.com", secret=SECRET))


@pytest.mark.django_db(transaction=True)
def test_worker_delivers_through_a_configured_webhook(receiver: str) -> None:
    config = {
        "DESTINATIONS": {
            "default": {"BACKEND": "reliable_outbox.destinations.local.LocalHandlers"},
            "partner": {
                "BACKEND": "reliable_outbox.destinations.webhook.WebhookDestination",
                "OPTIONS": {"url": receiver, "secret": SECRET},
            },
        },
        "ROUTES": {"partner.*": "partner"},
    }
    with override_settings(RELIABLE_OUTBOX=config):
        assert isinstance(get_destination("partner"), WebhookDestination)
        message_id = publish("partner.order_shipped", {"id": 1})
        drain()
    assert OutboxMessage.objects.get().status == Status.DELIVERED
    ((_, body),) = Receiver.requests
    assert json.loads(body)["message_id"] == str(message_id)


# --- Redis Streams ---------------------------------------------------------


@pytest.mark.redis
def test_redis_stream_receives_the_event() -> None:
    import redis  # noqa: PLC0415

    client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:56379/0"))
    stream = f"outbox-test:{uuid.uuid4()}"
    try:
        destination = RedisStreamDestination(client=client, stream=stream, maxlen=100)
        sent = envelope()
        destination.deliver(sent)
        ((_, fields),) = client.xrange(stream)
        assert fields[b"message_id"] == str(sent.message_id).encode()
        assert fields[b"key"] == b"order-7"
        assert json.loads(fields[b"payload"]) == {"id": 7}
    finally:
        client.delete(stream)


# --- Kafka (producer faked; the contract is flush-and-check) ---------------


class FakeProducer:
    def __init__(self, *, error: object = None, unflushed: int = 0) -> None:
        self.records: list[dict[str, Any]] = []
        self.error = error
        self.unflushed = unflushed
        self._callbacks: list[Any] = []

    def produce(self, topic: str, **record: Any) -> None:
        self.records.append({"topic": topic, **record})
        self._callbacks.append(record["on_delivery"])

    def flush(self, timeout: float) -> int:
        for callback in self._callbacks:
            callback(self.error, None)
        return self.unflushed


def test_kafka_produces_keyed_records_and_waits_for_the_ack() -> None:
    producer = FakeProducer()
    KafkaDestination(topic="events.{name}", producer=producer).deliver(envelope())
    (record,) = producer.records
    assert record["topic"] == "events.order.created"
    assert record["key"] == b"order-7"
    assert json.loads(record["value"])["payload"] == {"id": 7}


@pytest.mark.parametrize("producer", [FakeProducer(error="broker down"), FakeProducer(unflushed=1)])
def test_kafka_without_an_ack_is_an_error(producer: FakeProducer) -> None:
    with pytest.raises(KafkaDeliveryError):
        KafkaDestination(producer=producer).deliver(envelope(key=None))
