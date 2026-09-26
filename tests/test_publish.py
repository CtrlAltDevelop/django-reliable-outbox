from datetime import UTC, datetime, timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.test import override_settings

from reliable_outbox import publish
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.publishing import NotInTransaction

pytestmark = pytest.mark.django_db(transaction=True)


class Boom(Exception):
    pass


def test_rolled_back_transaction_publishes_nothing() -> None:
    with pytest.raises(Boom), transaction.atomic():
        publish("order.created", {"id": 1}, key="order-1")
        raise Boom

    assert not OutboxMessage.objects.exists()


def test_rolled_back_savepoint_drops_only_its_own_messages() -> None:
    with transaction.atomic():
        publish("order.created", {"id": 1})
        with pytest.raises(Boom), transaction.atomic():
            publish("order.paid", {"id": 1})
            raise Boom

    assert list(OutboxMessage.objects.values_list("name", flat=True)) == ["order.created"]


def test_committed_transaction_publishes_the_message() -> None:
    with transaction.atomic():
        message_id = publish(
            "order.created", {"id": 7, "total": "9.50"}, key="order-7", headers={"trace": "t1"}
        )

    message = OutboxMessage.objects.get()
    assert message.message_id == message_id
    assert message.status == Status.PENDING
    assert message.payload == {"id": 7, "total": "9.50"}
    assert message.key == "order-7"
    assert message.headers == {"trace": "t1"}
    assert message.destination == "default"
    assert message.max_attempts == 10
    assert message.attempts == 0


def test_autocommit_writes_immediately() -> None:
    # No surrounding transaction: the INSERT is its own transaction, so the
    # message is as committed as anything else written in autocommit mode.
    publish("order.created", {"id": 1})
    assert OutboxMessage.objects.count() == 1


@override_settings(RELIABLE_OUTBOX={"REQUIRE_TRANSACTION": True})
def test_require_transaction_refuses_autocommit() -> None:
    with pytest.raises(NotInTransaction):
        publish("order.created", {"id": 1})
    with transaction.atomic():
        publish("order.created", {"id": 1})
    assert OutboxMessage.objects.count() == 1


def test_delay_is_relative_to_the_database_clock() -> None:
    publish("reminder.due", {}, delay=timedelta(hours=1))
    message = OutboxMessage.objects.get()
    assert message.run_at - message.created_at == timedelta(hours=1)


def test_explicit_run_at_is_kept() -> None:
    when = datetime(2030, 1, 1, tzinfo=UTC)
    publish("reminder.due", {}, run_at=when)
    assert OutboxMessage.objects.get().run_at == when


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"run_at": datetime(2030, 1, 1)}, "timezone-aware"),
        ({"run_at": datetime(2030, 1, 1, tzinfo=UTC), "delay": timedelta(1)}, "not both"),
        ({"key": ""}, "non-empty"),
        ({"max_attempts": 0}, "at least 1"),
    ],
)
def test_invalid_arguments(kwargs: dict[str, object], error: str) -> None:
    with pytest.raises(ValueError, match=error):
        publish("order.created", {}, **kwargs)  # type: ignore[arg-type]


def test_payload_must_be_json_serialisable() -> None:
    with pytest.raises(TypeError):
        publish("order.created", {"blob": object()})


@override_settings(
    RELIABLE_OUTBOX={
        "DESTINATIONS": {
            "default": {"BACKEND": "reliable_outbox.destinations.local.LocalHandlers"},
            "billing": {"BACKEND": "reliable_outbox.destinations.local.LocalHandlers"},
        },
        "ROUTES": {"invoice.*": "billing"},
    }
)
def test_routes_pick_the_destination() -> None:
    publish("invoice.issued", {})
    publish("order.created", {})
    publish("order.created", {}, destination="billing")
    assert list(OutboxMessage.objects.order_by("id").values_list("destination", flat=True)) == [
        "billing",
        "default",
        "billing",
    ]
    with pytest.raises(ImproperlyConfigured):
        publish("order.created", {}, destination="nowhere")
