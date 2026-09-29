from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.test import Client
from django.urls import reverse

from reliable_outbox import publish, queries
from reliable_outbox.models import OutboxMessage, Status
from tests.helpers import drain

pytestmark = pytest.mark.django_db(transaction=True)

CHANGELIST = "admin:reliable_outbox_outboxmessage_changelist"


@pytest.fixture
def admin_client_() -> Client:
    user = User.objects.create_superuser("admin", "admin@example.com", "pw-test-only")
    client = Client()
    client.force_login(user)
    return client


def test_changelist_search_and_filters(admin_client_: Client) -> None:
    publish("order.created", {}, key="order-1")
    publish("invoice.issued", {}, key="inv-9")
    url = reverse(CHANGELIST)

    response = admin_client_.get(url)
    assert response.status_code == 200
    assert b"order.created" in response.content

    response = admin_client_.get(url, {"q": "inv-9"})
    assert b"invoice.issued" in response.content
    assert b"order.created" not in response.content

    for state in ("ready", "in_flight", "scheduled", "retrying"):
        assert admin_client_.get(url, {"state": state}).status_code == 200


def test_state_filter_distinguishes_leased_and_scheduled(admin_client_: Client) -> None:
    publish("test.record", {"seq": 1})
    publish("reminder.due", {}, delay=timedelta(hours=1))
    queries.claim("default", worker="w", limit=1, lease_seconds=60)
    url = reverse(CHANGELIST)

    in_flight = admin_client_.get(url, {"state": "in_flight"}).content
    assert b"test.record" in in_flight
    assert b"reminder.due" not in in_flight
    scheduled = admin_client_.get(url, {"state": "scheduled"}).content
    assert b"reminder.due" in scheduled


def test_detail_page_is_read_only(admin_client_: Client) -> None:
    publish("test.permanent", {})
    drain()
    message = OutboxMessage.objects.get()
    url = reverse("admin:reliable_outbox_outboxmessage_change", args=[message.pk])
    response = admin_client_.get(url)
    assert response.status_code == 200
    assert b"customer 42 no longer exists" in response.content
    assert b'name="_save"' not in response.content


def test_requeue_action(admin_client_: Client) -> None:
    publish("test.permanent", {})
    publish("test.record", {"seq": 1})
    drain()
    ids = [str(pk) for pk in OutboxMessage.objects.values_list("pk", flat=True)]

    response = admin_client_.post(
        reverse(CHANGELIST),
        {"action": "requeue_selected", "_selected_action": ids},
        follow=True,
    )
    assert b"Requeued 1 dead message(s). Skipped 1 that were not dead." in response.content
    assert set(OutboxMessage.objects.values_list("status", flat=True)) == {
        Status.PENDING,
        Status.DELIVERED,
    }


def test_inbox_changelist(admin_client_: Client) -> None:
    response = admin_client_.get(reverse("admin:reliable_outbox_inboxmessage_changelist"))
    assert response.status_code == 200
