import pytest
from django.core import checks
from django.db import connections

from reliable_outbox.models import Kind, OutboxMessage, Status


@pytest.mark.django_db
def test_database_clock_fills_the_timestamps() -> None:
    message = OutboxMessage.objects.create(
        name="order.created", payload={"id": 1}, max_attempts=3, backoff="exponential"
    )
    message.refresh_from_db()
    assert message.status == Status.PENDING
    assert message.kind == Kind.EVENT
    assert message.run_at is not None
    assert message.run_at == message.created_at
    assert message.locked_until is None
    assert str(message) == f"order.created #{message.pk} (pending)"


def test_postgresql_check_passes() -> None:
    assert [m for m in checks.run_checks() if m.id == "reliable_outbox.E001"] == []


def test_postgresql_check_flags_other_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connections["default"], "vendor", "sqlite")
    found = [m for m in checks.run_checks() if m.id == "reliable_outbox.E001"]
    assert len(found) == 1
    assert "sqlite" in found[0].msg
