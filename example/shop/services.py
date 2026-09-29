from django.db import transaction

from reliable_outbox import publish

from .jobs import send_receipt
from .models import Order


def place_order(email: str, total_cents: int) -> Order:
    """The order row, its event and its follow-up job commit together or not at all."""
    with transaction.atomic():
        order = Order.objects.create(email=email, total_cents=total_cents)
        publish(
            "order.created",
            {"id": order.pk, "total_cents": total_cents},
            key=f"order-{order.pk}",
        )
        send_receipt.enqueue(order.pk)
    return order
