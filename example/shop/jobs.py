from reliable_outbox import job

from .models import Order


@job(retries=5, backoff="exponential")
def send_receipt(order_id: int) -> None:
    order = Order.objects.get(pk=order_id)
    if order.receipt_sent:  # already done on an earlier, interrupted attempt
        return
    print(f"Emailing a receipt to {order.email}")  # noqa: T201 - stand-in for a mailer
    order.receipt_sent = True
    order.save(update_fields=["receipt_sent"])
