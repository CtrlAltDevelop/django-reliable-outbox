from reliable_outbox import Envelope, handler, inbox


@handler("order.created")
@inbox.idempotent("analytics")
def count_order(envelope: Envelope) -> None:
    # Runs at least once per event; the inbox makes the effect happen once.
    print(f"analytics: order {envelope.payload['id']} for {envelope.payload['total_cents']}c")  # noqa: T201
