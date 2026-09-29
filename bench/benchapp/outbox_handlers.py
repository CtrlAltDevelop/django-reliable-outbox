import time

from django.db import connection

from reliable_outbox import Envelope, handler


@handler("bench.noop")
def noop(envelope: Envelope) -> None:
    """Measures the queue itself: claim, dispatch, ack."""


@handler("bench.latency")
def latency(envelope: Envelope) -> None:
    # Same machine, so the wall clocks of publisher and worker agree.
    elapsed = time.time() - envelope.payload["sent_at"]
    with connection.cursor() as cursor:
        cursor.execute("INSERT INTO bench_latency (seconds) VALUES (%s)", [elapsed])
