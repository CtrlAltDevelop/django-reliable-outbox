import signal
import threading
import time

import pytest
from django.core.management import call_command

from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.shutdown import install_signal_handlers, shutdown_signals
from reliable_outbox.worker import Worker
from tests.helpers import make_messages, statuses
from tests.testapp.models import Handled


def test_first_signal_asks_to_stop_and_the_second_insists() -> None:
    stop = threading.Event()
    before = signal.getsignal(signal.SIGINT)
    restore = install_signal_handlers(stop)
    try:
        signal.raise_signal(signal.SIGINT)
        assert stop.is_set()
        with pytest.raises(KeyboardInterrupt):
            signal.raise_signal(signal.SIGINT)
        # The second signal put the original handlers back.
        assert signal.getsignal(signal.SIGINT) is before
    finally:
        restore()


def test_handlers_are_restored() -> None:
    before = {s: signal.getsignal(s) for s in shutdown_signals()}
    install_signal_handlers(threading.Event())()
    assert {s: signal.getsignal(s) for s in shutdown_signals()} == before
    assert signal.SIGTERM in shutdown_signals()


def test_off_main_thread_is_a_no_op() -> None:
    results: list[object] = []
    thread = threading.Thread(
        target=lambda: results.append(install_signal_handlers(threading.Event()))
    )
    thread.start()
    thread.join()
    assert callable(results[0])


@pytest.mark.django_db(transaction=True)
def test_stop_finishes_the_current_message_and_hands_back_the_rest() -> None:
    make_messages(5, payload=lambda i: {"seq": i, "sleep": 0.5})
    worker = Worker(batch_size=5, lease_seconds=30)
    thread = threading.Thread(target=worker.run)
    thread.start()
    time.sleep(0.25)  # mid-way through the first handler
    worker.stop()
    thread.join(timeout=10)
    assert not thread.is_alive()

    # The message in hand completed; the four unstarted ones are free for the
    # next worker immediately, with no attempt charged.
    assert list(Handled.objects.values_list("seq", flat=True)) == [0]
    assert statuses() == {"delivered": 1, "pending": 4}
    pending = OutboxMessage.objects.filter(status=Status.PENDING)
    assert set(pending.values_list("attempts", "locked_until", "locked_by")) == {(0, None, "")}


@pytest.mark.django_db(transaction=True)
def test_command_runs_several_worker_threads() -> None:
    make_messages(40)
    call_command("outbox_worker", "--burst", "--concurrency", "4", "--no-notify")
    assert statuses() == {"delivered": 40}
    assert len(set(OutboxMessage.objects.values_list("locked_by", flat=True))) >= 2
