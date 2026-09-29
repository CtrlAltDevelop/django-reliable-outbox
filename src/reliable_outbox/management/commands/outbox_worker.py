from __future__ import annotations

import threading
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from reliable_outbox.shutdown import install_signal_handlers
from reliable_outbox.worker import Worker, default_worker_name

# Joining with a timeout keeps the main thread responsive to signals: on
# Windows a bare join() blocks Ctrl+C until the thread exits.
_JOIN_SLICE = 0.5


class Command(BaseCommand):
    help = "Deliver outbox messages and run queued jobs."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--concurrency",
            type=int,
            default=1,
            help="Worker threads in this process, each with its own connection.",
        )
        parser.add_argument("--batch-size", type=int, help="Messages claimed per round trip.")
        parser.add_argument("--lease", type=float, help="Seconds a claimed message stays leased.")
        parser.add_argument("--poll-interval", type=float, help="Seconds between idle polls.")
        parser.add_argument("--database", help="Database alias holding the outbox table.")
        parser.add_argument(
            "--burst", action="store_true", help="Exit once nothing is due instead of waiting."
        )
        parser.add_argument(
            "--no-notify",
            dest="notify",
            action="store_false",
            default=None,
            help="Poll only; don't LISTEN for new messages.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        concurrency: int = options["concurrency"]
        if concurrency < 1:
            raise CommandError("--concurrency must be at least 1.")

        stop = threading.Event()
        workers = [
            Worker(
                name=default_worker_name(index),
                using=options["database"],
                batch_size=options["batch_size"],
                lease_seconds=options["lease"],
                poll_interval=options["poll_interval"],
                notify=options["notify"],
                stop_event=stop,
            )
            for index in range(concurrency)
        ]
        restore = install_signal_handlers(stop)
        try:
            if concurrency == 1:
                workers[0].run(burst=options["burst"])
                return
            threads = [
                threading.Thread(
                    target=worker.run, kwargs={"burst": options["burst"]}, name=worker.name
                )
                for worker in workers
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                while thread.is_alive():
                    thread.join(_JOIN_SLICE)
        finally:
            stop.set()
            restore()
