from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from reliable_outbox.worker import Worker


class Command(BaseCommand):
    help = "Deliver outbox messages and run queued jobs."

    def add_arguments(self, parser: CommandParser) -> None:
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
        Worker(
            using=options["database"],
            batch_size=options["batch_size"],
            lease_seconds=options["lease"],
            poll_interval=options["poll_interval"],
            notify=options["notify"],
        ).run(burst=options["burst"])
