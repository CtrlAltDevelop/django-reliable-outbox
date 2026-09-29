from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from reliable_outbox.conf import get_settings
from reliable_outbox.operations import cleanup


class Command(BaseCommand):
    help = "Delete delivered outbox rows (and optionally dead rows and inbox records)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--older-than",
            type=float,
            metavar="HOURS",
            help="Age in hours; defaults to RELIABLE_OUTBOX['RETENTION_HOURS'].",
        )
        parser.add_argument("--dead", action="store_true", help="Delete old dead rows too.")
        parser.add_argument(
            "--inbox", action="store_true", help="Delete inbox records past the same age."
        )
        parser.add_argument("--batch-size", type=int, default=5_000)
        parser.add_argument("--database", help="Database alias holding the outbox table.")

    def handle(self, *args: Any, **options: Any) -> None:
        hours = options["older_than"]
        if hours is None:
            hours = get_settings().RETENTION_HOURS
        if hours < 0 or options["batch_size"] < 1:
            raise CommandError("--older-than must be >= 0 and --batch-size >= 1.")
        deleted = cleanup(
            older_than=timedelta(hours=hours),
            include_dead=options["dead"],
            inbox=options["inbox"],
            batch_size=options["batch_size"],
            using=options["database"],
        )
        self.stdout.write(self.style.SUCCESS(f"Deleted {deleted} row(s) older than {hours:g}h."))
