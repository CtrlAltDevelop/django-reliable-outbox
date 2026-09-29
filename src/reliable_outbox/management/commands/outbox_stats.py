from __future__ import annotations

import dataclasses
import json
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from reliable_outbox.operations import stats


class Command(BaseCommand):
    help = "Print queue depth, lag and dead-letter counts."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--json", action="store_true", help="Machine-readable output.")
        parser.add_argument("--database", help="Database alias holding the outbox table.")

    def handle(self, *args: Any, **options: Any) -> None:
        snapshot = dataclasses.asdict(stats(using=options["database"]))
        if options["json"]:
            self.stdout.write(json.dumps(snapshot))
            return
        for name, value in snapshot.items():
            shown = (
                "-" if value is None else (f"{value:.1f}s" if isinstance(value, float) else value)
            )
            self.stdout.write(f"{name:>22}: {shown}")
