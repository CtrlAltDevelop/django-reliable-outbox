from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from reliable_outbox.conf import get_settings
from reliable_outbox.models import OutboxMessage, Status
from reliable_outbox.operations import requeue


class Command(BaseCommand):
    help = "Move dead-lettered messages back into the queue with fresh attempts."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("ids", nargs="*", type=int, help="Outbox message ids to requeue.")
        parser.add_argument("--all", action="store_true", help="Requeue every dead message.")
        parser.add_argument("--name", help="Only messages with this event type or job name.")
        parser.add_argument("--key", help="Only messages with this ordering key.")
        parser.add_argument("--database", help="Database alias holding the outbox table.")
        parser.add_argument(
            "--dry-run", action="store_true", help="Report how many would be requeued."
        )

    def handle(self, *args: Any, **options: Any) -> None:
        ids: list[int] = options["ids"]
        if not (ids or options["all"] or options["name"] or options["key"]):
            raise CommandError("Say what to requeue: ids, --name, --key, or --all.")

        alias = options["database"] or get_settings().DATABASE
        dead = OutboxMessage.objects.using(alias).filter(status=Status.DEAD)
        if ids:
            dead = dead.filter(pk__in=ids)
        if options["name"]:
            dead = dead.filter(name=options["name"])
        if options["key"]:
            dead = dead.filter(key=options["key"])

        if options["dry_run"]:
            self.stdout.write(f"Would requeue {dead.count()} dead message(s).")
            return
        count = requeue(dead, using=alias)
        self.stdout.write(self.style.SUCCESS(f"Requeued {count} dead message(s)."))
