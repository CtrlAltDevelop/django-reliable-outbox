from __future__ import annotations

import uuid

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.db.models.functions import Now


class Status(models.TextChoices):
    PENDING = "pending", "Pending"
    DELIVERED = "delivered", "Delivered"
    DEAD = "dead", "Dead"


class Kind(models.TextChoices):
    EVENT = "event", "Event"
    JOB = "job", "Job"


class OutboxMessage(models.Model):
    """One unit of work, written in the same transaction as the change it describes.

    "In flight" is not a status: a pending row whose ``locked_until`` lies in the
    future is leased to a worker. Keeping the lease in a timestamp rather than a
    status is what lets a dead worker's rows fall back into the queue on their own.

    Every time comparison (``run_at``, ``locked_until``) is made with the database
    clock, never the application's, so skew between app servers cannot make a
    message run early or a lease expire late.
    """

    # The auto-increment id doubles as insertion order for per-key ordering.
    id = models.BigAutoField(primary_key=True)
    message_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    kind = models.CharField(max_length=5, choices=Kind.choices, default=Kind.EVENT)
    name = models.CharField(max_length=255, help_text="Event type, or the job's registered name.")
    payload = models.JSONField(encoder=DjangoJSONEncoder)
    headers = models.JSONField(encoder=DjangoJSONEncoder, default=dict, blank=True)
    # NULL, not "", means "unordered": an empty string would be one giant shared key.
    key = models.CharField(  # noqa: DJ001
        max_length=255,
        null=True,
        blank=True,
        help_text="Messages sharing a key are processed one at a time, in insertion order.",
    )
    destination = models.CharField(max_length=100, blank=True)

    status = models.CharField(max_length=9, choices=Status.choices, default=Status.PENDING)
    attempts = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField()
    backoff = models.CharField(max_length=11)

    run_at = models.DateTimeField(db_default=Now(), help_text="Not claimed before this time.")
    locked_until = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=100, blank=True)

    created_at = models.DateTimeField(db_default=Now())
    completed_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    last_traceback = models.TextField(blank=True)

    class Meta:
        verbose_name = "outbox message"
        indexes = [
            # The claim query walks pending rows in id order and stops at LIMIT;
            # without this partial index it would wade through every delivered row.
            models.Index(
                fields=["id"],
                name="outbox_pending_idx",
                condition=models.Q(status="pending"),
            ),
            # Answers "is there an older unfinished message with this key?".
            models.Index(
                fields=["key", "id"],
                name="outbox_key_unfinished_idx",
                condition=models.Q(status__in=["pending", "dead"], key__isnull=False),
            ),
            models.Index(fields=["status", "completed_at"], name="outbox_completed_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.name} #{self.pk} ({self.status})"


class InboxMessage(models.Model):
    """A message a consumer has already processed, for deduplication.

    Delivery is at-least-once, so consumers see some messages twice. Recording
    ``message_id`` in the same transaction as the consumer's own writes turns
    "at least once" into "effects at most once" for everything in that database.
    """

    consumer = models.CharField(max_length=100)
    message_id = models.CharField(max_length=255)
    received_at = models.DateTimeField(db_default=Now())

    class Meta:
        verbose_name = "inbox message"
        constraints = [
            models.UniqueConstraint(fields=["consumer", "message_id"], name="inbox_unique_message")
        ]
        indexes = [models.Index(fields=["received_at"], name="inbox_received_idx")]

    def __str__(self) -> str:
        return f"{self.consumer}: {self.message_id}"
