from django.db import models
from django.db.models.functions import Now


class Handled(models.Model):
    """A row per successful handler or job run, written inside the ack transaction."""

    message_id = models.UUIDField(null=True)
    name = models.CharField(max_length=255)
    key = models.CharField(max_length=255, blank=True)
    seq = models.IntegerField(null=True)
    attempt = models.IntegerField(default=1)
    created_at = models.DateTimeField(db_default=Now())

    def __str__(self) -> str:
        return f"{self.name} {self.key}:{self.seq}"
