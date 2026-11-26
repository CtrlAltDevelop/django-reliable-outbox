from django.db import migrations, models


class Migration(migrations.Migration):
    """Two cheap column adds (metadata-only on PostgreSQL 11+).

    The indexes follow in 0005, built CONCURRENTLY so a large table keeps
    serving publishes and claims while they build.
    """

    dependencies = [("reliable_outbox", "0003_inbox")]

    operations = [
        migrations.AddField(
            model_name="outboxmessage",
            name="priority",
            field=models.SmallIntegerField(
                db_default=0,
                help_text="Higher is claimed first. Never overtakes an older message with the same key.",
            ),
        ),
        migrations.AddField(
            model_name="outboxmessage",
            name="unique_key",
            field=models.CharField(
                blank=True,
                help_text="At most one pending message per name and unique key.",
                max_length=255,
                null=True,
            ),
        ),
    ]
