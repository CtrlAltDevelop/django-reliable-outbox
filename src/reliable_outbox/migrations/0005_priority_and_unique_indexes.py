from django.contrib.postgres.operations import AddIndexConcurrently, RemoveIndexConcurrently
from django.db import migrations, models

UNIQUE_INDEX = "outbox_unique_pending"
PENDING_INDEX = models.Index(
    condition=models.Q(("status", "pending")),
    fields=["-priority", "id"],
    name="outbox_pending_prio_idx",
)


class Migration(migrations.Migration):
    """Build the claim and uniqueness indexes without locking out writers.

    Non-atomic because CREATE/DROP INDEX CONCURRENTLY cannot run in a
    transaction. If it is interrupted, drop the half-built (INVALID) index and
    run it again; every step is IF [NOT] EXISTS where Django allows it.
    """

    atomic = False

    dependencies = [("reliable_outbox", "0004_priority_and_unique_fields")]

    operations = [
        # New claim index first, then drop the old one, so claims are never unindexed.
        AddIndexConcurrently(model_name="outboxmessage", index=PENDING_INDEX),
        RemoveIndexConcurrently(model_name="outboxmessage", name="outbox_pending_idx"),
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {UNIQUE_INDEX} "
                        "ON reliable_outbox_outboxmessage (name, unique_key) "
                        "WHERE status = 'pending' AND unique_key IS NOT NULL"
                    ),
                    reverse_sql=f"DROP INDEX CONCURRENTLY IF EXISTS {UNIQUE_INDEX}",
                )
            ],
            state_operations=[
                migrations.AddConstraint(
                    model_name="outboxmessage",
                    constraint=models.UniqueConstraint(
                        condition=models.Q(("status", "pending"), ("unique_key__isnull", False)),
                        fields=("name", "unique_key"),
                        name=UNIQUE_INDEX,
                    ),
                )
            ],
        ),
    ]
