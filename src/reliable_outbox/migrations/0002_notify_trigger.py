from django.db import migrations

# A statement-level trigger rather than a NOTIFY from Python: it costs no extra
# round trip per publish, fires once per INSERT statement (so bulk inserts don't
# flood the channel), covers rows written by any client, and - like every
# NOTIFY - is only delivered if the inserting transaction commits.
CREATE = """
CREATE OR REPLACE FUNCTION reliable_outbox_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('reliable_outbox', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER reliable_outbox_notify
AFTER INSERT ON reliable_outbox_outboxmessage
FOR EACH STATEMENT EXECUTE FUNCTION reliable_outbox_notify();
"""

DROP = """
DROP TRIGGER IF EXISTS reliable_outbox_notify ON reliable_outbox_outboxmessage;
DROP FUNCTION IF EXISTS reliable_outbox_notify();
"""


class Migration(migrations.Migration):
    dependencies = [("reliable_outbox", "0001_initial")]

    operations = [migrations.RunSQL(CREATE, DROP)]
