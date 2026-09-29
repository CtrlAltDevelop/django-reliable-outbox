"""Settings for the benchmark driver and the worker processes it starts."""

import os
from urllib.parse import unquote, urlsplit

_db = urlsplit(
    os.environ.get("BENCH_DATABASE_URL", "postgres://postgres:postgres@localhost:55436/bench")
)

SECRET_KEY = "bench-only-not-a-real-key"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["django.contrib.contenttypes", "reliable_outbox", "bench.benchapp"]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _db.path.lstrip("/"),
        "USER": unquote(_db.username or ""),
        "PASSWORD": unquote(_db.password or ""),
        "HOST": _db.hostname or "localhost",
        "PORT": str(_db.port or 5432),
    }
}
