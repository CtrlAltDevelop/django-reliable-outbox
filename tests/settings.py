"""Settings for the test suite.

The database is PostgreSQL on purpose: SKIP LOCKED, leases and NOTIFY are the
point of this library, and SQLite has none of them. Point ``DATABASE_URL`` at
any server you like; the default matches ``make pg-up``.
"""

import os
from urllib.parse import unquote, urlsplit

_url = urlsplit(
    os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:55435/outbox")
)

SECRET_KEY = "tests-only-not-a-real-key"
USE_TZ = True
TIME_ZONE = "UTC"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        # Worker subprocesses spawned by the tests must use the test database
        # pytest-django created, not the one named in the URL.
        "NAME": os.environ.get("OUTBOX_DB_NAME") or _url.path.lstrip("/"),
        "USER": unquote(_url.username or ""),
        "PASSWORD": unquote(_url.password or ""),
        "HOST": _url.hostname or "localhost",
        "PORT": str(_url.port or 5432),
        "CONN_MAX_AGE": 0,
    }
}

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "reliable_outbox",
    "tests.testapp",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "tests.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

RELIABLE_OUTBOX = {
    "BACKOFF_BASE_SECONDS": 0.01,
    "BACKOFF_MAX_SECONDS": 0.05,
    "POLL_INTERVAL": 0.1,
}
