"""A minimal project wired for django-reliable-outbox. Not production settings."""

import os
from urllib.parse import unquote, urlsplit

_db = urlsplit(
    os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:55435/outbox")
)

SECRET_KEY = "example-only-not-a-real-key"
DEBUG = True
ALLOWED_HOSTS = ["localhost", "127.0.0.1"]
USE_TZ = True
ROOT_URLCONF = "config.urls"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "reliable_outbox",
    "shop",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

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

STATIC_URL = "static/"

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

RELIABLE_OUTBOX = {
    # Fail loudly if someone publishes outside a transaction by mistake.
    "REQUIRE_TRANSACTION": True,
    "LEASE_SECONDS": 30,
}

LOGGING = {
    "version": 1,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {"reliable_outbox": {"handlers": ["console"], "level": "INFO"}},
}
