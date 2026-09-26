from __future__ import annotations

import functools
import os

import psycopg
import pytest
from django.db import connections


@functools.cache
def postgres_unavailable() -> str | None:
    """Why PostgreSQL can't be used, or None when it can."""
    # Straight through psycopg: pytest-django blocks the ORM during collection.
    params = connections["default"].get_connection_params()
    try:
        psycopg.connect(**params, connect_timeout=3).close()
    except psycopg.OperationalError as exc:
        return f"PostgreSQL unreachable ({str(exc).strip().splitlines()[0]})"
    return None


@functools.cache
def redis_unavailable() -> str | None:
    try:
        import redis  # noqa: PLC0415 - only needed when the redis tests run
    except ImportError:
        return "the redis package is not installed"
    url = os.environ.get("REDIS_URL", "redis://localhost:56379/0")
    try:
        redis.Redis.from_url(url, socket_connect_timeout=2).ping()
    except redis.RedisError as exc:
        return f"Redis unreachable at {url} ({exc})"
    return None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Anything touching the database needs PostgreSQL, so a django_db test is a
    # postgres test without having to say so twice.
    for item in items:
        if item.get_closest_marker("django_db"):
            item.add_marker(pytest.mark.postgres)
        if item.get_closest_marker("postgres") and (reason := postgres_unavailable()):
            item.add_marker(pytest.mark.skip(reason=reason))
        if item.get_closest_marker("redis") and (reason := redis_unavailable()):
            item.add_marker(pytest.mark.skip(reason=reason))
