"""Typed, validated settings read from ``settings.RELIABLE_OUTBOX``.

Every key is optional. Unknown keys are rejected rather than ignored, because a
misspelt ``LEASE_SECOND`` that silently falls back to the default is the kind
of mistake that only shows up as duplicate deliveries in production.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cache
from types import MappingProxyType
from typing import Any, Literal, get_args

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver

Backoff = Literal["exponential", "fixed"]
BACKOFF_STRATEGIES: tuple[str, ...] = get_args(Backoff)

SETTING_NAME = "RELIABLE_OUTBOX"

LOCAL_DESTINATION = "reliable_outbox.destinations.local.LocalHandlers"


@dataclass(frozen=True, slots=True)
class DestinationConfig:
    """One entry of ``DESTINATIONS``: a dotted class path and its keyword options."""

    backend: str
    options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxSettings:
    DATABASE: str = "default"
    """Database alias holding the outbox table. It must be the one your business
    data lives in, otherwise ``publish`` is no longer atomic with your writes."""

    REQUIRE_TRANSACTION: bool = False
    """Refuse to ``publish``/``enqueue`` outside ``transaction.atomic()``. In
    autocommit mode the row is committed immediately, which is correct but
    rarely what the caller meant."""

    BATCH_SIZE: int = 10
    LEASE_SECONDS: float = 60.0
    POLL_INTERVAL: float = 1.0

    MAX_ATTEMPTS: int = 10
    """Attempts for published events. Jobs set their own through ``retries``."""

    BACKOFF: Backoff = "exponential"
    BACKOFF_BASE_SECONDS: float = 1.0
    BACKOFF_MAX_SECONDS: float = 3600.0

    BLOCK_KEY_ON_DEAD_LETTER: bool = True
    """When the oldest message of a key is dead-lettered, hold the rest of that
    key until it is requeued (strict order) instead of letting them overtake it."""

    NOTIFY: bool = True
    """Listen for ``NOTIFY`` so idle workers wake as soon as a row commits.
    Polling at ``POLL_INTERVAL`` still runs underneath as the fallback."""

    DESTINATIONS: Mapping[str, DestinationConfig] = field(
        default_factory=lambda: MappingProxyType({"default": DestinationConfig(LOCAL_DESTINATION)})
    )
    ROUTES: tuple[tuple[str, str], ...] = ()
    """``(pattern, destination)`` pairs matched with ``fnmatch`` against the
    event type, first match wins. Unmatched events go to ``"default"``."""

    RETENTION_HOURS: float = 24.0 * 7
    AUTODISCOVER: tuple[str, ...] = ("jobs", "outbox_handlers")


def _positive(name: str, value: object, *, integer: bool = False) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or (integer and not isinstance(value, int))
        or value <= 0
    ):
        noun = "a positive integer" if integer else "a positive number"
        raise ImproperlyConfigured(f"{SETTING_NAME}[{name!r}] must be {noun}, got {value!r}.")


def _flag(name: str, value: object) -> None:
    if not isinstance(value, bool):
        raise ImproperlyConfigured(f"{SETTING_NAME}[{name!r}] must be a bool, got {value!r}.")


def _destinations(raw: object) -> Mapping[str, DestinationConfig]:
    if not isinstance(raw, Mapping) or not raw:
        raise ImproperlyConfigured(f"{SETTING_NAME}['DESTINATIONS'] must be a non-empty mapping.")
    parsed: dict[str, DestinationConfig] = {}
    for name, entry in raw.items():
        if not isinstance(entry, Mapping) or not isinstance(entry.get("BACKEND"), str):
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['DESTINATIONS'][{name!r}] needs a 'BACKEND' dotted path."
            )
        options = entry.get("OPTIONS", {})
        if not isinstance(options, Mapping):
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['DESTINATIONS'][{name!r}]['OPTIONS'] must be a mapping."
            )
        parsed[str(name)] = DestinationConfig(entry["BACKEND"], MappingProxyType(dict(options)))
    if "default" not in parsed:
        raise ImproperlyConfigured(f"{SETTING_NAME}['DESTINATIONS'] must define 'default'.")
    return MappingProxyType(parsed)


def _routes(
    raw: object, destinations: Mapping[str, DestinationConfig]
) -> tuple[tuple[str, str], ...]:
    """Accept a dict (ordered) or a sequence of pairs; a sequence makes precedence explicit."""
    invalid = ImproperlyConfigured(
        f"{SETTING_NAME}['ROUTES'] must map event-type patterns to destination names."
    )
    if isinstance(raw, Mapping):
        pairs: list[object] = list(raw.items())
    elif isinstance(raw, list | tuple):
        pairs = list(raw)
    else:
        raise invalid
    routes: list[tuple[str, str]] = []
    for pair in pairs:
        if not isinstance(pair, tuple | list) or len(pair) != 2:
            raise invalid
        routes.append((str(pair[0]), str(pair[1])))
    for pattern, target in routes:
        if target not in destinations:
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['ROUTES'][{pattern!r}] points at unknown destination {target!r}."
            )
    return tuple(routes)


def _validate_scalars(values: dict[str, Any]) -> None:
    """Check the plain numeric, boolean and choice settings in place."""
    for name in ("BATCH_SIZE", "MAX_ATTEMPTS"):
        if name in values:
            _positive(name, values[name], integer=True)
    for name in (
        "LEASE_SECONDS",
        "POLL_INTERVAL",
        "BACKOFF_BASE_SECONDS",
        "BACKOFF_MAX_SECONDS",
        "RETENTION_HOURS",
    ):
        if name in values:
            _positive(name, values[name])
            values[name] = float(values[name])
    for name in ("REQUIRE_TRANSACTION", "BLOCK_KEY_ON_DEAD_LETTER", "NOTIFY"):
        if name in values:
            _flag(name, values[name])

    if "BACKOFF" in values and values["BACKOFF"] not in BACKOFF_STRATEGIES:
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['BACKOFF'] must be one of {BACKOFF_STRATEGIES}, "
            f"got {values['BACKOFF']!r}."
        )


def load(raw: Mapping[str, Any] | None) -> OutboxSettings:
    """Build and validate settings from the user's dictionary."""
    values = dict(raw or {})
    known = {f.name for f in dataclasses.fields(OutboxSettings)}
    unknown = sorted(set(values) - known)
    if unknown:
        raise ImproperlyConfigured(f"Unknown {SETTING_NAME} setting(s): {', '.join(unknown)}.")

    _validate_scalars(values)
    if "DATABASE" in values and values["DATABASE"] not in django_settings.DATABASES:
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['DATABASE'] is {values['DATABASE']!r}, which is not in DATABASES."
        )
    if "AUTODISCOVER" in values:
        values["AUTODISCOVER"] = tuple(str(module) for module in values["AUTODISCOVER"])

    if "DESTINATIONS" in values:
        values["DESTINATIONS"] = _destinations(values["DESTINATIONS"])
    if "ROUTES" in values:
        destinations = values.get("DESTINATIONS", OutboxSettings().DESTINATIONS)
        values["ROUTES"] = _routes(values["ROUTES"], destinations)

    loaded = OutboxSettings(**values)
    if loaded.BACKOFF_MAX_SECONDS < loaded.BACKOFF_BASE_SECONDS:
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['BACKOFF_MAX_SECONDS'] must not be below 'BACKOFF_BASE_SECONDS'."
        )
    return loaded


@cache
def get_settings() -> OutboxSettings:
    return load(getattr(django_settings, SETTING_NAME, None))


@receiver(setting_changed)
def _reset(*, setting: str, **kwargs: object) -> None:
    if setting in (SETTING_NAME, "DATABASES"):
        get_settings.cache_clear()
