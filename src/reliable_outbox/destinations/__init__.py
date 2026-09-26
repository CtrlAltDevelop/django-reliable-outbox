"""Pluggable destinations for published events."""

from __future__ import annotations

from functools import cache

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from reliable_outbox.conf import SETTING_NAME, get_settings

from .base import Destination

__all__ = ["Destination", "get_destination"]


@cache
def get_destination(name: str) -> Destination:
    """Build (once per process) the destination configured under ``name``."""
    config = get_settings().DESTINATIONS.get(name)
    if config is None:
        raise ImproperlyConfigured(f"{SETTING_NAME}['DESTINATIONS'] has no {name!r} entry.")
    factory = import_string(config.backend)
    instance = factory(**config.options)
    if not isinstance(instance, Destination):
        raise ImproperlyConfigured(f"{config.backend} does not implement deliver(envelope).")
    return instance


@receiver(setting_changed)
def _reset(*, setting: str, **kwargs: object) -> None:
    if setting == SETTING_NAME:
        get_destination.cache_clear()
