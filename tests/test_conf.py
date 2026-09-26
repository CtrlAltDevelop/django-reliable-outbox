import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from reliable_outbox.conf import LOCAL_DESTINATION, get_settings, load


def test_defaults_are_valid() -> None:
    config = load(None)
    assert config.BATCH_SIZE == 10
    assert config.DESTINATIONS["default"].backend == LOCAL_DESTINATION
    assert config.ROUTES == ()


def test_unknown_key_is_rejected() -> None:
    with pytest.raises(ImproperlyConfigured, match="LEASE_SECOND"):
        load({"LEASE_SECOND": 30})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("BATCH_SIZE", 0),
        ("BATCH_SIZE", 2.5),
        ("BATCH_SIZE", True),
        ("LEASE_SECONDS", -1),
        ("POLL_INTERVAL", "1"),
        ("NOTIFY", "yes"),
        ("BACKOFF", "linear"),
        ("DATABASE", "missing"),
    ],
)
def test_invalid_values_are_rejected(key: str, value: object) -> None:
    with pytest.raises(ImproperlyConfigured, match=key):
        load({key: value})


def test_backoff_cap_must_not_undercut_base() -> None:
    with pytest.raises(ImproperlyConfigured, match="BACKOFF_MAX_SECONDS"):
        load({"BACKOFF_BASE_SECONDS": 10, "BACKOFF_MAX_SECONDS": 5})


def test_destinations_need_a_default_and_a_backend() -> None:
    with pytest.raises(ImproperlyConfigured, match="'default'"):
        load({"DESTINATIONS": {"hooks": {"BACKEND": "x.Y"}}})
    with pytest.raises(ImproperlyConfigured, match="BACKEND"):
        load({"DESTINATIONS": {"default": {"OPTIONS": {}}}})


def test_routes_must_point_at_known_destinations() -> None:
    destinations = {
        "default": {"BACKEND": LOCAL_DESTINATION},
        "hooks": {"BACKEND": "x.Y", "OPTIONS": {"url": "https://example.com"}},
    }
    config = load({"DESTINATIONS": destinations, "ROUTES": {"billing.*": "hooks"}})
    assert config.ROUTES == (("billing.*", "hooks"),)
    assert config.DESTINATIONS["hooks"].options["url"] == "https://example.com"
    with pytest.raises(ImproperlyConfigured, match="unknown destination"):
        load({"ROUTES": [("billing.*", "nowhere")]})


def test_cache_follows_override_settings() -> None:
    with override_settings(RELIABLE_OUTBOX={"BATCH_SIZE": 3}):
        assert get_settings().BATCH_SIZE == 3
    assert get_settings().BATCH_SIZE == 10
