"""POST each event to an HTTP endpoint, signed with HMAC-SHA256.

The signature header looks like ``t=1767225600,v1=<hex>``, where the MAC is
computed over ``f"{t}.{body}"``. Binding the timestamp into the MAC lets the
receiver reject replays older than a tolerance window; comparing with
``hmac.compare_digest`` keeps the check constant-time. Receivers can use
``verify_signature`` from this module directly.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from collections.abc import Mapping

from reliable_outbox.envelope import Envelope
from reliable_outbox.exceptions import PermanentError

SIGNATURE_HEADER = "X-Outbox-Signature"
MESSAGE_ID_HEADER = "X-Outbox-Message-Id"

# Worth retrying: the receiver is overloaded, timed out, or broken for now.
_RETRYABLE_STATUS = frozenset({408, 425, 429})


class DeliveryError(Exception):
    """The endpoint could not take the message right now; it will be retried."""


def _key(secret: str | bytes) -> bytes:
    return secret.encode() if isinstance(secret, str) else secret


def sign(body: bytes, secret: str | bytes, *, timestamp: int | None = None) -> str:
    """Return the value for the signature header."""
    t = int(time.time()) if timestamp is None else timestamp
    mac = hmac.new(_key(secret), f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={t},v1={mac}"


def verify_signature(
    body: bytes,
    header: str,
    secret: str | bytes,
    *,
    tolerance: float = 300,
    now: float | None = None,
) -> bool:
    """Check a webhook signature on the receiving side.

    ``body`` must be the raw request bytes, not re-serialised JSON. Returns
    False for a bad MAC, a malformed header, or a timestamp outside
    ``tolerance`` seconds of ``now``.
    """
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
        received = parts["v1"]
    except (KeyError, ValueError):
        return False
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance:
        return False
    expected = sign(body, secret, timestamp=timestamp).partition("v1=")[2]
    return hmac.compare_digest(expected, received)


class WebhookDestination:
    """``OPTIONS``: ``url``, ``secret``, optional ``timeout`` (seconds) and ``headers``."""

    def __init__(
        self,
        *,
        url: str,
        secret: str | bytes,
        timeout: float = 10.0,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        if not url.startswith(("https://", "http://")):
            raise ValueError("Webhook url must be http(s).")
        if not secret:
            raise ValueError("Webhook secret must not be empty.")
        self.url = url
        self._secret = secret
        self.timeout = timeout
        self.headers = dict(headers or {})

    def __repr__(self) -> str:  # never show the secret
        return f"<WebhookDestination {self.url}>"

    def deliver(self, envelope: Envelope) -> None:
        body = json.dumps(envelope.to_dict(), separators=(",", ":"), sort_keys=True).encode()
        request = urllib.request.Request(  # noqa: S310 - scheme checked in __init__
            self.url,
            data=body,
            method="POST",
            headers={
                **self.headers,
                "Content-Type": "application/json",
                MESSAGE_ID_HEADER: str(envelope.message_id),
                SIGNATURE_HEADER: sign(body, self._secret),
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout):  # noqa: S310
                return
        except urllib.error.HTTPError as exc:
            # Other 4xx means the receiver understood and refused; sending the
            # same bytes again will not change its mind.
            if 400 <= exc.code < 500 and exc.code not in _RETRYABLE_STATUS:
                raise PermanentError(f"{self.url} rejected the message: HTTP {exc.code}") from exc
            raise DeliveryError(f"{self.url} answered HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise DeliveryError(f"{self.url} unreachable: {exc}") from exc
