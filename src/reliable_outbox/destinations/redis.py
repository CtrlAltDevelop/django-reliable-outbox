"""Append each event to a Redis Stream. Needs the ``redis`` extra."""

from __future__ import annotations

import json
from typing import Any

from reliable_outbox.envelope import Envelope


class RedisStreamDestination:
    """``OPTIONS``: ``url``, ``stream`` (``{name}`` is replaced by the event type),
    optional ``maxlen`` for approximate trimming.

    ``XADD`` returns once Redis has the entry, which is the acknowledgement we
    wait for. Redis persistence (AOF/RDB) decides what "has" means after a Redis
    crash; the outbox only guarantees the hand-over happened.
    """

    def __init__(
        self,
        *,
        url: str = "redis://localhost:6379/0",
        stream: str = "outbox:{name}",
        maxlen: int | None = None,
        client: Any = None,
    ) -> None:
        if client is None:
            import redis  # noqa: PLC0415 - optional dependency

            client = redis.Redis.from_url(url)
        self._client = client
        self.stream = stream
        self.maxlen = maxlen

    def deliver(self, envelope: Envelope) -> None:
        data = envelope.to_dict()
        fields = {
            "message_id": data["message_id"],
            "name": envelope.name,
            "key": envelope.key or "",
            "payload": json.dumps(data["payload"], separators=(",", ":")),
            "headers": json.dumps(data["headers"], separators=(",", ":")),
            "created_at": data["created_at"],
        }
        self._client.xadd(
            self.stream.format(name=envelope.name),
            fields,
            maxlen=self.maxlen,
            approximate=True,
        )
