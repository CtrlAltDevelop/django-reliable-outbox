from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class Envelope:
    """What a handler or destination receives: the message, detached from the ORM.

    ``attempt`` starts at 1. Because delivery is at-least-once, a handler may see
    the same ``message_id`` more than once; that is what the inbox helper is for.
    """

    message_id: UUID
    kind: str
    name: str
    payload: Any
    key: str | None
    headers: Mapping[str, Any]
    attempt: int
    max_attempts: int
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """A JSON-ready form for destinations that ship the message elsewhere."""
        return {
            "message_id": str(self.message_id),
            "kind": self.kind,
            "name": self.name,
            "payload": self.payload,
            "key": self.key,
            "headers": dict(self.headers),
            "attempt": self.attempt,
            "created_at": self.created_at.isoformat(),
        }
