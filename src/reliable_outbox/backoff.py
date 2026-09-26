"""How long to wait before trying a failed message again.

Exponential backoff with *full jitter*: the delay is drawn uniformly from
``[0, min(cap, base * 2 ** (attempt - 1))]``. Without the jitter, every message
that failed together during an outage retries together too, and the recovering
dependency is knocked straight back over. Full jitter spreads them out the most
for the least total waiting (see the AWS Architecture Blog post "Exponential
Backoff and Jitter").
"""

from __future__ import annotations

import random

# 2 ** 62 seconds is already longer than any cap anyone will configure; the
# clamp only exists so a huge attempt count can't overflow a float.
_MAX_EXPONENT = 62


def backoff_ceiling(attempt: int, *, base: float, cap: float) -> float:
    """The upper bound of the exponential delay after ``attempt`` failed attempts."""
    exponent = min(max(attempt - 1, 0), _MAX_EXPONENT)
    return float(min(cap, base * 2**exponent))


def retry_delay(
    strategy: str, attempt: int, *, base: float, cap: float, rng: random.Random
) -> float:
    """Seconds until the next attempt. ``"fixed"`` always waits ``base``."""
    if strategy == "fixed":
        return min(base, cap)
    return rng.uniform(0.0, backoff_ceiling(attempt, base=base, cap=cap))
