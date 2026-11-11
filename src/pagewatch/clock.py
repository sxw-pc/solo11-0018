"""Injectable clocks.

All persisted timestamps are UTC epoch seconds.  Tests use FrozenClock so that
scheduling, cooldowns and digest rendering never depend on wall-clock time.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Return the current UTC time as epoch seconds."""


class SystemClock:
    def now(self) -> float:
        return datetime.now(timezone.utc).timestamp()


class FrozenClock:
    def __init__(self, start: float = 1_767_225_600.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now

    def set(self, value: float) -> None:
        self._now = float(value)


def utc_iso(ts: float) -> str:
    """Render an epoch timestamp as a stable, second-precision UTC string."""
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> float:
    """Parse ISO-8601 or an epoch number into epoch seconds."""
    text = value.strip()
    try:
        return float(text)
    except ValueError:
        pass
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()
