"""Deterministic jitter, intra-round staggering and capped failure backoff."""

from __future__ import annotations

import hashlib

MAX_BACKOFF_SECONDS = 3600


def deterministic_jitter(name: str, jitter_seconds: int, seed: int = 0) -> int:
    if jitter_seconds <= 0:
        return 0
    digest = hashlib.sha256(f"{seed}:{name}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % (jitter_seconds + 1)


def stagger_offset(name: str, stagger_seconds: int, seed: int = 0) -> int:
    """Stable intra-round start offset for a watch.

    The value is a pure function of the watch name and the configured cap, so
    it never depends on a watch's position in the input list: reordering or
    reversing the watchlist keeps each watch's offset unchanged.  Reusing the
    same hash family as :func:`deterministic_jitter` keeps scheduling fully
    reproducible across runs.
    """
    return deterministic_jitter(name, stagger_seconds, seed)


def backoff_delay(interval_seconds: int, failures: int,
                  max_seconds: int = MAX_BACKOFF_SECONDS) -> int:
    """Return a capped delay after *failures* consecutive failed checks."""
    if failures <= 0:
        return interval_seconds
    exponent = min(failures, 20)
    return min(interval_seconds * (2 ** exponent), max_seconds)


def next_due_at(*, now: float, interval_seconds: int, failures: int,
                succeeded: bool, watch_name: str, jitter_seconds: int,
                seed: int = 0) -> float:
    """Compute the next run from *now*; overdue rounds are not replayed."""
    delay = interval_seconds if succeeded else backoff_delay(
        interval_seconds, max(1, failures))
    return now + delay + deterministic_jitter(watch_name, jitter_seconds, seed)
