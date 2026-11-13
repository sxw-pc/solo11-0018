"""Due selection and bounded-concurrency rounds."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

from ..models import WatchSpec
from .backoff import deterministic_jitter

T = TypeVar("T")


def is_due(state: dict[str, object] | None, now: float) -> bool:
    if state is None:
        return True
    return float(state.get("next_due_at", 0.0)) <= now


def select_watches(watches: Iterable[WatchSpec], *, names: Sequence[str] = (),
                   tags: Sequence[str] = (), due_only: bool = False,
                   states: dict[str, dict[str, object]] | None = None,
                   now: float = 0.0) -> list[WatchSpec]:
    requested_names = set(names)
    requested_tags = set(tags)
    selected: list[WatchSpec] = []
    for watch in sorted(watches, key=lambda item: item.name):
        if not watch.enabled:
            continue
        if requested_names and watch.name not in requested_names:
            continue
        if requested_tags and not requested_tags.intersection(watch.tags):
            continue
        if due_only and not is_due((states or {}).get(watch.name), now):
            continue
        selected.append(watch)
    return selected


def _default_sleep(seconds: float) -> None:
    time.sleep(max(0.0, seconds))


def execute_round(watches: Sequence[WatchSpec],
                  execute: Callable[[WatchSpec], T],
                  max_workers: int,
                  jitter_seconds: int | Callable[[WatchSpec], int] = 0,
                  *,
                  sleep: Callable[[float], None] = _default_sleep) -> list[T]:
    """Run one check per watch without exceeding the global worker cap.

    Each watch waits its deterministic, name-based jitter before issuing its
    request, so starts are spread across the round instead of saturating the
    worker pool instantly. Delays are applied inside the worker tasks (never on
    the submitting thread), therefore a slow or failing watch only delays its
    own retries and never blocks the other watches; results are returned in the
    input order, matching one-by-one execution.
    """
    if not watches:
        return []
    workers = max(1, min(int(max_workers), len(watches)))
    if workers == 1:
        return [execute(watch) for watch in watches]

    def delay_for(watch: WatchSpec) -> int:
        if callable(jitter_seconds):
            return max(0, int(jitter_seconds(watch)))
        return deterministic_jitter(watch.name, int(jitter_seconds))

    def run_with_jitter(watch: WatchSpec) -> T:
        delay = delay_for(watch)
        if delay:
            sleep(delay)
        return execute(watch)

    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="pagewatch") as pool:
        return list(pool.map(run_with_jitter, watches))
