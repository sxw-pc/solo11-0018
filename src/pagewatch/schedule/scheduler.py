"""Due selection and bounded-concurrency rounds."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

from ..models import WatchSpec

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


def execute_round(watches: Sequence[WatchSpec],
                  execute: Callable[[WatchSpec], T],
                  max_workers: int) -> list[T]:
    """Run one check per watch without exceeding the global worker cap."""
    if not watches:
        return []
    workers = max(1, min(int(max_workers), len(watches)))
    if workers == 1:
        return [execute(watch) for watch in watches]
    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="pagewatch") as pool:
        return list(pool.map(execute, watches))
