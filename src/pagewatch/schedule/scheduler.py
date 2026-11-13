"""Due selection and bounded-concurrency rounds."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Generic, TypeVar

from ..models import WatchSpec
from .backoff import stagger_offset

T = TypeVar("T")


@dataclass(frozen=True)
class RoundFailure(Generic[T]):
    """Result placeholder when a watch's callback raised unexpectedly.

    Well-behaved callbacks (such as :class:`~pagewatch.runner.Runner.run`)
    model their own failures as ordinary error results, so this marker only
    appears when an exception escapes the callback.  It keeps the round's
    result list aligned with the input order and prevents one failing or
    timing-out watch from aborting the rest of the round.
    """

    watch_name: str
    error: BaseException

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (f"RoundFailure(watch_name={self.watch_name!r}, "
                f"error={type(self.error).__name__}: {self.error})")


def is_due(state: dict[str, object] | None, now: float) -> bool:
    if state is None:
        return True
    return float(state.get("next_due_at", 0.0)) <= now


def select_watches(watches: Iterable[WatchSpec], *, names: Sequence[str] = (),
                   tags: Sequence[str] = (), due_only: bool = False,
                   states: dict[str, dict[str, object]] | None = None,
                   now: float = 0.0) -> list[WatchSpec]:
    """Return the watches that participate in a round, in a stable order.

    Disabled watches and watches excluded by the name/tag/due filters are
    dropped here, before any worker pool is sized, so they never occupy a
    concurrency slot or delay the watches that actually need fetching.
    """
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


def stable_offsets(watches: Sequence[WatchSpec], stagger_seconds: int,
                   seed: int = 0) -> dict[str, int]:
    """Map every watch name to its order-independent intra-round offset.

    The offset is a pure function of the watch name and the configured cap, so
    reversing or otherwise reordering the input leaves each watch's offset and
    request timeline unchanged.
    """
    if stagger_seconds <= 0:
        return {}
    return {
        watch.name: stagger_offset(watch.name, stagger_seconds, seed)
        for watch in watches
    }


def execute_round(watches: Sequence[WatchSpec],
                  execute: Callable[[WatchSpec], T], max_workers: int,
                  *, stagger_seconds: int = 0, seed: int = 0,
                  sleeper: Callable[[float], None] = time.sleep
                  ) -> list[T | RoundFailure[T]]:
    """Run one check per watch without exceeding the global worker cap.

    Concurrency is sized from the *filtered* input only: callers pass the list
    returned by :func:`select_watches`, so disabled or filtered-out watches do
    not consume slots.  When ``stagger_seconds`` is positive each watch waits
    for a deterministic, name-derived offset before its check, spreading load
    on shared targets regardless of the input ordering.  A watch whose
    callback raises is captured as a :class:`RoundFailure` instead of aborting
    the whole round; results keep the input order for reproducible timelines.
    """
    if not watches:
        return []
    ordered = list(watches)
    offsets = stable_offsets(ordered, int(stagger_seconds), seed)
    workers = max(1, min(int(max_workers), len(ordered)))

    def guarded(watch: WatchSpec) -> T | RoundFailure[T]:
        delay = offsets.get(watch.name, 0)
        if delay:
            sleeper(float(delay))
        try:
            return execute(watch)
        except BaseException as exc:  # isolate failures at the round boundary
            return RoundFailure(watch.name, exc)

    if workers == 1 and not offsets:
        # Fast path preserving the original sequential semantics exactly.
        return [execute(watch) for watch in ordered]

    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="pagewatch") as pool:
        return list(pool.map(guarded, ordered))
