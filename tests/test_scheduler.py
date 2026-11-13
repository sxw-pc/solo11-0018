from __future__ import annotations

import threading
import time

from pagewatch.models import WatchSpec
from pagewatch.schedule.backoff import (
    backoff_delay, deterministic_jitter, next_due_at, stagger_offset,
)
from pagewatch.schedule.scheduler import (
    RoundFailure, execute_round, is_due, select_watches, stable_offsets,
)


def watch(name, tags=(), enabled=True):
    return WatchSpec(name=name, url=f"http://127.0.0.1/{name}",
                     interval_seconds=60, tags=tuple(tags), enabled=enabled)


def test_jitter_is_stable_bounded_and_name_specific():
    first = deterministic_jitter("alpha", 10, seed=7)
    assert first == deterministic_jitter("alpha", 10, seed=7)
    assert 0 <= first <= 10
    assert deterministic_jitter("alpha", 0) == 0
    assert deterministic_jitter("alpha", 10, 7) != deterministic_jitter(
        "beta", 10, 7)


def test_stagger_offset_is_name_derived_and_order_independent():
    names = ["alpha", "beta", "gamma", "delta"]
    forward = [watch(name) for name in names]
    reversed_list = [watch(name) for name in reversed(names)]
    offsets_forward = stable_offsets(forward, 10)
    offsets_reversed = stable_offsets(reversed_list, 10)
    # Same name -> same offset no matter where it sits in the input list.
    assert offsets_forward == offsets_reversed
    assert all(0 <= value <= 10 for value in offsets_forward.values())
    assert stable_offsets(forward, 10) == stable_offsets(forward, 10)
    assert offsets_forward["alpha"] == stagger_offset("alpha", 10)
    # Zero cap disables staggering entirely.
    assert stable_offsets(forward, 0) == {}


def test_execute_round_applies_per_watch_stagger_and_keeps_order():
    items = [watch(name) for name in ("a", "b", "c", "d")]
    offsets = stable_offsets(items, 10)
    sleeps: list[float] = []
    results = execute_round(items, lambda item: item.name, max_workers=2,
                            stagger_seconds=10, sleeper=sleeps.append)
    assert results == ["a", "b", "c", "d"]
    # Each watch waited for its own name-derived offset.
    assert sorted(sleeps) == sorted(offsets.values())
    # Reversed input keeps each name's offset unchanged.
    reversed_items = list(reversed(items))
    reversed_sleeps: list[float] = []
    execute_round(reversed_items, lambda item: item.name, max_workers=2,
                  stagger_seconds=10, sleeper=reversed_sleeps.append)
    assert sorted(reversed_sleeps) == sorted(sleeps)


def test_zero_stagger_never_sleeps():
    items = [watch(name) for name in ("a", "b")]
    sleeps: list[float] = []
    execute_round(items, lambda item: item.name, max_workers=2,
                  stagger_seconds=0, sleeper=sleeps.append)
    assert sleeps == []


def test_exponential_backoff_is_capped():
    assert backoff_delay(60, 0) == 60
    assert backoff_delay(60, 1) == 120
    assert backoff_delay(60, 3) == 480
    assert backoff_delay(60, 99) == 3600
    assert next_due_at(now=1000, interval_seconds=60, failures=0,
                       succeeded=True, watch_name="a", jitter_seconds=0) == 1060
    assert next_due_at(now=1000, interval_seconds=60, failures=3,
                       succeeded=False, watch_name="a", jitter_seconds=5) >= 1480


def test_due_selection_by_name_tag_and_schedule():
    watches = [watch("a", ["docs"]), watch("b", ["notice"]),
               watch("c", ["docs"], enabled=False)]
    states = {"a": {"next_due_at": 500}, "b": {"next_due_at": 2000}}
    assert [w.name for w in select_watches(watches)] == ["a", "b"]
    assert [w.name for w in select_watches(watches, names=["b"])] == ["b"]
    assert [w.name for w in select_watches(watches, tags=["docs"])] == ["a"]
    assert [w.name for w in select_watches(
        watches, due_only=True, states=states, now=1000)] == ["a"]
    assert is_due(None, 1000) is True
    assert is_due({"next_due_at": 2000}, 1000) is False


def test_execute_round_honors_worker_cap_and_order():
    active = {"now": 0, "max": 0}
    lock = threading.Lock()

    def execute(item):
        with lock:
            active["now"] += 1
            active["max"] = max(active["max"], active["now"])
        time.sleep(0.02)
        with lock:
            active["now"] -= 1
        return item.name

    items = [watch(name) for name in ("a", "b", "c", "d")]
    assert execute_round(items, execute, max_workers=2) == ["a", "b", "c", "d"]
    assert active["max"] <= 2
    assert execute_round([], execute, 2) == []


def test_single_failure_is_isolated_and_does_not_abort_round():
    def execute(item):
        if item.name == "boom":
            raise RuntimeError("fetch exploded")
        return item.name

    items = [watch("a"), watch("boom"), watch("c"), watch("d")]
    results = execute_round(items, execute, max_workers=2)
    # Successful watches still return in order; the failure is a marker.
    assert [getattr(result, "watch_name", result) for result in results] == [
        "a", "boom", "c", "d"]
    failure = results[1]
    assert isinstance(failure, RoundFailure)
    assert isinstance(failure.error, RuntimeError)


def test_disabled_and_filtered_watches_never_occupy_worker_slots():
    # 3 enabled +tag, 1 disabled +tag, 1 enabled other tag, 1 disabled other.
    watches = [watch("keep-a", ["t"]), watch("keep-b", ["t"]),
               watch("keep-c", ["t"]), watch("off-t", ["t"], enabled=False),
               watch("other", ["u"]), watch("off-u", ["u"], enabled=False)]
    selected = select_watches(watches, tags=["t"])
    assert [item.name for item in selected] == ["keep-a", "keep-b", "keep-c"]

    active = {"now": 0, "max": 0}
    lock = threading.Lock()

    def execute(item):
        with lock:
            active["now"] += 1
            active["max"] = max(active["max"], active["now"])
        time.sleep(0.02)
        with lock:
            active["now"] -= 1
        return item.name

    results = execute_round(selected, execute, max_workers=2)
    assert results == ["keep-a", "keep-b", "keep-c"]
    # Only the 3 selected watches ran, never exceeding the cap of 2.
    assert active["max"] == 2


def test_explicit_name_still_excludes_disabled_watch():
    watches = [watch("on", ["t"]), watch("off", ["t"], enabled=False)]
    assert [item.name for item in
            select_watches(watches, names=["off"], tags=["t"])] == []
    assert [item.name for item in select_watches(watches, names=["on"])] == ["on"]
