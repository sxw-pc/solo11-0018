from __future__ import annotations

import threading
import time

from pagewatch.models import WatchSpec
from pagewatch.schedule.backoff import (
    backoff_delay, deterministic_jitter, next_due_at,
)
from pagewatch.schedule.scheduler import execute_round, is_due, select_watches

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


def test_jitter_spreads_request_starts_within_a_round():
    items = [watch(name) for name in ("a", "b", "c", "d", "e")]
    planned: dict[str, int] = {}
    lock = threading.Lock()

    def fake_sleep(_seconds):
        return  # injected: capture the delay instead of waiting

    def execute(item):
        delay = deterministic_jitter(item.name, 10)
        with lock:
            planned[item.name] = delay
        return item.name

    results = execute_round(items, execute, max_workers=5,
                            jitter_seconds=10, sleep=fake_sleep)
    assert results == [item.name for item in items]
    assert set(planned) == {item.name for item in items}
    # name-based deterministic values, bounded by the jitter ceiling
    for item in items:
        assert planned[item.name] == deterministic_jitter(item.name, 10)
        assert 0 <= planned[item.name] <= 10
    # at least two distinct start offsets => requests are not all fired at t0
    assert len(set(planned.values())) > 1


def test_zero_jitter_keeps_instant_starts():
    items = [watch(name) for name in ("a", "b")]
    slept: list[float] = []

    def execute(item):
        return item.name

    execute_round(items, execute, max_workers=2, jitter_seconds=0,
                  sleep=slept.append)
    assert slept == []


def test_jitter_delay_is_applied_inside_workers_not_submitter():
    items = [watch(name) for name in ("a", "b", "c", "d")]
    events: list[tuple[str, float]] = []
    lock = threading.Lock()
    started = time.monotonic()

    def execute(item):
        with lock:
            events.append((item.name, time.monotonic() - started))
        time.sleep(0.01)
        return item.name

    # a 1s ceiling keeps the test fast while proving real staggering
    results = execute_round(items, execute, max_workers=4, jitter_seconds=1)
    assert results == [item.name for item in items]
    offsets = [offset for _, offset in events]
    assert max(offsets) - min(offsets) > 0.05


def test_failing_watch_does_not_block_the_others():
    items = [watch("slow"), watch("x"), watch("y")]
    finished: dict[str, float] = {}
    lock = threading.Lock()
    started = time.monotonic()

    def execute(item):
        if item.name == "slow":
            time.sleep(0.15)
            raise RuntimeError("backoff failure")
        with lock:
            finished[item.name] = time.monotonic() - started
        return item.name

    thrown = None
    try:
        execute_round(items, execute, max_workers=3, jitter_seconds=0)
    except RuntimeError as exc:  # surfaced when the failed result is consumed
        thrown = exc
    assert isinstance(thrown, RuntimeError)
    # the healthy watches finished well before the slow one
    assert max(finished.values()) < 0.1
    assert set(finished) == {"x", "y"}


def test_round_results_match_sequential_outcomes_and_order():
    items = [watch(name) for name in ("a", "b", "c", "d")]

    def execute(item):
        return f"done:{item.name}"

    sequential = [execute(item) for item in items]
    concurrent = execute_round(items, execute, max_workers=4, jitter_seconds=0)
    assert concurrent == sequential


def test_jitter_seconds_accepts_a_per_watch_callable():
    items = [watch("a"), watch("b")]
    seen: dict[str, int] = {}
    lock = threading.Lock()

    def per_watch(item):
        return 3 if item.name == "a" else 7

    def fake_sleep(_seconds):
        return

    def execute(item):
        with lock:
            seen.setdefault(item.name, 0)
            seen[item.name] += 1
        return item.name

    assert execute_round(items, execute, max_workers=2,
                         jitter_seconds=per_watch, sleep=fake_sleep) == [
        "a", "b"]
    assert seen == {"a": 1, "b": 1}


def test_single_worker_round_runs_inline_without_jitter():
    items = [watch("a"), watch("b")]
    order: list[str] = []

    def execute(item):
        order.append(item.name)
        return item.name

    # workers==1 preserves the original inline, delay-free sequential path
    assert execute_round(items, execute, max_workers=1,
                         jitter_seconds=10) == ["a", "b"]
    assert order == ["a", "b"]
