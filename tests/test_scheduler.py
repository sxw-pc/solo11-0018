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


def test_demo_round_routes_through_enabled_aware_selection():
    """The offline demo must not hand raw watches straight to execute_round,
    otherwise disabled watches would be scheduled and produce artifacts."""
    import inspect

    from pagewatch import demo

    source = inspect.getsource(demo)
    assert "select_watches" in source
    # No direct execute_round(<raw watches>, ...) call bypassing selection.
    assert "execute_round(watches," not in source
    assert "execute_round(due," in source
