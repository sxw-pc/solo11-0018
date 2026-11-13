"""End-to-end scheduling behavior: staggering, cooldown, slots, isolation.

These tests drive the real Runner against a loopback Flask server whose body
changes on every request, so cooldown windows can be exercised across rounds
without depending on wall-clock time (a FrozenClock is injected).
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from flask import Flask
from werkzeug.serving import make_server

from pagewatch.alert.outbox import read_outbox
from pagewatch.clock import FrozenClock
from pagewatch.models import DataPaths, Rules, WatchSpec
from pagewatch.runner import Runner
from pagewatch.schedule.scheduler import (
    RoundFailure, execute_round, select_watches, stable_offsets,
)
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import list_alerts


class ChangingServer:
    """Loopback server: one focus page that changes every request."""

    def __init__(self) -> None:
        self.counter = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()
        app = Flask(__name__)

        @app.get("/changing")
        def changing():  # pragma: no cover - exercised over HTTP
            with self._lock:
                self.counter += 1
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                n = self.counter
            time.sleep(0.02)
            with self._lock:
                self.in_flight -= 1
            return (
                '<html><body><div id="main-notice">'
                f"重要 计费 变更编号{n} "
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                "</div></body></html>"
            )

        @app.get("/stable/<name>")
        def stable(name: str):  # pragma: no cover - exercised over HTTP
            return f"<html><body><main>{name} stable unchanged body</main></body></html>"

        self._server = make_server("127.0.0.1", 0, app, threaded=True)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self) -> "ChangingServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)


@pytest.fixture
def changing_server():
    server = ChangingServer().start()
    try:
        yield server
    finally:
        server.stop()


def _runner(paths: Path, rules: Rules, clock: FrozenClock) -> Runner:
    return Runner(db_path=paths.db_path, archive_dir=paths.archive_dir,
                  outbox_path=paths.outbox_path, rules=rules, clock=clock)


def test_timeline_is_reproducible_and_order_independent(
        tmp_path, changing_server):
    rules = Rules(jitter_seconds=10, max_workers=2,
                  focus_selectors=("#main-notice",))
    watches = [
        WatchSpec(name=f"watch-{n}",
                  url=f"{changing_server.base_url}/stable/{n}",
                  interval_seconds=60)
        for n in range(4)
    ]

    def captured_timeline(ordered):
        paths = DataPaths(tmp_path / f"run-{len(ordered)}-{id(ordered)}")
        init_paths(paths)
        clock = FrozenClock(1_767_225_600.0)
        runner = _runner(paths, rules, clock)
        sleeps: list[float] = []
        execute_round(ordered, runner.run, rules.max_workers,
                      stagger_seconds=rules.jitter_seconds,
                      sleeper=sleeps.append)
        return sleeps

    forward = captured_timeline(list(watches))
    reversed_list = captured_timeline(list(reversed(watches)))
    # Same per-name offset multiset regardless of input ordering.
    assert sorted(forward) == sorted(reversed_list)
    expected = stable_offsets(watches, rules.jitter_seconds)
    assert sorted(forward) == sorted(expected.values())
    # Repeating the identical run yields an identical timeline.
    again = captured_timeline(list(watches))
    assert again == forward


def test_per_watch_cooldown_override_priority_and_persistence(
        tmp_path, changing_server):
    rules = Rules(alert_cooldown_seconds=3600, jitter_seconds=0,
                  max_workers=1, focus_selectors=("#main-notice",),
                  major_change_rate=0.1)
    watch = WatchSpec(
        name="changing", url=f"{changing_server.base_url}/changing",
        interval_seconds=60, include_selectors=("#main-notice",),
        keywords=("重要",),
        rules_overrides={"alert_cooldown_seconds": 60},
    )
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    clock = FrozenClock(1_767_225_600.0)
    runner = _runner(paths, rules, clock)

    execute_round([watch], runner.run, 1)  # baseline snapshot, no alert
    clock.advance(60)
    execute_round([watch], runner.run, 1)  # first major -> alert #1
    # 100s later: outside the 60s override but inside the 3600s global window.
    clock.advance(100)
    execute_round([watch], runner.run, 1)  # override allows alert #2
    # 30s later: inside the override window -> suppressed, no outbox duplicate.
    clock.advance(30)
    execute_round([watch], runner.run, 1)

    with connect(paths.db_path) as connection:
        alerts = list_alerts(connection)
    assert [(item.watch_name, item.severity) for item in alerts] == [
        ("changing", "major"), ("changing", "major")]
    outbox = read_outbox(paths.outbox_path)
    assert len(outbox) == 2

    # A fresh Runner over the same database keeps the cooldown state.
    runner_two = _runner(paths, rules, clock)
    clock.advance(10)
    execute_round([watch], runner_two.run, 1)
    with connect(paths.db_path) as connection:
        assert len(list_alerts(connection)) == 2
    assert len(read_outbox(paths.outbox_path)) == 2


def test_disabled_and_filtered_watches_do_not_consume_slots(
        tmp_path, changing_server):
    rules = Rules(max_workers=2, jitter_seconds=0,
                  focus_selectors=("#main-notice",))
    watches = [
        WatchSpec(name="keep-a", url=f"{changing_server.base_url}/stable/a",
                  interval_seconds=60, tags=("t",)),
        WatchSpec(name="keep-b", url=f"{changing_server.base_url}/stable/b",
                  interval_seconds=60, tags=("t",)),
        WatchSpec(name="off-t", url=f"{changing_server.base_url}/stable/c",
                  interval_seconds=60, tags=("t",), enabled=False),
        WatchSpec(name="other-tag",
                  url=f"{changing_server.base_url}/stable/d",
                  interval_seconds=60, tags=("u",)),
    ]
    selected = select_watches(watches, tags=["t"])
    assert [item.name for item in selected] == ["keep-a", "keep-b"]

    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    clock = FrozenClock(1_767_225_600.0)
    runner = _runner(paths, rules, clock)
    results = execute_round(selected, runner.run, rules.max_workers)
    assert [item.watch_name for item in results] == ["keep-a", "keep-b"]
    assert changing_server.max_in_flight <= 2


def test_round_survives_a_callback_raising():
    watches = [
        WatchSpec(name="ok", url="http://127.0.0.1/ok", interval_seconds=60),
        WatchSpec(name="boom", url="http://127.0.0.1/boom",
                  interval_seconds=60),
    ]

    def callback(watch: WatchSpec):
        if watch.name == "boom":
            raise RuntimeError("kaboom")
        return watch.name

    results = execute_round(watches, callback, max_workers=2,
                            stagger_seconds=0)
    assert results[0] == "ok"
    assert isinstance(results[1], RoundFailure)
    assert results[1].watch_name == "boom"
