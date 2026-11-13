"""Regression tests for the three reported cross-round defects.

Covers the full Runner/Scheduler pipeline with in-process scripted fetch
clients (no sockets), a FrozenClock and an isolated tmp data directory:

1. fetch failure degrades only that watch for that round -- no phantom
   change/event, no snapshot write, no digest pollution, other watches advance;
2. disabled watches are never scheduled and never produce artifacts;
3. alert cooldown is scoped exactly to (watch, severity), suppresses repeats
   inside the window and recovers at/after expiry across rounds.
"""

from __future__ import annotations

from dataclasses import replace

from pagewatch.alert.outbox import read_outbox
from pagewatch.clock import FrozenClock
from pagewatch.fetch.client import FetchError, FetchResult
from pagewatch.models import DataPaths, Rules, WatchSpec
from pagewatch.report.digest import render_digest
from pagewatch.runner import Runner
from pagewatch.schedule.scheduler import execute_round, select_watches
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import list_alerts, list_events
from pagewatch.storage.snapshots import list_snapshots, probe_rows


def make_watch(name, *, enabled=True):
    return WatchSpec(name=name, url=f"http://127.0.0.1/{name}",
                     interval_seconds=60, enabled=enabled)


class ScriptedClient:
    """Fetch client whose per-URL behavior is driven by an external script.

    ``script`` maps a URL to a list of actions consumed in call order. An
    action is either a FetchError (raised) or an HTML string (returned 200).
    When the list is exhausted the last action is reused.
    """

    def __init__(self, script: dict[str, list]):
        self.script = {url: list(actions) for url, actions in script.items()}
        self.calls: dict[str, int] = {}

    def fetch(self, url, *, etag=None, last_modified=None):
        actions = self.script[url]
        index = min(self.calls.get(url, 0), len(actions) - 1)
        self.calls[url] = self.calls.get(url, 0) + 1
        action = actions[index]
        if isinstance(action, Exception):
            raise action
        return FetchResult(status_code=200, url=url, body=action.encode("utf-8"),
                           text=action, etag=None, last_modified=None,
                           elapsed_ms=1, not_modified=False)


def big_page(revision: int) -> str:
    # Enough distinct blocks for every revision to classify as a major change
    # under a low major threshold.
    return "<html><body>" + "".join(
        f"<p>paragraph {i} revision {revision}</p>" for i in range(20)
    ) + "</body></html>"


def build_runner(tmp_path, client, *, clock, cooldown=3600,
                 major_rate=0.01, max_workers=1):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    rules = Rules(jitter_seconds=0, alert_cooldown_seconds=cooldown,
                  major_change_rate=major_rate, max_workers=max_workers)
    runner = Runner(db_path=paths.db_path, archive_dir=paths.archive_dir,
                    outbox_path=paths.outbox_path, rules=rules, clock=clock,
                    client_factory=lambda _rules: client)
    return paths, runner, rules


def round_once(runner, watches, clock, rules):
    selected = select_watches(watches, now=clock.now())
    return selected, execute_round(selected, runner.run, rules.max_workers)


# --------------------------------------------------------------------------- #
# Scenario 1: fetch failure degrades one watch without phantom changes.
# --------------------------------------------------------------------------- #

def test_fetch_failure_creates_no_phantom_event_and_keeps_other_watches(tmp_path):
    clock = FrozenClock(1_000.0)
    wa = make_watch("alpha")
    wb = make_watch("beta")
    failure = FetchError("HTTP 503 for http://127.0.0.1/alpha")
    client = ScriptedClient({
        wa.url: ["<html><body><p>alpha v1</p></body></html>",
                 failure,
                 "<html><body><p>alpha v1</p></body></html>"],
        # beta changes for real on the same round alpha fails
        wb.url: ["<html><body><p>beta v1</p></body></html>",
                 big_page(2), big_page(2)],
    })
    paths, runner, rules = build_runner(tmp_path, client, clock=clock)

    # Round 0: baselines.
    clock.advance(60)
    _, results = round_once(runner, [wa, wb], clock, rules)
    assert {r.outcome for r in results} == {"first"}

    # Round 1: alpha 5xx, beta real change.
    clock.advance(60)
    selected, results = round_once(runner, [wa, wb], clock, rules)
    by_name = {r.watch_name: r for r in results}
    assert by_name["alpha"].outcome == "error"
    assert by_name["alpha"].event_id is None
    assert by_name["alpha"].snapshot_id is None
    assert by_name["beta"].outcome == "changed"
    assert by_name["beta"].event_id is not None

    with connect(paths.db_path) as connection:
        # No phantom event/snapshot for the failed watch; only its baseline.
        assert len(list_snapshots(connection, "alpha")) == 1
        alpha_events = [e for e in list_events(connection)
                        if e.watch_name == "alpha"]
        assert alpha_events == []
        # Failure is still observable as a probe + fetch-state error.
        alpha_probes = probe_rows(connection, "alpha")
        assert len(alpha_probes) == 2
        assert alpha_probes[-1]["status_code"] is None
        assert alpha_probes[-1]["error"]
        # The healthy watch's genuine event is the only one on record.
        events = list_events(connection)
        assert [e.watch_name for e in events] == ["beta"]

        # The digest groups only real events -- the failed watch never appears.
        html = render_digest(events)
        assert "alpha" not in html
        assert "beta" in html

    # Round 2: alpha recovers with unchanged content -> still no event.
    clock.advance(60)
    _, results = round_once(runner, [wa, wb], clock, rules)
    by_name = {r.watch_name: r for r in results}
    assert by_name["alpha"].outcome == "unchanged"
    assert by_name["alpha"].event_id is None
    with connect(paths.db_path) as connection:
        assert len(list_snapshots(connection, "alpha")) == 1
        assert [e.watch_name for e in list_events(connection)] == ["beta"]


def test_fetch_failure_does_not_poison_validators_or_block_the_round(tmp_path):
    clock = FrozenClock(1_000.0)
    wa = make_watch("alpha")
    wb = make_watch("beta")
    failure = FetchError("HTTP 500 for http://127.0.0.1/alpha")
    client = ScriptedClient({
        wa.url: [big_page(1), failure, big_page(3)],
        wb.url: [big_page(1), big_page(2), big_page(3)],
    })
    paths, runner, rules = build_runner(tmp_path, client, clock=clock)

    clock.advance(60)
    round_once(runner, [wa, wb], clock, rules)
    alpha_before = client.calls[wa.url]

    clock.advance(60)
    _, results = round_once(runner, [wa, wb], clock, rules)
    # One bad watch must not prevent the other watch from being executed.
    assert len(results) == 2
    assert {r.watch_name for r in results} == {"alpha", "beta"}

    clock.advance(60)
    round_once(runner, [wa, wb], clock, rules)
    # Every scheduled watch was still attempted on each round.
    assert client.calls[wa.url] == alpha_before + 2
    with connect(paths.db_path) as connection:
        # alpha has baseline + recovered snapshot, but no event for the gap
        # covered by the failed round.
        alpha_events = [e for e in list_events(connection)
                        if e.watch_name == "alpha"]
        assert len(alpha_events) == 1  # v1 -> v3 only, nothing phantom


# --------------------------------------------------------------------------- #
# Scenario 2: disabled watches never run and never leave artifacts.
# --------------------------------------------------------------------------- #

def test_disabled_watch_is_skipped_in_selection_and_round(tmp_path):
    clock = FrozenClock(1_000.0)
    enabled_w = make_watch("on", enabled=True)
    disabled_w = make_watch("off", enabled=False)
    watches = [enabled_w, disabled_w]
    client = ScriptedClient({
        enabled_w.url: [big_page(1)],
        disabled_w.url: [big_page(1)],
    })
    paths, runner, rules = build_runner(tmp_path, client, clock=clock)

    for _ in range(3):
        clock.advance(60)
        selected, results = round_once(runner, watches, clock, rules)
        assert [w.name for w in selected] == ["on"]
        assert [r.watch_name for r in results] == ["on"]

    assert disabled_w.url not in client.calls  # never even attempted a fetch
    with connect(paths.db_path) as connection:
        assert list_snapshots(connection, "off") == []
        assert probe_rows(connection, "off") == []
        assert [e.watch_name for e in list_events(connection)
                if e.watch_name == "off"] == []
        stored = {row[0] for row in
                  connection.execute("SELECT name FROM watches")}
        scheduled = {row[0] for row in
                     connection.execute("SELECT watch_name FROM schedule_state")}
        assert "off" not in stored and "off" not in scheduled
        html = render_digest(list_events(connection))
        # The disabled watch must not get its own digest group (the header
        # word "offline" is unrelated, so match the group heading markup).
        assert "<h2>off" not in html


def test_disabled_watch_cannot_be_resurrected_by_name_or_due_state(tmp_path):
    clock = FrozenClock(1_000.0)
    disabled_w = make_watch("off", enabled=False)
    enabled_w = make_watch("on", enabled=True)
    watches = [disabled_w, enabled_w]
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    from pagewatch.storage.db import connect as _connect
    from pagewatch.storage.snapshots import upsert_schedule
    with _connect(paths.db_path) as connection:
        # A stale "due" schedule row left over from when the watch was enabled.
        upsert_schedule(connection, watch_name="off", next_due_at=clock.now() - 1,
                        consecutive_failures=0, last_run_at=0,
                        last_outcome="unchanged", updated_at=0)
        connection.commit()
    from pagewatch.storage.snapshots import list_schedules
    with _connect(paths.db_path) as connection:
        states = list_schedules(connection)
    assert select_watches(watches, due_only=True, states=states,
                          now=clock.now()) == [enabled_w]
    assert select_watches(watches, names=["off"], now=clock.now()) == []
    assert select_watches(watches, tags=(), now=clock.now()) == [enabled_w]


# --------------------------------------------------------------------------- #
# Scenario 3: cooldown scoped to (watch, severity), recovers across rounds.
# --------------------------------------------------------------------------- #

def test_cooldown_suppresses_repeats_and_recovers_across_rounds(tmp_path):
    clock = FrozenClock(1_800_000_000.0)
    wa = make_watch("alpha")
    wb = make_watch("beta")
    # Each fetch returns fresh major content so every round is a change.
    revision = {"n": 0}

    class AlwaysChanging(ScriptedClient):
        def fetch(self, url, *, etag=None, last_modified=None):
            revision["n"] += 1
            return FetchResult(200, url, big_page(revision["n"]).encode(),
                               big_page(revision["n"]), None, None, 1, False)

    client = AlwaysChanging({})
    cooldown = 3600
    paths, runner, rules = build_runner(tmp_path, client, clock=clock,
                                        cooldown=cooldown)

    def run_round():
        selected, results = round_once(runner, [wa, wb], clock, rules)
        return {r.watch_name: r for r in results}

    # Round 0: first snapshots, no alerts yet.
    clock.advance(60)
    first = run_round()
    assert all(r.outcome == "first" for r in first.values())

    # Round 1 at +600: first major changes -> both alert and hit the outbox.
    clock.advance(540)
    changed = run_round()
    assert all(r.severity == "major" and r.alert_id is not None
               for r in changed.values())
    assert len(read_outbox(paths.outbox_path)) == 2

    # Rounds 2..6 stay inside the rolling 3600s window: changes are recorded
    # but no duplicate alerts are emitted to the JSONL outbox.
    for _ in range(5):
        clock.advance(600)
        suppressed = run_round()
        assert all(r.severity == "major" and r.alert_id is None
                   for r in suppressed.values())
    assert len(read_outbox(paths.outbox_path)) == 2  # unchanged

    # Next round lands exactly on cooldown expiry relative to the first alert:
    # 60 (r0) + 540 (r1) + 6*600 (r2..r7) == 4200 elapsed; first alert was at
    # +600, so we are now at +3600 from it -> must recover.
    clock.advance(600)
    recovered = run_round()
    assert all(r.severity == "major" and r.alert_id is not None
               for r in recovered.values())
    outbox = read_outbox(paths.outbox_path)
    assert len(outbox) == 4  # one new alert per watch, no duplicates

    with connect(paths.db_path) as connection:
        alerts = list_alerts(connection)
        keys = {(a.watch_name, a.severity) for a in alerts}
        # Cooldown keys are exactly watch+severity; the two watches and the
        # two recovered rounds are independently represented.
        assert keys == {("alpha", "major"), ("beta", "major")}
        for watch_name in ("alpha", "beta"):
            times = sorted(a.created_at for a in alerts
                           if a.watch_name == watch_name)
            assert times[1] - times[0] == cooldown  # precise recovery point


def test_cooldown_is_independent_between_watches_and_severities(tmp_path):
    clock = FrozenClock(1_800_000_000.0)
    wa = make_watch("alpha")
    # A second watch whose override uses a different (shorter) cooldown.
    wb = replace(make_watch("beta"),
                 rules_overrides={"alert_cooldown_seconds": 60})
    client = ScriptedClient({wa.url: [big_page(1)], wb.url: [big_page(1)]})
    from pagewatch.models import DataPaths as _Paths
    data = _Paths(tmp_path / "data")
    init_paths(data)
    rules = Rules(jitter_seconds=0, alert_cooldown_seconds=3600,
                  major_change_rate=0.01)
    runner = Runner(db_path=data.db_path, archive_dir=data.archive_dir,
                    outbox_path=data.outbox_path, rules=rules, clock=clock,
                    client_factory=lambda _r: client)
    # Baseline snapshots (outcome "first", no alerts).
    clock.advance(60)
    execute_round(select_watches([wa, wb], now=clock.now()), runner.run, 1)
    # First real change -> each watch emits its baseline major alert.
    client.script[wa.url].append(big_page(2))
    client.script[wb.url].append(big_page(2))
    clock.advance(60)
    baseline = {r.watch_name: r for r in execute_round(
        select_watches([wa, wb], now=clock.now()), runner.run, 1)}
    assert baseline["alpha"].alert_id is not None
    assert baseline["beta"].alert_id is not None
    # Another change 120s later: past beta's 60s cooldown, inside alpha's 3600s.
    client.script[wa.url].append(big_page(3))
    client.script[wb.url].append(big_page(3))
    clock.advance(120)
    results = {r.watch_name: r for r in execute_round(
        select_watches([wa, wb], now=clock.now()), runner.run, 1)}
    assert results["alpha"].alert_id is None      # still cooling (3600)
    assert results["beta"].alert_id is not None   # recovered (60 elapsed)
