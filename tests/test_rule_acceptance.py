"""End-to-end acceptance: conflict adjudication, replay, and no drift."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import yaml

from pagewatch.config import load_all, effective_rules
from pagewatch.diff.classify import (
    classify_detailed, replay_severity, rule_snapshot, trace_to_json,
)
from pagewatch.models import DiffMetrics, RulePatch, Rules, WatchSpec
from pagewatch.report.digest import render_digest, render_event_json
from pagewatch.runner import Runner
from pagewatch.storage.db import SCHEMA_VERSION, connect, init_paths
from pagewatch.storage.events import (
    get_event, insert_alert, insert_event, list_alerts, list_events,
)

ROOT = Path(__file__).parents[1]


def metric(**overrides):
    values = dict(
        base_lines=10, new_lines=10, added_lines=1, removed_lines=1,
        changed_lines=2, change_rate=0.2, regions=("#main",),
        focus_hit=False, noise_only=False, keyword_hits=(),
    )
    values.update(overrides)
    return DiffMetrics(**values)


def write_three_round_config(tmp_path: Path) -> tuple[Path, Path]:
    """One watch whose keyword rule is disabled, plus an adjudicating cap."""
    watchlist = {
        "version": 1,
        "watches": [{
            "name": "conflict",
            "url": "http://127.0.0.1/never-used",
            "interval_seconds": 60,
            "include_selectors": ["#content"],
            "exclude_selectors": [".ad"],
            "noise_patterns": [r"Visits:\s*\d+"],
            "keywords": ["updated"],
            "rule_overrides": {"rules": [{"id": "keyword-promote",
                                          "enabled": False}]},
        }],
    }
    rules = {
        "version": 1,
        "classification": {"noise_max_change_rate": 0.02,
                           "major_change_rate": 0.35,
                           "focus_selectors": ["#notice"]},
        "rules": [
            # Higher priority than focus: caps a focus+insert change to minor
            # to prove adjudication is by priority, not by if-order.
            {"id": "focus-and-insert-cap", "kind": "severity_mapping",
             "severity": "minor", "priority": 70,
             "when": ["focus-promote", "block-structure-change"]},
        ],
    }
    watch_path = tmp_path / "watchlist.yaml"
    rules_path = tmp_path / "rules.yaml"
    watch_path.write_text(yaml.safe_dump(watchlist, allow_unicode=True),
                          encoding="utf-8")
    rules_path.write_text(yaml.safe_dump(rules, allow_unicode=True),
                          encoding="utf-8")
    return watch_path, rules_path


class SequenceClient:
    """Serves one fixed HTML revision per call; no sockets involved."""

    def __init__(self, pages: list[str]) -> None:
        self.pages = pages
        self.calls = 0

    def fetch(self, url: str, *, etag=None, last_modified=None):
        from pagewatch.fetch.client import FetchResult
        page = self.pages[min(self.calls, len(self.pages) - 1)]
        self.calls += 1
        body = page.encode("utf-8")
        return FetchResult(status_code=200, url=url, body=body, text=page,
                           etag=None, last_modified=None, elapsed_ms=1,
                           not_modified=False)


def fixture_revisions(fixtures_dir: Path) -> list[str]:
    baseline = (fixtures_dir / "baseline.html").read_text(encoding="utf-8")
    round1 = baseline.replace("Documentation paragraph two.",
                              "Documentation paragraph two updated.")
    round2 = round1.replace("Service is stable.", "Service remains stable.")
    round2 = round2.replace('<aside class="ad">',
                            "<p>Critical maintenance required.</p>\n"
                            '<aside class="ad">')
    assert round1 != baseline and round2 != round1
    # Round 3 touches only non-focus blocks, so focus does not fire again.
    round3 = (round2
              .replace("Release notes", "Release notes updated")
              .replace("Documentation paragraph one.",
                       "Documentation paragraph one updated.")
              .replace("Documentation paragraph two updated.",
                       "Documentation paragraph two rewritten.")
              .replace("Critical maintenance required.",
                       "Critical maintenance completed."))
    assert round3 != round2
    return [baseline, round1, round2, round3]


def make_runner(data_dir: Path, rules: Rules, clock) -> Runner:
    from pagewatch.models import DataPaths
    init_paths(DataPaths(data_dir))
    return Runner(db_path=data_dir / "pagewatch.sqlite",
                  archive_dir=data_dir / "archive",
                  outbox_path=data_dir / "outbox" / "alerts.jsonl",
                  rules=rules, clock=clock)


def test_three_round_conflict_resolution_keyword_disable_and_replay(
        tmp_path, fixtures_dir, frozen_clock):
    watch_path, rules_path = write_three_round_config(tmp_path)
    watches, rules = load_all(watch_path, rules_path)
    watch = watches[0]
    data_dir = tmp_path / "data"
    runner = make_runner(data_dir, rules, frozen_clock)
    client = SequenceClient(fixture_revisions(fixtures_dir))
    runner.client_factory = lambda _rules: client

    assert runner.run(watch).outcome == "first"

    # Round 1: only the keyword paragraph changed.  Keyword promotion is
    # disabled for this watch, so it stays minor instead of being upgraded.
    frozen_clock.advance(60)
    first = runner.run(watch)
    assert (first.outcome, first.severity) == ("changed", "minor")
    with connect(data_dir / "pagewatch.sqlite") as connection:
        event = get_event(connection, first.event_id)
        trace = json.loads(event.rule_trace)
        assert trace["final"] == "minor"
        assert replay_severity(event.rule_trace) == "minor"
        keyword = next(item for item in trace["verdicts"]
                       if item["rule_id"] == "keyword-promote")
        assert keyword["matched"] is False
        assert keyword["detail"] == "disabled by override"
        # The watch-scoped disable is recorded with its provenance.
        assert keyword["source"] == "watch:conflict"

    # Round 2: focus region and an inserted block disagree in one round.
    # The cap has priority 70 > focus's 60, so the cap wins: minor.
    frozen_clock.advance(60)
    second = runner.run(watch)
    assert (second.outcome, second.severity) == ("changed", "minor")
    with connect(data_dir / "pagewatch.sqlite") as connection:
        event = get_event(connection, second.event_id)
        trace = json.loads(event.rule_trace)
        matched = {item["rule_id"]: item for item in trace["verdicts"]
                   if item["matched"]}
        assert matched["focus-promote"]["severity"] == "major"
        assert matched["block-structure-change"]["severity"] == "minor"
        assert matched["focus-and-insert-cap"]["severity"] == "minor"
        # Highest-priority match is the winner; the trace is the full record.
        assert max(matched.values(), key=lambda item: item["priority"])[
            "rule_id"] == "focus-and-insert-cap"
        assert replay_severity(event.rule_trace) == "minor"

    # Round 3: a large non-focus rewrite with the keyword still present.
    # Keyword stays disabled, but high-change-rate still promotes to major.
    frozen_clock.advance(60)
    third = runner.run(watch)
    assert (third.outcome, third.severity) == ("changed", "major")
    with connect(data_dir / "pagewatch.sqlite") as connection:
        events = list_events(connection)
        assert [item.severity for item in events] == ["minor", "minor", "major"]
        for event in events:
            assert replay_severity(event.rule_trace) == event.severity
        # Only the major round is alert-eligible; key is watch+final level.
        alerts = list_alerts(connection)
        assert len(alerts) == 1
        assert alerts[0].dedup_key == "conflict:major"
    outbox = data_dir / "outbox" / "alerts.jsonl"
    assert len(outbox.read_text(encoding="utf-8").splitlines()) == 1


def test_priority_conflict_and_deterministic_ties_across_runs():
    """Multiple rules disagree in one round; priority and ties are stable."""
    conflicting = Rules(rules=(
        RulePatch("cap-low-rate", source="global", kind="threshold",
                  severity="noise", priority=80, threshold=0.5, direction="<"),
        RulePatch("aaa-mapping", source="global", kind="severity_mapping",
                  severity="minor", priority=80,
                  when=("focus-promote", "keyword-promote")),
        RulePatch("zzz-threshold", source="global", kind="threshold",
                  severity="minor", priority=80, threshold=0.9, direction="<"),
    ))
    metrics = metric(change_rate=0.1, focus_hit=True, keyword_hits=("重要",))
    first = classify_detailed(metrics, conflicting)
    second = classify_detailed(metrics, conflicting)
    # All three match at priority 80.  The kind order puts severity_mapping
    # ahead of both threshold rules, so the deterministic winner is minor.
    assert first.severity == second.severity == "minor"
    assert [item.as_dict() for item in first.trace] == \
        [item.as_dict() for item in second.trace]
    tied = [item for item in first.trace
            if item.matched and item.priority == 80]
    assert [item.rule_id for item in tied] == \
        ["aaa-mapping", "cap-low-rate", "zzz-threshold"]
    assert replay_severity(trace_to_json(first)) == "minor"
    assert trace_to_json(first) == trace_to_json(second)

    # Remove the mapping and the tie is between two threshold rules; the
    # rule id breaks it (cap-low-rate < zzz-threshold), not definition order.
    thresholds_only = replace(conflicting, rules=tuple(
        item for item in conflicting.rules if item.rule_id != "aaa-mapping"))
    tie = classify_detailed(metrics, thresholds_only)
    tied = [item for item in tie.trace if item.matched]
    assert tied[0].rule_id == "cap-low-rate"
    assert tie.severity == "noise"


def test_historical_events_do_not_drift_when_replayed(tmp_path):
    """Pre-engine rows keep their severity; new engine replays them identically."""
    data_dir = tmp_path / "data"
    paths = init_data(data_dir)
    legacy = metric(change_rate=0.4, focus_hit=False, keyword_hits=("计费",))
    rules = Rules()
    severity = classify_detailed(legacy, rules).severity
    assert severity == "major"
    with connect(paths.db_path) as connection:
        # Simulate a row written before the engine existed: empty trace.
        old = insert_event(
            connection, watch_name="announcements", from_snapshot_id=1,
            to_snapshot_id=2, created_at=1000.0, severity="major",
            change_rate=legacy.change_rate, added_lines=1, removed_lines=1,
            regions=legacy.regions, keyword_hits=legacy.keyword_hits,
            focus_hit=False, summary="major: legacy row", diff_text="",
        )
        connection.commit()
        assert old.rule_trace == ""
        assert old.severity == "major"
        exported = json.loads(render_event_json(list_events(connection)))
    assert exported[0]["severity"] == "major"
    # The fields the event table already exposed are unchanged.
    assert set(exported[0]) == {
        "id", "watch_name", "from_snapshot_id", "to_snapshot_id",
        "created_at", "severity", "change_rate", "added_lines",
        "removed_lines", "regions", "keyword_hits", "focus_hit", "summary",
        "diff_text",
    }
    html = render_digest(list_events(connect(paths.db_path)))
    assert "announcements" in html and "major" in html


def init_data(data_dir: Path):
    from pagewatch.models import DataPaths
    paths = DataPaths(data_dir)
    init_paths(paths)
    return paths


def test_schema_migration_adds_trace_without_touching_old_rows(tmp_path):
    import sqlite3
    db_path = tmp_path / "old.sqlite"
    legacy_schema = """
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_name TEXT NOT NULL, from_snapshot_id INTEGER NOT NULL,
        to_snapshot_id INTEGER NOT NULL, created_at REAL NOT NULL,
        severity TEXT NOT NULL, change_rate REAL NOT NULL,
        added_lines INTEGER NOT NULL, removed_lines INTEGER NOT NULL,
        regions_json TEXT NOT NULL, keyword_hits_json TEXT NOT NULL,
        focus_hit INTEGER NOT NULL DEFAULT 0, summary TEXT NOT NULL,
        diff_text TEXT NOT NULL
    );
    INSERT INTO meta(key, value) VALUES('schema_version', '1');
    INSERT INTO events(watch_name, from_snapshot_id, to_snapshot_id,
        created_at, severity, change_rate, added_lines, removed_lines,
        regions_json, keyword_hits_json, focus_hit, summary, diff_text)
    VALUES('legacy', 1, 2, 1000.0, 'major', 0.4, 1, 1, '[]', '[]', 0,
           'old summary', '');
    """
    with sqlite3.connect(str(db_path)) as connection:
        connection.executescript(legacy_schema)
        connection.commit()
    from pagewatch.storage.db import init_db
    init_db(db_path)
    with connect(db_path) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] \
            == str(SCHEMA_VERSION)
        row = connection.execute(
            "SELECT severity, rule_trace_json, summary FROM events").fetchone()
        assert tuple(row) == ("major", "", "old summary")
        event = get_event(connection, 1)
        assert event is not None and event.severity == "major"


def test_alert_cooldown_still_keys_on_watch_and_final_level(tmp_path):
    from pagewatch.models import DataPaths
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    now = 1_767_225_600.0
    with connect(paths.db_path) as connection:
        first = insert_alert(connection, watch_name="conflict", severity="minor",
                             event_id=1, created_at=now,
                             message="first", cooldown_seconds=600)
        assert first is not None and first.dedup_key == "conflict:minor"
        duplicate = insert_alert(connection, watch_name="conflict",
                                 severity="minor", event_id=2,
                                 created_at=now + 300, message="dup",
                                 cooldown_seconds=600)
        assert duplicate is None
        promoted = insert_alert(connection, watch_name="conflict",
                                severity="major", event_id=3,
                                created_at=now + 300, message="promoted",
                                cooldown_seconds=600)
        # A different final level is a different cooldown key.
        assert promoted is not None and promoted.dedup_key == "conflict:major"


def test_digest_renders_rule_trace_and_handles_legacy_rows(tmp_path):
    from pagewatch.models import DataPaths
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    classification = classify_detailed(
        metric(change_rate=0.8, focus_hit=True), Rules())
    with connect(paths.db_path) as connection:
        traced = insert_event(
            connection, watch_name="a", from_snapshot_id=1, to_snapshot_id=2,
            created_at=1000.0, severity=classification.severity,
            change_rate=0.8, added_lines=1, removed_lines=1,
            regions=("#notice",), keyword_hits=(), focus_hit=True,
            summary="major", diff_text="--- previous\n+++ current\n",
            rule_trace=trace_to_json(classification),
        )
        insert_event(connection, watch_name="b", from_snapshot_id=1,
                     to_snapshot_id=2, created_at=1001.0, severity="noise",
                     change_rate=0.0, added_lines=0, removed_lines=0,
                     regions=(), keyword_hits=(), focus_hit=False,
                     summary="noise", diff_text="")
        connection.commit()
        events = list_events(connection)
    html = render_digest(events, generated_at="2026-01-01T00:05:00Z")
    assert "Rule trace (3 matched, final major)" in html
    assert "focus-promote" in html
    # Legacy rows without a trace render normally, without an empty widget.
    assert "<code></code>" not in html
    assert render_digest(events, generated_at="2026-01-01T00:05:00Z") == html
    assert traced.id == 1


def test_watch_keyword_disable_does_not_disable_other_rules():
    watch = WatchSpec(name="w", url="http://127.0.0.1/w", interval_seconds=60,
                      rules_overrides={"rules": (
                          RulePatch("keyword-promote", source="watch:w",
                                    enabled=False),
                      )})
    merged = effective_rules(Rules(), watch)
    snapshot = {item["id"]: item for item in rule_snapshot(merged)}
    assert snapshot["keyword-promote"]["enabled"] is False
    assert snapshot["focus-promote"]["enabled"] is True
    assert snapshot["high-change-rate"]["enabled"] is True
    assert classify_detailed(metric(change_rate=0.001, keyword_hits=("x",)),
                             merged).severity == "noise"
    assert classify_detailed(metric(change_rate=0.001, keyword_hits=("x",),
                                    focus_hit=True), merged).severity == "major"
    assert classify_detailed(metric(change_rate=0.9, keyword_hits=("x",)),
                             merged).severity == "major"
