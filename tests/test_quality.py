"""Tests for the round-level quality gate: checks, quarantine, rebaseline."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace

import pytest

from pagewatch.alert.outbox import append_alert, read_outbox
from pagewatch.models import Rules, DataPaths, WatchSpec
from pagewatch.quality.checks import (
    check_alert_pairing, check_event_references, check_major_retention,
    check_outbox_pairing, check_snapshot_chain, run_all_checks,
)
from pagewatch.quality.models import (
    CHECK_ALERT_PAIRING, CHECK_EVENT_REFERENCES, CHECK_MAJOR_RETENTION,
    CHECK_OUTBOX_PAIRING, CHECK_SNAPSHOT_CHAIN, FAIL, PASS, UNKNOWN,
    WATCH_QUARANTINED, WATCH_REBASELINED,
)
from pagewatch.quality.round import verify_round
from pagewatch.quality.store import TableUnavailable
from pagewatch.runner import Runner
from pagewatch.storage.db import (
    connect, connect_read_only, init_db, init_paths,
)
from pagewatch.storage.events import insert_alert, insert_event
from pagewatch.storage.snapshots import append_snapshot

NOW = 1_800_000_000.0
WATCH = "docs"


def _snapshot(connection, *, watch_name=WATCH, version=None, text=None,
              major=False):
    marker = text or f"body-{version}"
    snapshot = append_snapshot(
        connection, watch_name=watch_name, fetched_at=NOW, status_code=200,
        url=f"http://127.0.0.1/{watch_name}", etag=None, last_modified=None,
        content_sha256=f"hash-{watch_name}-{marker}", raw_sha256=f"raw-{marker}",
        normalized_text=marker, blocks_json="[]",
        normalizer_version="normalizer-v1",
    )
    if major:
        connection.execute("UPDATE snapshots SET is_major=1 WHERE id=?",
                           (snapshot.id,))
    return snapshot


def _event(connection, snapshot_a, snapshot_b, *, watch_name=WATCH,
           severity="minor"):
    return insert_event(
        connection, watch_name=watch_name,
        from_snapshot_id=snapshot_a.id, to_snapshot_id=snapshot_b.id,
        created_at=NOW, severity=severity, change_rate=0.5,
        added_lines=1, removed_lines=1, regions=("body",),
        keyword_hits=(), focus_hit=False, summary="summary", diff_text="diff",
    )


def _fresh_db(tmp_path):
    db_path = tmp_path / "pagewatch.sqlite"
    init_db(db_path)
    return db_path


def _healthy_chain(connection, count=3, *, watch_name=WATCH):
    snapshots = [_snapshot(connection, watch_name=watch_name, version=i + 1)
                 for i in range(count)]
    for previous, current in zip(snapshots, snapshots[1:]):
        _event(connection, previous, current, watch_name=watch_name)
    connection.commit()
    return snapshots


# ---------------------------------------------------------------- checks

def test_snapshot_chain_detects_gap_and_non_adjacent_event(tmp_path):
    db_path = _fresh_db(tmp_path)
    with connect(db_path) as connection:
        first = _snapshot(connection, version=1)
        second = _snapshot(connection, version=2)
        third = _snapshot(connection, version=3)
        # Healthy adjacency between v1 and v2 ...
        _event(connection, first, second)
        # ... then remove v2: a version gap appears and the event dangles
        # (the dangling reference itself is covered by the reference check).
        connection.execute("DELETE FROM snapshots WHERE id=?", (second.id,))
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_snapshots, load_events
            snapshots = load_snapshots(ro)
            events = load_events(ro)

    chain = check_snapshot_chain(
        snapshots, watch_names={WATCH}, events=events,
        rebaseline_versions={})
    assert chain.status == FAIL
    assert {finding.code for finding in chain.findings
            if finding.severity == FAIL} == {"snapshot_version_gap"}

    # A non-adjacent event whose endpoints both still exist is detected too.
    with connect(db_path) as connection:
        connection.execute(
            "INSERT INTO snapshots(watch_name, version, fetched_at, "
            "status_code, url, content_sha256, normalized_text, blocks_json, "
            "normalizer_version) VALUES(?, 2, ?, 200, ?, ?, ?, '[]', ?)",
            (WATCH, NOW, f"http://127.0.0.1/{WATCH}", "restored", "restored",
             "normalizer-v1"))
        jump = insert_event(
            connection, watch_name=WATCH, from_snapshot_id=first.id,
            to_snapshot_id=third.id, created_at=NOW + 1, severity="minor",
            change_rate=0.2, added_lines=1, removed_lines=1, regions=(),
            keyword_hits=(), focus_hit=False, summary="jump", diff_text="d")
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_snapshots, load_events
            chain = check_snapshot_chain(
                load_snapshots(ro), watch_names={WATCH},
                events=load_events(ro), rebaseline_versions={})
    adjacency = [f for f in chain.findings
                 if f.code == "event_chain_not_adjacent"]
    assert adjacency and adjacency[0].evidence["event_id"] == jump.id


def test_event_reference_check_flags_missing_and_cross_watch(tmp_path):
    db_path = _fresh_db(tmp_path)
    with connect(db_path) as connection:
        a1 = _snapshot(connection, watch_name="a", version=1)
        a2 = _snapshot(connection, watch_name="a", version=2)
        b1 = _snapshot(connection, watch_name="b", version=1)
        good = _event(connection, a1, a2, watch_name="a")
        # Cross-watch reference: event of "a" pointing at snapshot of "b".
        cross = insert_event(
            connection, watch_name="a", from_snapshot_id=a1.id,
            to_snapshot_id=b1.id, created_at=NOW, severity="minor",
            change_rate=0.1, added_lines=1, removed_lines=0,
            regions=(), keyword_hits=(), focus_hit=False, summary="x",
            diff_text="d")
        # Dangling reference to a snapshot id that no longer exists.
        dangling = insert_event(
            connection, watch_name="b", from_snapshot_id=b1.id,
            to_snapshot_id=9999, created_at=NOW, severity="minor",
            change_rate=0.1, added_lines=1, removed_lines=0,
            regions=(), keyword_hits=(), focus_hit=False, summary="x",
            diff_text="d")
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_snapshots, load_events
            result = check_event_references(
                load_events(ro), watch_names={"a", "b"},
                snapshots=load_snapshots(ro))

    by_event = {finding.evidence["event_id"]: finding.code
                for finding in result.findings}
    assert result.status == FAIL
    assert by_event[cross.id] == "event_snapshot_wrong_watch"
    assert by_event[dangling.id] == "event_snapshot_missing"
    assert good.id not in by_event


def test_alert_pairing_bidirectional_and_cooldown_aware(tmp_path):
    db_path = _fresh_db(tmp_path)
    with connect(db_path) as connection:
        s1 = _snapshot(connection, version=1)
        s2 = _snapshot(connection, version=2)
        s3 = _snapshot(connection, version=3)
        major_event = _event(connection, s1, s2, severity="major")
        # Second major event far enough away to fall outside the cooldown.
        insert_event(
            connection, watch_name=WATCH, from_snapshot_id=s2.id,
            to_snapshot_id=s3.id, created_at=NOW + 4000, severity="major",
            change_rate=0.9, added_lines=2, removed_lines=0, regions=(),
            keyword_hits=(), focus_hit=False, summary="x", diff_text="d")
        insert_alert(
            connection, watch_name=WATCH, severity="major",
            event_id=major_event.id, created_at=NOW, cooldown_seconds=0,
            message="one")
        # Orphan alert pointing at an event that does not exist.
        insert_alert(
            connection, watch_name=WATCH, severity="major", event_id=4242,
            created_at=NOW, cooldown_seconds=0, message="ghost")
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_alerts, load_events
            events = load_events(ro)
            alerts = load_alerts(ro)

    missing = check_alert_pairing(
        events, alerts, watch_names={WATCH},
        alert_severities=("major",), cooldown_seconds=3600)
    codes = {finding.code for finding in missing.findings}
    assert missing.status == FAIL
    assert "alert_record_missing" in codes  # second_major, outside cooldown?
    assert "alert_source_missing" in codes  # ghost alert -> event 4242

    # With a wide cooldown the second event is suppressed, not a failure.
    cooled = check_alert_pairing(
        events, alerts, watch_names={WATCH},
        alert_severities=("major",), cooldown_seconds=10_000)
    suppressed = [f for f in cooled.findings
                  if f.code == "alert_suppressed_by_cooldown"]
    assert suppressed and suppressed[0].severity == PASS


def test_outbox_pairing_detects_missing_and_orphan_entries(tmp_path):
    db_path = _fresh_db(tmp_path)
    outbox = tmp_path / "outbox" / "alerts.jsonl"
    with connect(db_path) as connection:
        s1 = _snapshot(connection, version=1)
        s2 = _snapshot(connection, version=2)
        event = _event(connection, s1, s2, severity="major")
        ledger_alert = insert_alert(
            connection, watch_name=WATCH, severity="major",
            event_id=event.id, created_at=NOW, cooldown_seconds=0,
            message="ledger")
        connection.commit()
    append_alert(outbox, ledger_alert)
    # Append an orphan line whose alert id exists nowhere in the ledger.
    with outbox.open("a", encoding="utf-8") as handle:
        handle.write('{"outbox_version":1,"id":777,"watch_name":"docs",'
                     '"severity":"major","event_id":1,"created_at":1,'
                     '"dedup_key":"x","message":"orphan"}\n')
        handle.write("{not valid json\n")

    with connect_read_only(db_path) as ro:
        from pagewatch.quality.store import load_alerts
        alerts = load_alerts(ro)
    result = check_outbox_pairing(alerts, watch_names={WATCH},
                                  outbox_path=outbox)
    codes = {finding.code for finding in result.findings}
    assert result.status == FAIL
    assert "outbox_orphan_entry" in codes
    assert "outbox_unparseable_line" in codes

    # A fresh ledger alert that never reached the outbox is reported missing.
    with connect(db_path) as connection:
        extra = insert_alert(
            connection, watch_name=WATCH, severity="major",
            event_id=event.id, created_at=NOW + 50, cooldown_seconds=0,
            message="never shipped")
        connection.commit()
    with connect_read_only(db_path) as ro:
        from pagewatch.quality.store import load_alerts
        result = check_outbox_pairing(load_alerts(ro),
                                      watch_names={WATCH},
                                      outbox_path=outbox)
    missing = next(f for f in result.findings
                   if f.code == "outbox_entry_missing")
    assert missing.evidence["alert_id"] == extra.id


def test_major_retention_detects_real_loss_without_false_positives(tmp_path):
    db_path = _fresh_db(tmp_path)
    with connect(db_path) as connection:
        snapshots = _healthy_chain(connection, count=4)
        major_event = insert_event(
            connection, watch_name=WATCH, from_snapshot_id=snapshots[1].id,
            to_snapshot_id=snapshots[2].id, created_at=NOW, severity="major",
            change_rate=0.9, added_lines=2, removed_lines=0, regions=(),
            keyword_hits=(), focus_hit=False, summary="major", diff_text="d")
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_events, load_snapshots
            healthy = check_major_retention(
                load_events(ro), watch_names={WATCH},
                snapshots=load_snapshots(ro))
        assert healthy.status == PASS

        # Simulate a buggy compaction that removes the protected major target.
        connection.execute("DELETE FROM snapshots WHERE id=?",
                           (snapshots[2].id,))
        connection.commit()
        with connect_read_only(db_path) as ro:
            from pagewatch.quality.store import load_events, load_snapshots
            broken = check_major_retention(
                load_events(ro), watch_names={WATCH},
                snapshots=load_snapshots(ro))
    assert broken.status == FAIL
    finding = broken.findings[0]
    assert finding.code == "major_target_snapshot_missing"
    assert finding.evidence == {"event_id": major_event.id,
                                "snapshot_id": snapshots[2].id}


def test_checks_are_unknown_when_old_database_lacks_columns(tmp_path):
    db_path = _fresh_db(tmp_path)
    # Build an old-shape events table without severity / endpoint columns.
    with connect(db_path) as connection:
        connection.execute("DROP TABLE events")
        connection.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, watch_name TEXT, "
            "created_at REAL)")
        connection.commit()
    with connect_read_only(db_path) as ro:
        with pytest.raises(TableUnavailable):
            from pagewatch.quality.store import load_events
            load_events(ro)
        checks, findings = run_all_checks(
            ro, watch_names=(WATCH,), outbox_path=tmp_path / "outbox.jsonl",
            alert_severities=("major",), cooldown_seconds=3600,
            rebaseline_versions={})
    statuses = {item.check: item.status for item in checks}
    assert statuses[CHECK_EVENT_REFERENCES] == UNKNOWN
    assert statuses[CHECK_MAJOR_RETENTION] == UNKNOWN
    assert statuses[CHECK_SNAPSHOT_CHAIN] == PASS  # table intact -> still runs
    assert all(f.severity == UNKNOWN
               for f in findings if f.check == CHECK_EVENT_REFERENCES)


# ------------------------------------------------------------- round flow

def _runner(tmp_path, rules=None):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    return paths, Runner(
        db_path=paths.db_path, archive_dir=paths.archive_dir,
        outbox_path=paths.outbox_path, rules=rules or Rules(),
    )


def _watch(name=WATCH, url=None):
    return WatchSpec(name=name, url=url or f"http://127.0.0.1:9999/{name}",
                     interval_seconds=60)


def _real_watch(base_url: str, name: str = "docs", path: str = "/docs"):
    return WatchSpec(name=name, url=f"{base_url}{path}", interval_seconds=60)


def test_verify_round_report_is_reproducible_and_persisted(tmp_path,
                                                           mock_server):
    base_url, scenario = mock_server
    paths, runner = _runner(tmp_path)
    watch = _real_watch(base_url)
    scenario.set_step(0)
    runner.run(watch)
    scenario.set_step(1)
    runner.run(watch)
    first = verify_round(db_path=paths.db_path, outbox_path=paths.outbox_path,
                         rules=Rules(), watch_names=(WATCH,), checked_at=NOW)
    assert first.report.status == PASS
    assert first.report.content_hash

    # Re-verifying the same data (without new writes) yields the same hash.
    second = verify_round(db_path=paths.db_path,
                          outbox_path=paths.outbox_path, rules=Rules(),
                          watch_names=(WATCH,), checked_at=NOW + 999)
    assert second.report.content_hash == first.report.content_hash
    assert second.report.round_id != first.report.round_id
    with connect(paths.db_path) as connection:
        rows = connection.execute(
            "SELECT round_id, content_hash FROM quality_reports ORDER BY id"
        ).fetchall()
    assert [row["round_id"] for row in rows] == [
        first.report.round_id, second.report.round_id]


def test_failed_verification_quarantines_watch_without_failing_round(
        tmp_path, mock_server):
    base_url, scenario = mock_server
    paths, runner = _runner(tmp_path)
    watch = _real_watch(base_url)
    scenario.set_step(0)
    runner.run(watch)
    scenario.set_step(1)
    runner.run(watch)  # two snapshots, one change event
    # Sabotage: delete the newest snapshot so the event dangles.
    with connect(paths.db_path) as connection:
        target = connection.execute(
            "SELECT MAX(id) AS mid FROM snapshots").fetchone()["mid"]
        connection.execute("DELETE FROM snapshots WHERE id=?", (target,))
        connection.commit()

    verification = verify_round(
        db_path=paths.db_path, outbox_path=paths.outbox_path, rules=Rules(),
        watch_names=(WATCH,), checked_at=NOW)
    assert verification.report.status == FAIL
    assert verification.report.quarantined == (WATCH,)
    state = verification.states[WATCH]
    assert state.status == WATCH_QUARANTINED
    assert state.consecutive_failures == 1

    # Quarantined watch still archives changes but emits no graded alert.
    with connect(paths.db_path) as connection:
        before_alerts = connection.execute(
            "SELECT COUNT(*) FROM alerts").fetchone()[0]
        # Force a full fetch instead of an ETag 304 short-circuit.
        connection.execute("UPDATE watches SET etag=NULL, last_modified=NULL")
        connection.commit()
    scenario.set_step(2)
    result = runner.run(watch)
    assert result.quarantined is True
    assert result.outcome == "quarantined"
    with connect(paths.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM alerts").fetchone()[0] == before_alerts
        audit_kinds = {row["kind"] for row in connection.execute(
            "SELECT DISTINCT kind FROM audit_log").fetchall()}
    assert "quality.quarantined_run" in audit_kinds


def test_repeated_breakage_triggers_bounded_rebaseline(tmp_path, mock_server):
    rules = replace(Rules(), quarantine_rebaseline_limit=2)
    base_url, scenario = mock_server
    paths, runner = _runner(tmp_path, rules)
    watch = _real_watch(base_url)
    scenario.set_step(0)
    runner.run(watch)
    scenario.set_step(1)
    runner.run(watch)
    with connect(paths.db_path) as connection:
        target = connection.execute(
            "SELECT MAX(id) AS mid FROM snapshots").fetchone()["mid"]
        connection.execute("DELETE FROM snapshots WHERE id=?", (target,))
        connection.commit()

    first = verify_round(db_path=paths.db_path,
                         outbox_path=paths.outbox_path, rules=rules,
                         watch_names=(WATCH,), checked_at=NOW)
    assert first.states[WATCH].status == WATCH_QUARANTINED
    second = verify_round(db_path=paths.db_path,
                          outbox_path=paths.outbox_path, rules=rules,
                          watch_names=(WATCH,), checked_at=NOW + 60)
    assert second.states[WATCH].status == WATCH_REBASELINED
    assert second.states[WATCH].consecutive_failures == 2

    # Next run force-rebaselines: new anchor snapshot, no event, no alert.
    scenario.set_step(3)
    with connect(paths.db_path) as connection:
        connection.execute("UPDATE watches SET etag=NULL, last_modified=NULL")
        connection.commit()
    result = runner.run(watch)
    assert result.rebaselined is True
    assert result.outcome == "rebaseline"
    with connect(paths.db_path) as connection:
        state = connection.execute(
            "SELECT * FROM quality_watch_states WHERE watch_name=?",
            (WATCH,)).fetchone()
        assert state["status"] == WATCH_QUARANTINED
        assert state["rebaseline_version"] is not None
        assert state["consecutive_failures"] == 0
        kinds = {row["kind"] for row in connection.execute(
            "SELECT DISTINCT kind FROM audit_log").fetchall()}
    assert "quality.rebaseline" in kinds

    # Chain check is now scoped to the anchor: old damage stops failing.
    recovered = verify_round(db_path=paths.db_path,
                             outbox_path=paths.outbox_path, rules=rules,
                             watch_names=(WATCH,), checked_at=NOW + 120)
    chain = next(item for item in recovered.report.checks
                 if item.check == CHECK_SNAPSHOT_CHAIN)
    assert chain.status == PASS
    assert recovered.states[WATCH].status == "ok"


def test_clean_round_clears_quarantine(tmp_path, mock_server):
    base_url, scenario = mock_server
    paths, runner = _runner(tmp_path)
    watch = _real_watch(base_url)
    scenario.set_step(0)
    runner.run(watch)
    scenario.set_step(1)
    runner.run(watch)
    with connect(paths.db_path) as connection:
        target = connection.execute(
            "SELECT MAX(id) AS mid FROM snapshots").fetchone()["mid"]
        connection.execute("DELETE FROM snapshots WHERE id=?", (target,))
        connection.commit()
    broken = verify_round(db_path=paths.db_path,
                          outbox_path=paths.outbox_path, rules=Rules(),
                          watch_names=(WATCH,), checked_at=NOW)
    assert broken.states[WATCH].status == WATCH_QUARANTINED

    # Repair the damage: re-insert the deleted snapshot with the same id.
    with connect(paths.db_path) as connection:
        connection.execute(
            "INSERT INTO snapshots(id, watch_name, version, fetched_at, "
            "status_code, url, content_sha256, normalized_text, blocks_json, "
            "normalizer_version) VALUES(?, ?, 2, ?, 200, ?, ?, ?, '[]', ?)",
            (target, WATCH, NOW, f"http://127.0.0.1/{WATCH}", "restored",
             "restored body", "normalizer-v1"))
        connection.commit()
    healed = verify_round(db_path=paths.db_path,
                          outbox_path=paths.outbox_path, rules=Rules(),
                          watch_names=(WATCH,), checked_at=NOW + 60)
    assert healed.report.status == PASS
    assert healed.states[WATCH].status == "ok"
    assert healed.states[WATCH].consecutive_failures == 0


def test_verification_runs_after_workers_commit_under_concurrency(
        tmp_path, mock_server):
    """Round finalization must observe every worker's committed rows."""
    base_url, scenario = mock_server
    scenario.set_step(0)
    rules = replace(Rules(), max_workers=4)
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    runner = Runner(
        db_path=paths.db_path, archive_dir=paths.archive_dir,
        outbox_path=paths.outbox_path, rules=rules,
    )
    names = ("announcements", "docs", "pricing", "noisy-status")
    barrier = threading.Barrier(len(names))

    def slow_run(watch: WatchSpec):
        barrier.wait()  # maximise overlap, then all finish close together
        return runner.run(watch)

    from pagewatch.schedule.scheduler import execute_round
    routes = {"announcements": "/announcements", "docs": "/docs",
              "pricing": "/pricing", "noisy-status": "/noisy"}
    watches = [
        WatchSpec(name=name, url=f"{base_url}{routes[name]}",
                  interval_seconds=60)
        for name in names
    ]
    results = execute_round(watches, slow_run, max_workers=4)
    assert len(results) == 4
    assert all(result.outcome != "error" for result in results)

    verification = verify_round(
        db_path=paths.db_path, outbox_path=paths.outbox_path, rules=rules,
        watch_names=names, checked_at=NOW)
    # No missing snapshots despite concurrent writers: all four first
    # snapshots are visible to the read-only verification connection.
    assert verification.report.status == PASS
    with connect_read_only(paths.db_path) as ro:
        seen = {row["watch_name"] for row in ro.execute(
            "SELECT DISTINCT watch_name FROM snapshots").fetchall()}
    assert set(names) <= seen


def test_verification_connection_cannot_write(tmp_path):
    db_path = _fresh_db(tmp_path)
    with connect_read_only(db_path) as ro:
        with pytest.raises(sqlite3.OperationalError):
            ro.execute("CREATE TABLE attempted_write(id INTEGER)")
        with pytest.raises(sqlite3.OperationalError):
            ro.execute("INSERT INTO meta(key, value) VALUES('x', 'y')")


def test_outbox_and_ledger_pairing_matches_actual_database_contents(
        tmp_path, mock_server):
    """Acceptance-style cross-check against raw SQL ground truth."""
    base_url, scenario = mock_server
    paths, runner = _runner(tmp_path)
    watch = _real_watch(base_url)
    scenario.set_step(0)
    runner.run(watch)
    scenario.set_step(3)  # focus change -> major event in the demo scenario
    runner.run(watch)
    verification = verify_round(
        db_path=paths.db_path, outbox_path=paths.outbox_path, rules=Rules(),
        watch_names=(WATCH,), checked_at=NOW)

    with connect_read_only(paths.db_path) as ro:
        ledger_ids = {row["id"] for row in ro.execute(
            "SELECT id FROM alerts").fetchall()}
    outbox_ids = {row["id"] for row in read_outbox(paths.outbox_path)}
    pairing = next(item for item in verification.report.checks
                   if item.check == CHECK_OUTBOX_PAIRING)
    # The check's verdict must equal the raw set comparison.
    assert (pairing.status == PASS) == (ledger_ids == outbox_ids)
    assert pairing.failure_count == len(ledger_ids ^ outbox_ids)


def test_major_retention_survives_real_compaction_and_flags_buggy_one(tmp_path):
    """End-to-end: the shipped compaction protects major targets; a defective
    deletion that removes one is caught by verification without false alarms."""
    from pagewatch.retention.compact import compact

    db_path = _fresh_db(tmp_path)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    with connect(db_path) as connection:
        snapshots = _healthy_chain(connection, count=6)
        # Major event targets snapshot #3 (id of the third snapshot).
        major_target = snapshots[2]
        insert_event(
            connection, watch_name=WATCH,
            from_snapshot_id=snapshots[1].id,
            to_snapshot_id=major_target.id, created_at=NOW, severity="major",
            change_rate=0.9, added_lines=2, removed_lines=0, regions=(),
            keyword_hits=(), focus_hit=False, summary="major", diff_text="d")
        connection.commit()

    rules = replace(Rules(), keep_last=2, major_keep_forever=True,
                    archive_max_age_days=0)
    compact(db_path, archive_dir, rules, NOW + 100)
    verification = verify_round(
        db_path=db_path, outbox_path=tmp_path / "outbox.jsonl", rules=rules,
        watch_names=(WATCH,), checked_at=NOW + 200)
    major_check = next(item for item in verification.report.checks
                       if item.check == CHECK_MAJOR_RETENTION)
    assert major_check.status == PASS  # real compaction kept the major target
    with connect_read_only(db_path) as ro:
        assert ro.execute("SELECT 1 FROM snapshots WHERE id=?",
                          (major_target.id,)).fetchone() is not None

    # Now simulate a defective compaction that violates the protection.
    with connect(db_path) as connection:
        connection.execute("DELETE FROM snapshots WHERE id=?",
                           (major_target.id,))
        connection.commit()
    broken = verify_round(
        db_path=db_path, outbox_path=tmp_path / "outbox.jsonl", rules=rules,
        watch_names=(WATCH,), checked_at=NOW + 300)
    major_check = next(item for item in broken.report.checks
                       if item.check == CHECK_MAJOR_RETENTION)
    assert major_check.status == FAIL
    assert major_check.findings[0].code == "major_target_snapshot_missing"
    assert broken.report.quarantined == (WATCH,)


def test_missing_snapshot_table_degrades_checks_to_unknown(tmp_path):
    """No ledger means 'cannot judge', never a cascade of false failures."""
    db_path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db_path) as conn:
        # Events and alerts exist but the snapshots table is entirely absent.
        conn.executescript("""
        CREATE TABLE events (id INTEGER PRIMARY KEY, watch_name TEXT,
          from_snapshot_id INTEGER, to_snapshot_id INTEGER, created_at REAL,
          severity TEXT, change_rate REAL, added_lines INTEGER,
          removed_lines INTEGER, regions_json TEXT, keyword_hits_json TEXT,
          focus_hit INTEGER, summary TEXT, diff_text TEXT);
        INSERT INTO events VALUES(1,'docs',1,2,1.0,'major',0.9,1,0,'[]','[]',0,'s','d');
        CREATE TABLE alerts (id INTEGER PRIMARY KEY, watch_name TEXT,
          severity TEXT, event_id INTEGER, created_at REAL, dedup_key TEXT,
          message TEXT);
        """)
        conn.commit()
    with connect_read_only(db_path) as ro:
        checks, _ = run_all_checks(
            ro, watch_names=("docs",), outbox_path=tmp_path / "o.jsonl",
            alert_severities=("major",), cooldown_seconds=3600,
            rebaseline_versions={})
    statuses = {item.check: item.status for item in checks}
    assert statuses[CHECK_SNAPSHOT_CHAIN] == UNKNOWN
    assert statuses[CHECK_EVENT_REFERENCES] == UNKNOWN
    assert statuses[CHECK_MAJOR_RETENTION] == UNKNOWN
    # Alert pairing only needs events + alerts: the missing alert record for
    # the major event is a genuine, independently detectable failure.
    assert statuses[CHECK_ALERT_PAIRING] == FAIL
