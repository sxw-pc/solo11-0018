from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from pagewatch.alert.outbox import (
    alert_line, append_alert, outbox_lock, read_outbox, rewrite_outbox,
    stage_rewrite,
)
from pagewatch.fetch.archive import write_archive
from pagewatch.models import AlertRecord, Rules, Snapshot
from pagewatch.retention import compact as compact_mod
from pagewatch.retention.compact import compact, plan_from_database
from pagewatch.retention.policy import (
    build_cascade_plan, build_plan, format_plan, snapshots_to_prune,
)
from pagewatch.storage.db import connect, init_db
from pagewatch.storage.events import (
    insert_alert, insert_event, list_alerts, list_events,
)
from pagewatch.storage.snapshots import (
    append_snapshot, mark_snapshot_major, snapshot_by_id,
)

NOW = 1_800_000_000.0


def snap(snapshot_id, version, watch_name="a", raw="raw", major=False,
         fetched_at=NOW):
    return Snapshot(
        id=snapshot_id, watch_name=watch_name, version=version,
        fetched_at=fetched_at, status_code=200,
        url=f"http://127.0.0.1/{watch_name}",
        etag=None, last_modified=None, content_sha256=f"c{snapshot_id}",
        raw_sha256=raw, normalized_text=f"text {version}",
        blocks_json="[]", normalizer_version="normalizer-v1", is_major=major,
    )


def add_snapshot(connection, version, *, watch_name="a", text=None,
                 raw_digest=None, fetched_at=NOW):
    text = text if text is not None else f"v{version}"
    snapshot = append_snapshot(
        connection, watch_name=watch_name, fetched_at=fetched_at,
        status_code=200, url=f"http://127.0.0.1/{watch_name}", etag=None,
        last_modified=None, content_sha256=f"h{watch_name}{version}",
        raw_sha256=raw_digest, normalized_text=text, blocks_json="[]",
        normalizer_version="normalizer-v1",
    )
    return snapshot


def add_event(connection, from_id, to_id, *, watch_name="a",
              severity="minor", created_at=None):
    event = insert_event(
        connection, watch_name=watch_name, from_snapshot_id=from_id,
        to_snapshot_id=to_id, created_at=created_at or NOW,
        severity=severity, change_rate=0.2, added_lines=1, removed_lines=1,
        regions=(), keyword_hits=(), focus_hit=False,
        summary=f"{severity} change", diff_text="diff",
    )
    if severity == "major":
        mark_snapshot_major(connection, to_id)
    return event


def add_alert_row(connection, event, *, created_at=None):
    return insert_alert(
        connection, watch_name=event.watch_name, severity=event.severity,
        event_id=event.id, created_at=created_at or event.created_at,
        message=f"alert for event {event.id}", cooldown_seconds=0,
    )


def rules(**overrides):
    values = {"keep_last": 1, "major_keep_forever": True,
              "archive_max_age_days": 1}
    values.update(overrides)
    return replace(Rules(), **values)


def dangling_rows(connection):
    events = connection.execute(
        "SELECT e.id FROM events e WHERE NOT EXISTS ("
        "SELECT 1 FROM snapshots s WHERE s.id=e.from_snapshot_id) "
        "OR NOT EXISTS ("
        "SELECT 1 FROM snapshots s WHERE s.id=e.to_snapshot_id)"
    ).fetchall()
    alerts = connection.execute(
        "SELECT a.id FROM alerts a WHERE NOT EXISTS ("
        "SELECT 1 FROM events e WHERE e.id=a.event_id)"
    ).fetchall()
    return [row[0] for row in events], [row[0] for row in alerts]


def old_archive(text, archive_dir):
    path, digest, _ = write_archive(text.encode(), archive_dir)
    os.utime(path, (NOW - 10 * 86_400, NOW - 10 * 86_400))
    return path, digest


# --------------------------------------------------------------------------
# Pure policy unit tests
# --------------------------------------------------------------------------

def test_keep_last_and_major_permanence():
    snapshots = [snap(1, 1, major=True), snap(2, 2), snap(3, 3)]
    assert [item.id for item in snapshots_to_prune(
        snapshots, {1}, keep_last=1, major_keep_forever=True)] == [2]
    assert [item.id for item in snapshots_to_prune(
        snapshots, set(), keep_last=1, major_keep_forever=False)] == [1, 2]
    assert [item.id for item in snapshots_to_prune(
        snapshots, set(), keep_last=5)] == []


def test_plan_is_per_watch_and_archives_respect_references(tmp_path):
    paths, digests = {}, {}
    for value in ("drop-a", "keep-a", "drop-b", "keep-b", "orphan"):
        path, digest = old_archive(value, tmp_path)
        paths[value] = path
        digests[value] = digest
    snapshots = [
        snap(1, 1, "a", raw=digests["drop-a"]),
        snap(2, 2, "a", raw=digests["keep-a"]),
        snap(3, 1, "b", raw=digests["drop-b"]),
        snap(4, 2, "b", raw=digests["keep-b"]),
    ]
    plan = build_plan(snapshots, set(), keep_last=1, major_keep_forever=True,
                      archive_dir=tmp_path, now=NOW, archive_max_age_days=1)
    assert plan.snapshot_ids == (1, 3)
    assert paths["drop-a"] in plan.archives
    assert paths["drop-b"] in plan.archives
    assert paths["orphan"] in plan.archives
    assert paths["keep-a"] not in plan.archives
    assert paths["keep-b"] not in plan.archives
    assert "estimated reclaimable" in format_plan(plan)
    empty = build_plan([], set(), keep_last=1, major_keep_forever=True,
                       archive_dir=tmp_path / "missing", now=NOW,
                       archive_max_age_days=1)
    assert format_plan(empty) == "nothing to clean (plan is empty)"


def test_cascade_plan_one_judgment_for_snapshot_event_alert(tmp_path):
    snapshots = [snap(1, 1), snap(2, 2), snap(3, 3)]

    class _Event:
        def __init__(self, eid, from_id, to_id):
            self.id = eid
            self.from_snapshot_id = from_id
            self.to_snapshot_id = to_id

    events = [_Event(10, 1, 2), _Event(20, 2, 3)]

    class _Alert:
        def __init__(self, aid, event_id):
            self.id = aid
            self.event_id = event_id

    alerts = [_Alert(100, 10), _Alert(200, 20)]
    plan = build_cascade_plan(
        snapshots, events, alerts, set(), keep_last=1,
        major_keep_forever=True, archive_dir=tmp_path, now=NOW,
        not_after=NOW + 1, archive_max_age_days=1,
    )
    # keep_last=1 protects only s3; s1 and s2 are pruned.  Event 10 touches
    # s1, event 20 touches s2, so both events (and their alerts) cascade.
    assert plan.snapshot_ids == (1, 2)
    assert plan.event_ids == (10, 20)
    assert plan.alert_ids == (100, 200)


def test_cascade_plan_repairs_legacy_dangling_rows(tmp_path):
    snapshots = [snap(2, 2), snap(3, 3)]  # snapshot 1 already gone

    class _Event:
        def __init__(self, eid, from_id, to_id):
            self.id = eid
            self.from_snapshot_id = from_id
            self.to_snapshot_id = to_id

    class _Alert:
        def __init__(self, aid, event_id):
            self.id = aid
            self.event_id = event_id

    events = [_Event(10, 1, 2), _Event(20, 2, 3)]
    alerts = [_Alert(100, 10), _Alert(200, 20), _Alert(300, 999)]
    plan = build_cascade_plan(
        snapshots, events, alerts, set(), keep_last=1,
        major_keep_forever=True, archive_dir=tmp_path, now=NOW,
        not_after=NOW + 1, archive_max_age_days=1,
    )
    # Event 10 is legacy-dangling (snapshot 1 gone); event 20 is removed
    # because s2 is pruned by keep_last; alert 300 is a legacy dangling alert.
    assert set(plan.event_ids) == {10, 20}
    assert set(plan.alert_ids) == {100, 200, 300}
    assert plan.snapshot_ids == (2,)


def test_time_bound_excludes_fresh_snapshots_from_keep_last_window(tmp_path):
    snapshots = [snap(1, 1, fetched_at=NOW - 100),
                 snap(2, 2, fetched_at=NOW - 50),
                 snap(3, 3, fetched_at=NOW + 10)]
    plan = build_cascade_plan(
        snapshots, (), (), set(), keep_last=1, major_keep_forever=True,
        archive_dir=tmp_path, now=NOW, not_after=NOW, archive_max_age_days=1,
    )
    # Only s1/s2 are in scope; keep_last protects s2, so s1 is pruned.  s3 is
    # outside the bound and must never enter the prune set.
    assert plan.snapshot_ids == (1,)


def test_archive_bound_protects_inflight_archives(tmp_path):
    import time as wall_time

    fresh_path, _, _ = write_archive(b"fresh", tmp_path)
    real_now = wall_time.time()
    # Simulate an archive written while the compaction round is in flight:
    # its mtime lands at/after the fixed lower bound.
    os.utime(fresh_path, (real_now + 10, real_now + 10))
    plan = build_cascade_plan(
        (), (), (), set(), keep_last=1, major_keep_forever=True,
        archive_dir=tmp_path, now=real_now, not_after=real_now,
        archive_max_age_days=0,
    )
    assert plan.archives == ()  # mtime >= bound, never treated as garbage


# --------------------------------------------------------------------------
# End-to-end cascade compaction
# --------------------------------------------------------------------------

def _build_history(tmp_path, *, with_alerts=True, archive_dir=None):
    db_path = tmp_path / "pagewatch.sqlite"
    archive_dir = archive_dir or tmp_path / "archive"
    outbox_path = tmp_path / "outbox" / "alerts.jsonl"
    init_db(db_path)
    with connect(db_path) as connection:
        digests = {}
        snapshots = []
        for version in range(1, 5):
            path, digest = old_archive(f"body{version}", archive_dir)
            digests[version] = digest
            snapshots.append(add_snapshot(
                connection, version, raw_digest=digest,
                fetched_at=NOW - (5 - version) * 100,
            ))
        # events: 1->2 (minor), 2->3 (minor), 3->4 (minor)
        events = [add_event(connection, snapshots[i].id, snapshots[i + 1].id)
                  for i in range(3)]
        alerts = []
        if with_alerts:
            for event in events:
                alert = add_alert_row(connection, event)
                alerts.append(alert)
        connection.commit()
    if with_alerts:
        for alert in alerts:
            append_alert(outbox_path, alert)
    return db_path, archive_dir, outbox_path, snapshots, events, alerts


def test_compact_cascade_removes_event_alert_and_jsonl_together(tmp_path):
    db_path, archive_dir, outbox_path, snaps, events, alerts = _build_history(
        tmp_path)
    result = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert result.executed is True
    # keep_last=1: v1..v3 pruned, v4 kept; every event touches a pruned row.
    assert result.removed_snapshots == 3
    assert result.removed_events == 3
    assert result.removed_alerts == 3
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, snaps[3].id) is not None
        assert all(snapshot_by_id(connection, s.id) is None
                   for s in snaps[:3])
        assert list_events(connection) == []
        assert list_alerts(connection) == []
        dangling_events, dangling_alerts = dangling_rows(connection)
        assert dangling_events == [] and dangling_alerts == []
    # jsonl mirrors the alert ledger exactly.
    assert read_outbox(outbox_path) == []
    # Gzip archives of pruned, unreferenced snapshots are gone; the retained
    # snapshot's archive survives even though it is older than max_age.
    assert not (archive_dir / f"{snaps[0].raw_sha256}.gz").exists()
    assert (archive_dir / f"{snaps[3].raw_sha256}.gz").exists()


def test_major_event_target_snapshots_are_kept_forever(tmp_path):
    db_path = tmp_path / "pagewatch.sqlite"
    archive_dir = tmp_path / "archive"
    outbox_path = tmp_path / "outbox" / "alerts.jsonl"
    init_db(db_path)
    with connect(db_path) as connection:
        snaps = [add_snapshot(connection, v, fetched_at=NOW - (5 - v) * 100)
                 for v in range(1, 5)]
        # v3 is the target of a major event and must be exempt from keep_last.
        major_event = add_event(connection, snaps[1].id, snaps[2].id,
                                severity="major")
        # Event whose endpoints both survive must survive with them.
        kept_event = add_event(connection, snaps[2].id, snaps[3].id,
                               severity="minor")
        # Event touching the old, pruned chain is cascaded away.
        minor_event = add_event(connection, snaps[0].id, snaps[1].id,
                                severity="minor")
        connection.commit()
    major_target = snaps[2].id
    result = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert result.removed_snapshots == 2  # v1 and v2; v3 major, v4 newest
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, major_target) is not None
        assert snapshot_by_id(connection, snaps[3].id) is not None
        assert snapshot_by_id(connection, snaps[0].id) is None
        assert snapshot_by_id(connection, snaps[1].id) is None
        surviving = {event.id for event in list_events(connection)}
        assert major_event.id not in surviving   # references pruned v2
        assert kept_event.id in surviving        # endpoints v3/v4 retained
        assert minor_event.id not in surviving
        dangling_events, _ = dangling_rows(connection)
        assert dangling_events == []


def test_dry_run_changes_nothing_and_plan_matches_execute(tmp_path):
    db_path, archive_dir, outbox_path, *_ = _build_history(tmp_path)
    before_archives = sorted(p.name for p in archive_dir.glob("*.gz"))
    jsonl_before = outbox_path.read_bytes()
    dry = compact(db_path, archive_dir, outbox_path, rules(), NOW,
                  dry_run=True)
    assert dry.executed is False
    with connect(db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 4
        assert len(list_events(connection)) == 3
        assert len(list_alerts(connection)) == 3
    assert outbox_path.read_bytes() == jsonl_before
    assert sorted(p.name for p in archive_dir.glob("*.gz")) == before_archives
    assert len(dry.plan.snapshot_ids) == 3
    assert len(dry.plan.event_ids) == 3
    assert len(dry.plan.alert_ids) == 3


def test_jsonl_rewrite_matches_event_ledger_and_is_atomic(tmp_path):
    db_path, archive_dir, outbox_path, *_ = _build_history(tmp_path)
    # Plant an orphan jsonl line (alert row missing) plus keep two real rows.
    orphan = AlertRecord(id=999, watch_name="ghost", severity="major",
                         event_id=999, created_at=NOW, dedup_key="ghost:major",
                         message="ghost")
    append_alert(outbox_path, orphan)
    compact(db_path, archive_dir, outbox_path,
            rules(keep_last=3), NOW)  # retain newest 3 snapshots -> no cascade
    with connect(db_path) as connection:
        ledger_ids = {row["id"] for row in
                      connection.execute("SELECT id FROM alerts")}
    jsonl_ids = {row["id"] for row in read_outbox(outbox_path)}
    assert jsonl_ids == ledger_ids
    assert 999 not in jsonl_ids
    # No leftover temp files from the atomic replace.
    assert not list(outbox_path.parent.glob("*.tmp"))


def test_repeated_compaction_is_stable_and_does_no_second_damage(tmp_path):
    db_path, archive_dir, outbox_path, *_ = _build_history(tmp_path)
    first = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert first.removed_snapshots == 3

    def state_signature():
        with connect(db_path) as connection:
            db_state = (
                connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0],
                connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0],
            )
        return (db_state, outbox_path.read_bytes(),
                sorted(p.name for p in archive_dir.glob("*.gz")))

    after_first = state_signature()
    second = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert second.plan.empty is True
    assert (second.removed_snapshots, second.removed_events,
            second.removed_alerts, second.removed_archives) == (0, 0, 0, 0)
    assert state_signature() == after_first

    # A third run is still a no-op.
    third = compact(db_path, archive_dir, outbox_path, rules(), NOW,
                    dry_run=True)
    assert third.plan.empty is True


def test_concurrent_snapshot_is_not_collected_by_inflight_compaction(tmp_path):
    db_path, archive_dir, outbox_path, snaps, _, _ = _build_history(
        tmp_path, with_alerts=False)
    # Simulate the scheduler committing a new snapshot while compaction has
    # already fixed its lower bound.
    fresh_snapshot = []

    real_connect = compact_mod.connect

    def spying_connect(path):
        connection = real_connect(path)
        real_execute = connection.execute

        def execute(sql, params=()):
            if sql.strip().upper().startswith("BEGIN"):
                with real_connect(db_path) as writer:
                    if not fresh_snapshot:
                        fresh_snapshot.append(add_snapshot(
                            writer, 5, fetched_at=NOW + 1_000))
                    writer.commit()
            return real_execute(sql, params)

        connection.execute = execute  # type: ignore[method-assign]
        return connection

    monkeypatch_target = compact_mod
    original = monkeypatch_target.connect
    monkeypatch_target.connect = spying_connect  # type: ignore[assignment]
    try:
        result = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    finally:
        monkeypatch_target.connect = original  # type: ignore[assignment]

    assert fresh_snapshot, "concurrent snapshot fixture did not run"
    fresh_id = fresh_snapshot[0].id
    assert fresh_id not in result.plan.snapshot_ids
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, fresh_id) is not None
        # Pre-bound snapshots still compacted normally.
        assert snapshot_by_id(connection, snaps[0].id) is None


def test_concurrent_outbox_append_during_compaction_is_preserved(tmp_path):
    db_path, archive_dir, outbox_path, snaps, _, _ = _build_history(
        tmp_path, with_alerts=False)
    with connect(db_path) as connection:
        event = add_event(connection, snaps[2].id, snaps[3].id)
        late_alert = add_alert_row(connection, event)
        connection.commit()
    append_alert(outbox_path, late_alert)

    # keep everything so the plan itself deletes nothing; the point of this
    # test is the lock-mediated jsonl reconciliation under contention.
    keep_all = rules(keep_last=5)

    # Hold the outbox lock so compact blocks; meanwhile the runner commits a
    # new alert row and appends its jsonl line, exactly like scheduler/compact
    # contention on a real machine.
    holder = outbox_lock(outbox_path)
    holder.__enter__()
    outcome = {}

    def run_compact():
        try:
            compact(db_path, archive_dir, outbox_path, keep_all, NOW)
        except BaseException as exc:  # noqa: BLE001 - surfaced from thread
            outcome["error"] = exc

    thread = threading.Thread(target=run_compact)
    thread.start()
    time.sleep(0.2)  # let the compaction thread park on the flock
    with connect(db_path) as writer:
        extra = insert_alert(
            writer, watch_name=event.watch_name, severity="major",
            event_id=event.id, created_at=NOW + 50,
            message="concurrent alert", cooldown_seconds=0,
        )
        writer.commit()
    assert extra is not None
    # Simulate a runner that committed its row before compact took the lock;
    # append directly because this thread itself still owns the flock.
    with outbox_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(alert_line(extra) + "\n")
    holder.__exit__(None, None, None)
    thread.join(timeout=10)
    assert "error" not in outcome
    assert not thread.is_alive()

    jsonl_ids = {row["id"] for row in read_outbox(outbox_path)}
    with connect(db_path) as connection:
        ledger_ids = {row["id"] for row in
                      connection.execute("SELECT id FROM alerts")}
    assert jsonl_ids == ledger_ids
    assert {late_alert.id, extra.id} <= jsonl_ids


# --------------------------------------------------------------------------
# Failure injection: no half-deleted state
# --------------------------------------------------------------------------

def test_sqlite_failure_rolls_back_everything_and_keeps_outbox(tmp_path,
                                                               monkeypatch):
    db_path, archive_dir, outbox_path, snaps, events, alerts = _build_history(
        tmp_path)
    archives_before = sorted(p.name for p in archive_dir.glob("*.gz"))
    jsonl_before = outbox_path.read_bytes()

    def boom(*args, **kwargs):
        raise RuntimeError("injected sqlite failure")

    monkeypatch.setattr(compact_mod, "delete_events", boom)
    with pytest.raises(RuntimeError, match="injected sqlite failure"):
        compact(db_path, archive_dir, outbox_path, rules(), NOW)

    with connect(db_path) as connection:
        for snapshot in snaps:
            assert snapshot_by_id(connection, snapshot.id) is not None
        assert len(list_events(connection)) == len(events)
        assert len(list_alerts(connection)) == len(alerts)
        dangling_events, dangling_alerts = dangling_rows(connection)
        assert dangling_events == [] and dangling_alerts == []
    assert outbox_path.read_bytes() == jsonl_before
    assert sorted(p.name for p in archive_dir.glob("*.gz")) == archives_before
    assert not list(outbox_path.parent.glob("*.tmp"))

    # After the fault clears, a re-run completes the cleanup normally.
    monkeypatch.undo()
    result = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert result.removed_snapshots == 3
    assert read_outbox(outbox_path) == []


def test_replace_failure_keeps_original_outbox_and_intact_database(tmp_path,
                                                                   monkeypatch):
    db_path, archive_dir, outbox_path, snaps, _, _ = _build_history(tmp_path)
    archives_before = sorted(p.name for p in archive_dir.glob("*.gz"))
    jsonl_before = outbox_path.read_bytes()

    real_replace = os.replace

    def fail_once(src, dst):
        if Path(dst) == outbox_path:
            raise OSError("injected replace failure")
        return real_replace(src, dst)

    monkeypatch.setattr(compact_mod.os, "replace", fail_once)
    with pytest.raises(OSError, match="injected replace failure"):
        compact(db_path, archive_dir, outbox_path, rules(), NOW)

    # Database transaction was already committed; the original jsonl must be
    # untouched (temp staged file cleaned up), no archives unlinked yet.
    assert outbox_path.read_bytes() == jsonl_before
    assert sorted(p.name for p in archive_dir.glob("*.gz")) == archives_before
    assert not list(outbox_path.parent.glob("*.tmp"))
    with connect(db_path) as connection:
        dangling_events, dangling_alerts = dangling_rows(connection)
        assert dangling_events == [] and dangling_alerts == []

    # Re-run converges: stale jsonl lines are reconciled against the ledger.
    monkeypatch.undo()
    compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert read_outbox(outbox_path) == []
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, snaps[3].id) is not None


def test_archive_unlink_failure_is_retried_by_rerun(tmp_path, monkeypatch):
    db_path, archive_dir, outbox_path, snaps, _, _ = _build_history(tmp_path)
    target = archive_dir / f"{snaps[0].raw_sha256}.gz"
    real_unlink = Path.unlink
    calls = {"n": 0}

    def flaky_unlink(self, *args, **kwargs):
        if self == target and calls["n"] == 0:
            calls["n"] += 1
            raise OSError("injected unlink failure")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    with pytest.raises(OSError, match="injected unlink failure"):
        compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert target.exists()
    monkeypatch.undo()
    second = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert not target.exists()
    assert second.removed_archives >= 1


def test_plan_uses_consistent_read_snapshot(tmp_path):
    db_path, archive_dir, outbox_path, snaps, _, _ = _build_history(
        tmp_path, with_alerts=False)
    plan = plan_from_database(db_path, archive_dir, rules(), NOW)
    assert set(plan.snapshot_ids) == {s.id for s in snaps[:3]}
    assert plan.not_after == NOW


def test_dangling_guard_aborts_the_whole_transaction(tmp_path, monkeypatch):
    db_path, archive_dir, outbox_path, snaps, events, alerts = _build_history(
        tmp_path)
    # Force the post-delete invariant check to trip: it must roll back all
    # three deletes rather than commit a half-cascade.
    def always_tripping(connection):
        raise RuntimeError("injected invariant failure")

    monkeypatch.setattr(compact_mod, "_assert_no_dangling_references",
                        always_tripping)
    with pytest.raises(RuntimeError, match="injected invariant failure"):
        compact(db_path, archive_dir, outbox_path, rules(), NOW)
    with connect(db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM snapshots").fetchone()[0] == len(snaps)
        assert len(list_events(connection)) == len(events)
        assert len(list_alerts(connection)) == len(alerts)
    assert len(read_outbox(outbox_path)) == len(alerts)
    assert not list(outbox_path.parent.glob("*.tmp"))


def test_stage_failure_cleans_temp_and_leaves_outbox_untouched(tmp_path,
                                                                monkeypatch):
    db_path, archive_dir, outbox_path, *_ = _build_history(tmp_path)
    jsonl_before = outbox_path.read_bytes()
    real_mkstemp = tempfile.mkstemp

    def failing_mkstemp(*args, **kwargs):
        raise OSError("injected mkstemp failure")

    monkeypatch.setattr(tempfile, "mkstemp", failing_mkstemp)
    with pytest.raises(OSError, match="injected mkstemp failure"):
        compact(db_path, archive_dir, outbox_path, rules(), NOW)
    monkeypatch.undo()
    assert outbox_path.read_bytes() == jsonl_before
    assert not list(outbox_path.parent.glob("*.tmp"))
    # Database was never touched: re-run succeeds normally.
    result = compact(db_path, archive_dir, outbox_path, rules(), NOW)
    assert result.removed_snapshots == 3


def test_reconcile_outbox_runs_even_when_database_plan_is_empty(tmp_path):
    # Empty database (nothing to prune) but a stale orphan jsonl line: the
    # standalone reconciliation path must still converge the file.
    db_path = tmp_path / "pagewatch.sqlite"
    outbox_path = tmp_path / "outbox" / "alerts.jsonl"
    init_db(db_path)
    ghost = AlertRecord(id=4242, watch_name="ghost", severity="major",
                        event_id=4242, created_at=NOW,
                        dedup_key="ghost:major", message="stale")
    append_alert(outbox_path, ghost)
    result = compact(db_path, tmp_path / "archive", outbox_path, rules(), NOW)
    assert result.removed_snapshots == 0
    assert read_outbox(outbox_path) == []


# --------------------------------------------------------------------------
# Outbox primitives
# --------------------------------------------------------------------------

def test_rewrite_outbox_keeps_only_retained_ids(tmp_path):
    path = tmp_path / "alerts.jsonl"
    records = [
        AlertRecord(id=i, watch_name="w", severity="major", event_id=i,
                    created_at=NOW, dedup_key=f"w:major:{i}",
                    message=f"m{i}")
        for i in range(1, 4)
    ]
    for record in records:
        append_alert(path, record)
    retained = rewrite_outbox(path, {1, 3})
    assert retained == 2
    assert [row["id"] for row in read_outbox(path)] == [1, 3]


def test_stage_then_replace_under_lock(tmp_path):
    path = tmp_path / "alerts.jsonl"
    record = AlertRecord(id=1, watch_name="w", severity="major", event_id=1,
                         created_at=NOW, dedup_key="w:major", message="m")
    append_alert(path, record)
    with outbox_lock(path):
        temp = stage_rewrite(path, set())
        assert path.read_text().strip()  # original untouched
        os.replace(temp, path)
    assert read_outbox(path) == []


def test_malformed_outbox_line_is_rejected_not_silently_dropped(tmp_path):
    path = tmp_path / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"no_id": True}) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="alert id"):
        rewrite_outbox(path, set())
