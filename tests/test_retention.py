from __future__ import annotations

import os
from dataclasses import replace

from pagewatch.fetch.archive import write_archive
from pagewatch.models import Rules, Snapshot
from pagewatch.retention.compact import compact
from pagewatch.retention.policy import (
    build_plan, format_plan, snapshots_to_prune,
)
from pagewatch.storage.db import connect, init_db
from pagewatch.storage.events import (
    event_referenced_snapshot_ids, insert_event,
)
from pagewatch.storage.snapshots import (
    append_snapshot, mark_snapshot_major, snapshot_by_id,
)

NOW = 1_800_000_000.0


def snap(snapshot_id, version, watch_name="a", raw="raw", major=False):
    return Snapshot(
        id=snapshot_id, watch_name=watch_name, version=version,
        fetched_at=NOW, status_code=200, url=f"http://127.0.0.1/{watch_name}",
        etag=None, last_modified=None, content_sha256=f"c{snapshot_id}",
        raw_sha256=raw, normalized_text=f"text {version}",
        blocks_json="[]", normalizer_version="normalizer-v1", is_major=major,
    )


def test_keep_last_and_major_permanence():
    snapshots = [snap(1, 1, major=True), snap(2, 2), snap(3, 3)]
    assert [item.id for item in snapshots_to_prune(
        snapshots, {1}, keep_last=1, major_keep_forever=True)] == [2]
    assert [item.id for item in snapshots_to_prune(
        snapshots, set(), keep_last=1, major_keep_forever=False)] == [1, 2]
    assert [item.id for item in snapshots_to_prune(
        snapshots, set(), keep_last=5)] == []


def test_plan_is_per_watch_and_archives_respect_references(tmp_path):
    old = NOW - 10 * 86_400
    paths, digests = {}, {}
    for value in ("drop-a", "keep-a", "drop-b", "keep-b", "orphan"):
        path, digest, _ = write_archive(value.encode(), tmp_path)
        os.utime(path, (old, old))
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


def test_compact_dry_run_then_execute_is_idempotent(tmp_path):
    db_path = tmp_path / "pagewatch.sqlite"
    archive_dir = tmp_path / "archive"
    init_db(db_path)
    with connect(db_path) as connection:
        ids = []
        for version, text in ((1, "one"), (2, "two"), (3, "three")):
            path, digest, _ = write_archive(text.encode(), archive_dir)
            os.utime(path, (NOW - 10 * 86_400, NOW - 10 * 86_400))
            snapshot = append_snapshot(
                connection, watch_name="a", fetched_at=NOW, status_code=200,
                url="http://127.0.0.1/a", etag=None, last_modified=None,
                content_sha256=f"hash{version}", raw_sha256=digest,
                normalized_text=text, blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
            ids.append(snapshot.id)
        mark_snapshot_major(connection, ids[0])
        connection.commit()
    orphan_path, _, _ = write_archive(b"orphan", archive_dir)
    os.utime(orphan_path, (NOW - 10 * 86_400, NOW - 10 * 86_400))
    rules = replace(Rules(), keep_last=1, major_keep_forever=True,
                    archive_max_age_days=1)

    dry = compact(db_path, archive_dir, rules, NOW, dry_run=True)
    assert dry.executed is False
    assert len(dry.plan.snapshot_ids) == 1  # s2; s1 major, s3 newest
    assert len(dry.plan.archives) == 2     # s2 archive + orphan
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, ids[1]) is not None

    executed = compact(db_path, archive_dir, rules, NOW, dry_run=False)
    assert executed.executed is True
    assert executed.removed_snapshots == 1
    assert executed.removed_archives == 2
    with connect(db_path) as connection:
        assert snapshot_by_id(connection, ids[0]) is not None
        assert snapshot_by_id(connection, ids[1]) is None
        assert snapshot_by_id(connection, ids[2]) is not None
    second = compact(db_path, archive_dir, rules, NOW, dry_run=True)
    assert second.plan.empty is True


def test_event_referenced_snapshots_cover_both_endpoints(tmp_path):
    db_path = tmp_path / "pagewatch.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        ids = []
        for version, text in ((1, "one"), (2, "two"), (3, "three")):
            snapshot = append_snapshot(
                connection, watch_name="a", fetched_at=NOW, status_code=200,
                url="http://127.0.0.1/a", etag=None, last_modified=None,
                content_sha256=f"hash{version}", raw_sha256=None,
                normalized_text=text, blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
            ids.append(snapshot.id)
        insert_event(
            connection, watch_name="a", from_snapshot_id=ids[0],
            to_snapshot_id=ids[1], created_at=NOW, severity="major",
            change_rate=0.5, added_lines=1, removed_lines=1,
            regions=("body",), keyword_hits=(), focus_hit=False,
            summary="major", diff_text="d",
        )
        connection.commit()
        referenced = event_referenced_snapshot_ids(connection)
    assert referenced == {ids[0], ids[1]}


def test_snapshots_to_prune_protects_event_endpoints():
    snapshots = [snap(1, 1), snap(2, 2), snap(3, 3)]
    # event 1 -> 2 keeps both even though keep_last only protects snapshot 3
    pruned = snapshots_to_prune(
        snapshots, major_ids={2}, keep_last=1, major_keep_forever=True,
        referenced_ids={1, 2})
    assert [item.id for item in pruned] == []
    # without the reference set, snapshot 1 would otherwise be prunable
    pruned_without = snapshots_to_prune(
        snapshots, major_ids={2}, keep_last=1, major_keep_forever=True)
    assert [item.id for item in pruned_without] == [1]


def _seed_event_chain(tmp_path, *, count):
    db_path = tmp_path / "pagewatch.sqlite"
    archive_dir = tmp_path / "archive"
    init_db(db_path)
    digests = {}
    with connect(db_path) as connection:
        ids = []
        for version in range(1, count + 1):
            text = f"body-{version}"
            path, digest, _ = write_archive(text.encode(), archive_dir)
            os.utime(path, (NOW - 10 * 86_400, NOW - 10 * 86_400))
            digests[version] = digest
            snapshot = append_snapshot(
                connection, watch_name="a", fetched_at=NOW, status_code=200,
                url="http://127.0.0.1/a", etag=None, last_modified=None,
                content_sha256=f"hash{version}", raw_sha256=digest,
                normalized_text=text, blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
            ids.append(snapshot.id)
        connection.commit()
    return db_path, archive_dir, ids, digests


def test_compact_keeps_event_snapshots_and_their_gzip(tmp_path):
    db_path, archive_dir, ids, digests = _seed_event_chain(tmp_path, count=3)
    with connect(db_path) as connection:
        insert_event(
            connection, watch_name="a", from_snapshot_id=ids[0],
            to_snapshot_id=ids[1], created_at=NOW, severity="major",
            change_rate=0.5, added_lines=1, removed_lines=1,
            regions=("body",), keyword_hits=(), focus_hit=False,
            summary="major", diff_text="d",
        )
        mark_snapshot_major(connection, ids[1])
        connection.commit()
    rules = replace(Rules(), keep_last=1, major_keep_forever=True,
                    archive_max_age_days=1)
    result = compact(db_path, archive_dir, rules, NOW)
    # keep_last only protects snapshot 3, but the event pins 1 and 2
    assert result.plan.snapshot_ids == ()
    assert result.plan.archives == ()
    with connect(db_path) as connection:
        for snapshot_id in ids:
            assert snapshot_by_id(connection, snapshot_id) is not None
    gz_names = {path.name for path in archive_dir.glob("*.gz")}
    for version in (1, 2, 3):
        assert f"{digests[version]}.gz" in gz_names


def test_compact_prunes_only_unreferenced_chain_and_stays_idempotent(tmp_path):
    db_path, archive_dir, ids, digests = _seed_event_chain(tmp_path, count=6)
    with connect(db_path) as connection:
        # minor event 1->2 and major event 4->5; only snapshot 3 is unprotected
        insert_event(
            connection, watch_name="a", from_snapshot_id=ids[0],
            to_snapshot_id=ids[1], created_at=NOW, severity="minor",
            change_rate=0.1, added_lines=1, removed_lines=1,
            regions=("body",), keyword_hits=(), focus_hit=False,
            summary="minor", diff_text="d",
        )
        insert_event(
            connection, watch_name="a", from_snapshot_id=ids[3],
            to_snapshot_id=ids[4], created_at=NOW, severity="major",
            change_rate=0.5, added_lines=2, removed_lines=2,
            regions=("body",), keyword_hits=(), focus_hit=False,
            summary="major", diff_text="d",
        )
        mark_snapshot_major(connection, ids[4])
        connection.commit()
    rules = replace(Rules(), keep_last=2, major_keep_forever=True,
                    archive_max_age_days=1)
    first = compact(db_path, archive_dir, rules, NOW)
    assert first.plan.snapshot_ids == (ids[2],)
    assert [path.name for path in first.plan.archives] == [
        f"{digests[3]}.gz"]
    with connect(db_path) as connection:
        for index in (0, 1, 3, 4, 5):
            assert snapshot_by_id(connection, ids[index]) is not None
        assert snapshot_by_id(connection, ids[2]) is None
    # digest comparison links still resolve to retained snapshots
    with connect(db_path) as connection:
        assert event_referenced_snapshot_ids(connection) == {
            ids[0], ids[1], ids[3], ids[4]}
    second = compact(db_path, archive_dir, rules, NOW)
    assert second.plan.empty is True
    assert second.removed_snapshots == 0
    assert second.removed_archives == 0


def test_build_plan_referenced_ids_feed_archive_protection(tmp_path):
    snapshots = [
        snap(1, 1, "a", raw="hash-1"), snap(2, 2, "a", raw="hash-2"),
        snap(3, 3, "a", raw="hash-3"),
    ]
    old = NOW - 10 * 86_400
    for digest in ("hash-1", "hash-2", "hash-3"):
        path = tmp_path / f"{digest}.gz"
        path.write_bytes(b"x")
        os.utime(path, (old, old))
    plan = build_plan(
        snapshots, major_ids=set(), keep_last=1, major_keep_forever=True,
        archive_dir=tmp_path, now=NOW, archive_max_age_days=1,
        referenced_ids={1, 2})
    # snapshot 3 is keep_last; 1 and 2 are event-referenced -> nothing pruned
    assert plan.snapshot_ids == ()
    assert plan.archives == ()
