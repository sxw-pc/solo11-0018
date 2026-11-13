from __future__ import annotations

import os

from pagewatch.fetch.archive import write_archive
from pagewatch.models import Rules, Snapshot
from pagewatch.retention.compact import compact
from pagewatch.retention.policy import (
    build_plan, format_plan, snapshots_to_prune,
)
from pagewatch.storage.db import connect, init_db
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
    rules = Rules().model_copy(update={
        "keep_last": 1, "major_keep_forever": True,
        "archive_max_age_days": 1})

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
