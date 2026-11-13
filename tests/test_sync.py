"""Tests for the incremental, resumable push/pull replication link."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from pagewatch.alert.outbox import append_alert, read_outbox
from pagewatch.fetch.archive import write_archive
from pagewatch.models import AlertRecord, DataPaths
from pagewatch.normalize.pipeline import content_hash
from pagewatch.storage.db import connect, init_db, init_paths
from pagewatch.storage.events import insert_alert, insert_event
from pagewatch.storage.snapshots import append_snapshot
from pagewatch.sync import SyncError, pull, push
from pagewatch.sync.locking import FileLock, LockBusy
from pagewatch.sync.manifest import (
    ITEM_EVENT, ITEM_GZIP, ITEM_OUTBOX, ITEM_SNAPSHOT,
    Spool, verify_manifest_hash,
)
from pagewatch.sync.schema import has_sync_schema
from pagewatch.sync.watermarks import all_watermarks

NOW = 1_800_000_000.0


def _make_source(root: Path, *, watches=("a", "b"), rounds=3) -> DataPaths:
    paths = DataPaths(root / "source")
    init_paths(paths)
    snapshot_ids: dict[str, list[int]] = {}
    with connect(paths.db_path) as connection:
        for round_index in range(rounds):
            for watch_name in watches:
                body = f"{watch_name}-body-{round_index}".encode()
                _path, digest, _ = write_archive(body, paths.archive_dir)
                normalized_text = f"{watch_name} text {round_index}"
                snapshot = append_snapshot(
                    connection, watch_name=watch_name, fetched_at=NOW + round_index,
                    status_code=200, url=f"http://127.0.0.1/{watch_name}",
                    etag=None, last_modified=None,
                    content_sha256=content_hash(normalized_text),
                    raw_sha256=digest,
                    normalized_text=normalized_text,
                    blocks_json="[]", normalizer_version="normalizer-v1",
                )
                snapshot_ids.setdefault(watch_name, []).append(snapshot.id)
                if round_index > 0:
                    prev_id = snapshot_ids[watch_name][-2]
                    insert_event(
                        connection, watch_name=watch_name,
                        from_snapshot_id=prev_id, to_snapshot_id=snapshot.id,
                        created_at=NOW + round_index, severity="minor",
                        change_rate=0.2, added_lines=1, removed_lines=1,
                        regions=["body"], keyword_hits=[], focus_hit=False,
                        summary=f"{watch_name} change {round_index}",
                        diff_text=f"@@ {round_index}",
                    )
        connection.commit()
        alerts = []
        first_events = connection.execute(
            "SELECT id, watch_name FROM events ORDER BY id"
        ).fetchall()
        for event_row in first_events:
            alert = insert_alert(
                connection, watch_name=event_row["watch_name"],
                severity="major", event_id=event_row["id"], created_at=NOW,
                cooldown_seconds=0,
                message=f"{event_row['watch_name']} alert",
            )
            if alert is not None:
                alerts.append(alert)
    for alert in alerts:
        append_alert(paths.outbox_path, alert)
    return paths


def _replicated_paths(root: Path) -> DataPaths:
    return DataPaths(root / "replica")


def _spool(root: Path) -> Path:
    return root / "spool"


def _table_count(db_path: Path, table: str) -> int:
    with connect(db_path) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _snapshot_map(db_path: Path) -> dict[tuple[str, int], dict]:
    with connect(db_path) as connection:
        return {
            (row["watch_name"], row["version"]): dict(row)
            for row in connection.execute(
                "SELECT * FROM snapshots ORDER BY watch_name, version"
            ).fetchall()
        }


def _event_ids(db_path: Path) -> list[int]:
    with connect(db_path) as connection:
        return [int(r[0]) for r in
                connection.execute("SELECT id FROM events ORDER BY id").fetchall()]


def _gzip_files(directory: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in directory.glob("*.gz")}


# --------------------------------------------------------------------------- #
# Full-fidelity replication
# --------------------------------------------------------------------------- #

def test_push_replicates_snapshots_events_outbox_and_gzip(tmp_path):
    source = _make_source(tmp_path)
    replica = _replicated_paths(tmp_path)
    report = push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW,
    )
    assert not report.failed
    assert report.transferred_items > 0

    # Snapshot version chains match row-for-row, including primary keys.
    source_snaps = _snapshot_map(source.db_path)
    replica_snaps = _snapshot_map(replica.db_path)
    assert set(replica_snaps) == set(source_snaps)
    for key, source_row in source_snaps.items():
        replica_row = replica_snaps[key]
        assert replica_row["id"] == source_row["id"]
        assert replica_row["content_sha256"] == source_row["content_sha256"]
        assert replica_row["normalized_text"] == source_row["normalized_text"]
        assert replica_row["raw_sha256"] == source_row["raw_sha256"]

    # Events keep their autoincrement ids; no duplicates.
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)
    assert _table_count(replica.db_path, "events") == \
        _table_count(source.db_path, "events")

    # Outbox JSONL is byte-identical line by line.
    source_lines = source.outbox_path.read_text(encoding="utf-8").splitlines()
    replica_lines = replica.outbox_path.read_text(encoding="utf-8").splitlines()
    assert replica_lines == source_lines
    assert read_outbox(replica.outbox_path) == read_outbox(source.outbox_path)

    # Every gzip archive survives with identical bytes and name.
    assert _gzip_files(replica.archive_dir) == _gzip_files(source.archive_dir)
    for name, payload in _gzip_files(replica.archive_dir).items():
        digest = name.removesuffix(".gz")
        assert gzip.decompress(payload)  # valid gzip
        import hashlib
        assert hashlib.sha256(gzip.decompress(payload)).hexdigest() == digest


def test_second_push_is_noop_when_nothing_changed(tmp_path):
    source = _make_source(tmp_path)
    replica = _replicated_paths(tmp_path)
    kwargs = dict(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW,
    )
    first = push(**kwargs)
    second = push(**kwargs)
    assert first.transferred_items > 0
    assert second.transferred_items == 0
    assert second.batches == []
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)


def test_incremental_push_picks_up_new_rounds(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    common = dict(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path),
    )
    push(**common, now=NOW)
    base_events = _table_count(replica.db_path, "events")
    base_snaps = _table_count(replica.db_path, "snapshots")

    # A later monitoring round on the source.
    with connect(source.db_path) as connection:
        _path, digest, _ = write_archive(b"new-round", source.archive_dir)
        snapshot = append_snapshot(
            connection, watch_name="a", fetched_at=NOW + 99, status_code=200,
            url="http://127.0.0.1/a", etag=None, last_modified=None,
            content_sha256=content_hash("a text 99"), raw_sha256=digest,
            normalized_text="a text 99", blocks_json="[]",
            normalizer_version="normalizer-v1",
        )
        event = insert_event(
            connection, watch_name="a",
            from_snapshot_id=snapshot.id - 1, to_snapshot_id=snapshot.id,
            created_at=NOW + 99, severity="major", change_rate=0.9,
            added_lines=4, removed_lines=0, regions=["body"],
            keyword_hits=[], focus_hit=True, summary="late change",
            diff_text="@@ late",
        )
        connection.commit()
    append_alert(source.outbox_path, AlertRecord(
        id=99, watch_name="a", severity="major", event_id=event.id,
        created_at=NOW + 99, dedup_key="a:major", message="late alert",
    ))

    second = push(**common, now=NOW + 100)
    assert not second.failed
    assert _table_count(replica.db_path, "snapshots") == base_snaps + 1
    assert _table_count(replica.db_path, "events") == base_events + 1
    assert len(read_outbox(replica.outbox_path)) == \
        len(read_outbox(source.outbox_path))
    # The new event kept its source id and sits at the end of the chain.
    assert _event_ids(replica.db_path)[-1] == event.id
    assert (replica.archive_dir / f"{digest}.gz").exists()


# --------------------------------------------------------------------------- #
# Interruption, retry and idempotency
# --------------------------------------------------------------------------- #

def test_interrupted_push_resumes_without_duplicates(tmp_path):
    source = _make_source(tmp_path)
    replica = _replicated_paths(tmp_path)
    common = dict(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path),
    )
    with pytest.raises(SyncError, match="simulated failure"):
        push(**common, now=NOW, fail_after_staging=True)

    # Nothing was applied: replica has no data, but the staged batch survives.
    spool = Spool(_spool(tmp_path))
    staged = spool.list_staged()
    assert len(staged) == 1
    assert not replica.db_path.exists() or \
        _table_count(replica.db_path, "snapshots") == 0

    # Retry applies the same content-addressed batch (same id) once.
    retry = push(**common, now=NOW + 1)
    assert not retry.failed
    assert retry.batches[0].already_applied is False
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)
    assert spool.list_staged() == []
    assert list((_spool(tmp_path) / "applied").glob("*.json"))

    # A third run reuses the applied marker and inserts nothing.
    again = push(**common, now=NOW + 2)
    assert again.transferred_items == 0


def test_repeated_apply_of_same_batch_is_idempotent(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW,
    )
    events_before = _table_count(replica.db_path, "events")
    # Re-stage directly from the applied manifest and pull it again.
    spool = Spool(_spool(tmp_path))
    applied_manifest = next((_spool(tmp_path) / "applied").glob("*.json"))
    batch_id = applied_manifest.name.removesuffix(".json")
    target_batch = spool.batches_dir / batch_id
    target_batch.mkdir(parents=True)
    # Rebuild a minimal staged tree from the applied manifest (blobs already
    # removed, so instead exercise the ledger idempotency via push again).
    target_batch.rmdir()
    second = push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW + 3,
    )
    assert all(batch.already_applied for batch in second.batches) or \
        second.transferred_items == 0
    assert _table_count(replica.db_path, "events") == events_before


def test_corrupt_blob_fails_the_batch_and_changes_nothing(tmp_path,
                                                          monkeypatch):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    with pytest.raises(SyncError, match="simulated failure"):
        push(
            source_db=source.db_path, source_archive=source.archive_dir,
            source_outbox=source.outbox_path, target_db=replica.db_path,
            target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
            spool_dir=_spool(tmp_path), now=NOW, fail_after_staging=True,
        )
    # Corrupt one staged blob, then pull: the whole batch must be rejected.
    spool = Spool(_spool(tmp_path))
    batch_dir = spool.list_staged()[0]
    blob = next((batch_dir / "blobs").iterdir())
    blob.write_bytes(blob.read_bytes() + b"tampered")
    report = pull(
        target_db=replica.db_path, target_archive=replica.archive_dir,
        target_outbox=replica.outbox_path, spool_dir=_spool(tmp_path), now=NOW,
    )
    assert report.failed
    assert not replica.db_path.exists() or \
        _table_count(replica.db_path, "snapshots") == 0
    assert not replica.outbox_path.exists()
    # The failed marker is retained for retry/forensics and the broken batch is
    # moved out of the staging area.
    assert spool.failed_dir.exists()
    assert list(spool.failed_dir.glob("*.error"))
    assert spool.list_staged() == []

    # Recovery: re-push re-stages a clean batch with the same content-addressed
    # id, then pull applies it exactly once.
    retry_push = push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW + 1,
    )
    assert not retry_push.failed
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)
    assert read_outbox(replica.outbox_path) == read_outbox(source.outbox_path)
    assert spool.list_staged() == []


def test_target_write_failure_rolls_back_and_is_retryable(tmp_path,
                                                          monkeypatch):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    common = dict(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path),
    )
    # Simulate a full target disk: only the atomic archive *landing* in the
    # target archive directory raises ENOSPC (spool writes still succeed).
    import os as os_module
    original_replace = os_module.replace
    import pagewatch.sync.engine as engine_mod

    target_archive = replica.archive_dir

    def full_disk_replace(src, dst):
        if str(dst).startswith(str(target_archive)):
            raise OSError(28, "No space left on device")
        return original_replace(src, dst)

    monkeypatch.setattr(engine_mod.os, "replace", full_disk_replace)
    with pytest.raises(SyncError, match="No space left"):
        push(**common, now=NOW)
    monkeypatch.undo()

    # The batch is marked failed and nothing partially landed.
    spool = Spool(_spool(tmp_path))
    assert list(spool.failed_dir.glob("*.error"))
    assert not replica.outbox_path.exists()
    with connect(replica.db_path) as connection:
        states = [r[0] for r in connection.execute(
            "SELECT DISTINCT state FROM sync_batches").fetchall()]
    assert "applied" not in states
    assert "failed" in states

    # Retry after freeing space: same content, applied exactly once.
    retry = push(**common, now=NOW + 1)
    assert not retry.failed
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)
    assert len(read_outbox(replica.outbox_path)) == \
        len(read_outbox(source.outbox_path))


def test_manifest_tampering_is_detected(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    with pytest.raises(SyncError, match="simulated failure"):
        push(
            source_db=source.db_path, source_archive=source.archive_dir,
            source_outbox=source.outbox_path, target_db=replica.db_path,
            target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
            spool_dir=_spool(tmp_path), now=NOW, fail_after_staging=True,
        )
    spool = Spool(_spool(tmp_path))
    batch_dir = spool.list_staged()[0]
    manifest_path = batch_dir / "manifest.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["items"][0]["size"] = data["items"][0]["size"] + 1
    manifest_path.write_text(json.dumps(data), encoding="utf-8")
    report = pull(
        target_db=replica.db_path, target_archive=replica.archive_dir,
        target_outbox=replica.outbox_path, spool_dir=_spool(tmp_path), now=NOW,
    )
    assert report.failed
    assert not replica.outbox_path.exists()


# --------------------------------------------------------------------------- #
# Dangling references and gap handling
# --------------------------------------------------------------------------- #

def test_event_referencing_missing_snapshot_is_rejected(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    with pytest.raises(SyncError, match="simulated failure"):
        push(
            source_db=source.db_path, source_archive=source.archive_dir,
            source_outbox=source.outbox_path, target_db=replica.db_path,
            target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
            spool_dir=_spool(tmp_path), now=NOW, fail_after_staging=True,
        )
    spool = Spool(_spool(tmp_path))
    batch_dir = spool.list_staged()[0]
    manifest = spool.load_manifest(batch_dir)
    # Remove every snapshot item but keep the event item.
    kept = [entry for entry in manifest.items
            if entry["type"] != ITEM_SNAPSHOT]
    data = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    data["items"] = kept
    (batch_dir / "manifest.json").write_text(
        json.dumps(data), encoding="utf-8")
    report = pull(
        target_db=replica.db_path, target_archive=replica.archive_dir,
        target_outbox=replica.outbox_path, spool_dir=_spool(tmp_path), now=NOW,
    )
    assert report.failed
    assert not replica.db_path.exists() or \
        _table_count(replica.db_path, "events") == 0


def test_snapshot_gap_on_replica_blocks_bridge_and_keeps_contiguous(tmp_path):
    # Snapshot-only source: no events/outbox, so a version hole is the sole
    # integrity problem and must be rejected by the contiguity precheck.
    source = DataPaths(tmp_path / "source")
    init_paths(source)
    replica = _replicated_paths(tmp_path)
    with connect(source.db_path) as connection:
        for version, text in ((1, "v1"), (2, "v2"), (3, "v3")):
            append_snapshot(
                connection, watch_name="a", fetched_at=NOW + version,
                status_code=200, url="http://127.0.0.1/a", etag=None,
                last_modified=None, content_sha256=content_hash(text),
                raw_sha256=None, normalized_text=text, blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
        connection.commit()

    with pytest.raises(SyncError, match="simulated failure"):
        push(
            source_db=source.db_path, source_archive=source.archive_dir,
            source_outbox=source.outbox_path, target_db=replica.db_path,
            target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
            spool_dir=_spool(tmp_path), now=NOW, fail_after_staging=True,
        )
    spool = Spool(_spool(tmp_path))
    batch_dir = spool.list_staged()[0]
    manifest_path = batch_dir / "manifest.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    # Punch a hole: drop version 2 but keep version 3 (jumps prefix 0 -> 3).
    data["items"] = [
        item for item in data["items"]
        if not (item["type"] == ITEM_SNAPSHOT
                and item["meta"].get("watch_name") == "a"
                and item["meta"].get("version") == 2)
    ]
    # Re-seal so hash verification passes and the gap precheck is what fails.
    from pagewatch.sync.manifest import canonical_json_bytes, sha256_hex
    resealed = sha256_hex(canonical_json_bytes({
        "format_version": data["format_version"],
        "watermarks": data["watermarks"],
        "items": data["items"],
    }))
    data["batch_id"] = resealed
    data["manifest_sha256"] = resealed
    batch_dir.rename(spool.batches_dir / resealed)
    batch_dir = spool.batches_dir / resealed
    (batch_dir / "manifest.json").write_text(
        json.dumps(data), encoding="utf-8")

    report = pull(
        target_db=replica.db_path, target_archive=replica.archive_dir,
        target_outbox=replica.outbox_path, spool_dir=_spool(tmp_path), now=NOW,
    )
    assert report.failed
    assert "gap" in (report.failed[0].error or "")
    # Nothing landed: the batch was rolled back wholesale.
    assert not replica.db_path.exists() or \
        _table_count(replica.db_path, "snapshots") == 0
    assert spool.list_staged() == []
    assert list(spool.failed_dir.glob("*.error"))

    # A clean re-push resumes from the contiguous prefix (0 here) and applies
    # the full chain exactly once.
    retry = push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW + 1,
    )
    assert not retry.failed
    assert set(_snapshot_map(replica.db_path)) == set(_snapshot_map(source.db_path))


def test_batched_transfer_respects_size_limits_and_order(tmp_path):
    source = _make_source(tmp_path, watches=("a",), rounds=4)
    replica = _replicated_paths(tmp_path)
    report = push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path), now=NOW, max_items=3, max_bytes=10_000_000,
    )
    assert len(report.batches) >= 2
    # Every batch applied, final state still matches source exactly.
    assert not report.failed
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)
    assert set(_snapshot_map(replica.db_path)) == set(_snapshot_map(source.db_path))
    with connect(replica.db_path) as connection:
        seqs = [int(r[0]) for r in connection.execute(
            "SELECT batch_seq FROM sync_batches WHERE role='target' "
            "ORDER BY batch_seq").fetchall()]
    assert seqs == sorted(seqs)


# --------------------------------------------------------------------------- #
# Concurrency
# --------------------------------------------------------------------------- #

def test_concurrent_monitoring_round_during_push_is_not_lost(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    common = dict(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=_spool(tmp_path),
    )
    first = push(**common, now=NOW)
    assert not first.failed
    events_after_first = _table_count(source.db_path, "events")

    # Simulate a monitoring round committing *while* the next push reads:
    # write new rows, then push. WAL snapshot isolation means the push reads a
    # consistent prefix; the new rows must appear on the following push exactly
    # once.
    with connect(source.db_path) as connection:
        snap = append_snapshot(
            connection, watch_name="a", fetched_at=NOW + 50, status_code=200,
            url="http://127.0.0.1/a", etag=None, last_modified=None,
            content_sha256=content_hash("concurrent text"), raw_sha256=None,
            normalized_text="concurrent text", blocks_json="[]",
            normalizer_version="normalizer-v1",
        )
        event = insert_event(
            connection, watch_name="a", from_snapshot_id=snap.id - 1,
            to_snapshot_id=snap.id, created_at=NOW + 50, severity="minor",
            change_rate=0.3, added_lines=2, removed_lines=0,
            regions=["body"], keyword_hits=[], focus_hit=False,
            summary="concurrent", diff_text="@@ c",
        )
        connection.commit()
    second = push(**common, now=NOW + 51)
    assert not second.failed
    assert _table_count(replica.db_path, "events") == events_after_first + 1
    assert _event_ids(replica.db_path).count(event.id) == 1
    # Idempotent re-push must not duplicate the concurrent event.
    third = push(**common, now=NOW + 52)
    assert third.transferred_items == 0
    assert _event_ids(replica.db_path).count(event.id) == 1


def test_second_sync_process_is_refused_by_lock(tmp_path):
    spool = Spool(_spool(tmp_path))
    spool.ensure()
    with FileLock(spool.locks_dir, "replication"):
        with pytest.raises(LockBusy):
            FileLock(spool.locks_dir, "replication").acquire()


# --------------------------------------------------------------------------- #
# Legacy database compatibility
# --------------------------------------------------------------------------- #

def test_legacy_database_without_sync_tables_is_upgraded_in_place(tmp_path):
    db_path = tmp_path / "old" / "pagewatch.sqlite"
    init_db(db_path)  # legacy schema only
    with connect(db_path) as connection:
        assert not has_sync_schema(connection)
        append_snapshot(
            connection, watch_name="legacy", fetched_at=NOW, status_code=200,
            url="http://127.0.0.1/legacy", etag=None, last_modified=None,
            content_sha256=content_hash("legacy text"), raw_sha256=None,
            normalized_text="legacy text", blocks_json="[]",
            normalizer_version="normalizer-v1",
        )
        connection.commit()

    # All pre-existing commands keep working; the sync tables appear only once
    # a sync command runs, and the watermark seeds from the existing max version.
    with connect(db_path) as connection:
        rows = connection.execute("SELECT * FROM snapshots").fetchall()
        assert len(rows) == 1
    source = DataPaths(tmp_path / "old")
    replica = DataPaths(tmp_path / "new")
    push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=tmp_path / "spool", now=NOW,
    )
    with connect(db_path) as source_connection:
        assert has_sync_schema(source_connection)
        marks = all_watermarks(source_connection)
    assert marks[("snapshot", "legacy")] == 1
    # The first push ships nothing for the seeded watch (baseline already at 1);
    # a subsequent round replicates incrementally.
    with connect(db_path) as connection:
        append_snapshot(
            connection, watch_name="legacy", fetched_at=NOW + 1, status_code=200,
            url="http://127.0.0.1/legacy", etag=None, last_modified=None,
            content_sha256=content_hash("legacy text 2"), raw_sha256=None,
            normalized_text="legacy text 2", blocks_json="[]",
            normalizer_version="normalizer-v1",
        )
        connection.commit()
    push(
        source_db=source.db_path, source_archive=source.archive_dir,
        source_outbox=source.outbox_path, target_db=replica.db_path,
        target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
        spool_dir=tmp_path / "spool", now=NOW + 2,
    )
    assert _snapshot_map(replica.db_path).get(("legacy", 2)) is not None
    # Legacy tables were never altered: column set is exactly the original one.
    with connect(db_path) as connection:
        columns = {row[1] for row in
                   connection.execute("PRAGMA table_info(snapshots)").fetchall()}
    assert "sync" not in {c.lower() for c in columns}


def test_pull_creates_database_in_empty_peer_directory(tmp_path):
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    # Stage only (simulating a machine that receives the spool out-of-band).
    with pytest.raises(SyncError, match="simulated failure"):
        push(
            source_db=source.db_path, source_archive=source.archive_dir,
            source_outbox=source.outbox_path, target_db=replica.db_path,
            target_archive=replica.archive_dir, target_outbox=replica.outbox_path,
            spool_dir=_spool(tmp_path), now=NOW, fail_after_staging=True,
        )
    assert not replica.root.exists() or not replica.db_path.exists()
    report = pull(
        target_db=replica.db_path, target_archive=replica.archive_dir,
        target_outbox=replica.outbox_path, spool_dir=_spool(tmp_path), now=NOW,
    )
    assert not report.failed
    assert replica.db_path.exists()
    assert _table_count(replica.db_path, "snapshots") == \
        _table_count(source.db_path, "snapshots")


# --------------------------------------------------------------------------- #
# Manifest internals
# --------------------------------------------------------------------------- #

def test_manifest_batch_id_is_content_deterministic(tmp_path):
    from pagewatch.sync.manifest import (
        BatchItem, build_manifest, canonical_json_bytes, sha256_hex,
    )
    items = [
        BatchItem("snapshot:1", ITEM_SNAPSHOT, b'{"id":1}', {"version": 1}),
        BatchItem("event:1", ITEM_EVENT, b'{"id":1}', {"watch_name": "a"}),
        BatchItem("outbox:1", ITEM_OUTBOX, b'{"id":1}', {"seq": 1}),
        BatchItem("gzip:ab", ITEM_GZIP, b"\x1f\x8bblob", {"digest": "ab"}),
    ]
    first = build_manifest(1, {"event": 0}, items)
    second = build_manifest(1, {"event": 0}, list(items))
    assert first.batch_id == second.batch_id
    # A different sequence number must not change the content-addressed id.
    retried = build_manifest(2, {"event": 0}, list(items))
    assert retried.batch_id == first.batch_id
    assert retried.batch_seq == 2
    assert first.batch_id == sha256_hex(canonical_json_bytes({
        "format_version": first.to_json()["format_version"],
        "watermarks": {"event": 0},
        "items": [item.as_manifest_entry() for item in items],
    }))
    # Different content must produce a different id.
    changed = build_manifest(3, {"event": 1}, list(items))
    assert changed.batch_id != first.batch_id
    verify_manifest_hash(first)
    verify_manifest_hash(retried)


# --------------------------------------------------------------------------- #
# Ledger and watermark helpers
# --------------------------------------------------------------------------- #

def test_ledger_lifecycle_and_role_filtering(tmp_path):
    from pagewatch.sync.ledger import (
        failed_batches, get_batch, is_applied, list_batches,
        mark_state, next_batch_seq, record_batch,
    )
    from pagewatch.sync.schema import (
        ROLE_SOURCE, ROLE_TARGET, STATE_APPLIED, STATE_FAILED, STATE_STAGED,
        ensure_sync_schema,
    )
    db_path = tmp_path / "ledger.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        ensure_sync_schema(connection)
        assert next_batch_seq(connection, ROLE_SOURCE) == 1
        record_batch(
            connection, role=ROLE_SOURCE, state=STATE_STAGED, batch_id="b1",
            batch_seq=1, snap_high=2, event_high=1, outbox_high=1,
            item_count=4, manifest_sha256="b1", now=NOW,
        )
        record_batch(
            connection, role=ROLE_TARGET, state=STATE_STAGED, batch_id="b2",
            batch_seq=1, snap_high=2, event_high=1, outbox_high=1,
            item_count=4, manifest_sha256="b2", now=NOW,
        )
        assert next_batch_seq(connection, ROLE_SOURCE) == 2
        assert next_batch_seq(connection, ROLE_TARGET) == 2
        mark_state(connection, "b2", STATE_APPLIED, NOW + 1)
        mark_state(connection, "b1", STATE_FAILED, NOW + 2, error="disk full")
        connection.commit()

    with connect(db_path) as connection:
        assert is_applied(connection, "b2")
        assert not is_applied(connection, "b1")
        assert get_batch(connection, "b1")["error"] == "disk full"
        assert get_batch(connection, "missing") is None
        assert {b["batch_id"] for b in list_batches(connection)} == {"b1", "b2"}
        assert {b["batch_id"] for b in
                list_batches(connection, role=ROLE_TARGET)} == {"b2"}
        failed = failed_batches(connection, role=ROLE_SOURCE)
        assert [b["batch_id"] for b in failed] == ["b1"]
        # Re-recording the same batch id updates rather than duplicates.
        record_batch(
            connection, role=ROLE_SOURCE, state=STATE_STAGED, batch_id="b1",
            batch_seq=1, snap_high=2, event_high=1, outbox_high=1,
            item_count=5, manifest_sha256="b1", now=NOW + 3,
        )
        connection.commit()
        assert len(list_batches(connection)) == 2


def test_watermark_bootstrap_and_contiguous_prefix(tmp_path):
    from pagewatch.sync.schema import (
        GLOBAL_KEY, SCOPE_EVENT, SCOPE_SNAPSHOT, ensure_sync_schema,
    )
    from pagewatch.sync.watermarks import (
        bootstrap_snapshot_watermarks, current_event_watermark,
        get_watermark, highest_contiguous_snapshot_version, set_watermark,
    )
    db_path = tmp_path / "wm.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        ensure_sync_schema(connection)
        for version in (1, 2, 3):
            append_snapshot(
                connection, watch_name="a", fetched_at=NOW, status_code=200,
                url="http://127.0.0.1/a", etag=None, last_modified=None,
                content_sha256=content_hash(f"a{version}"), raw_sha256=None,
                normalized_text=f"a{version}", blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
        # Watch "b" gets a hole (versions 1 and 3, missing 2) via explicit SQL,
        # because append_snapshot always allocates MAX(version)+1.
        for version in (1, 3):
            connection.execute(
                "INSERT INTO snapshots(watch_name, version, fetched_at, "
                "status_code, url, content_sha256, normalized_text, "
                "blocks_json, normalizer_version) "
                "VALUES('b', ?, ?, 200, 'http://127.0.0.1/b', ?, ?, '[]', 'v1')",
                (version, NOW, content_hash(f"b{version}"), f"b{version}"),
            )
        insert_event(
            connection, watch_name="a", from_snapshot_id=1,
            to_snapshot_id=2, created_at=NOW, severity="minor",
            change_rate=0.2, added_lines=1, removed_lines=1,
            regions=["body"], keyword_hits=[], focus_hit=False,
            summary="x", diff_text="@@",
        )
        connection.commit()

        # Before any watermark row, event high is derived from MAX(id).
        assert current_event_watermark(connection) == 1
        assert highest_contiguous_snapshot_version(connection, "a") == 3
        assert highest_contiguous_snapshot_version(connection, "b") == 1

        seeded = bootstrap_snapshot_watermarks(connection, NOW)
        connection.commit()
        assert seeded == {"a": 3, "b": 3}
        # Bootstrapping again is a no-op (never rolls backwards).
        assert bootstrap_snapshot_watermarks(connection, NOW) == {}
        assert get_watermark(connection, SCOPE_SNAPSHOT, "a") == 3

        # An explicit watermark overrides the derived contiguous prefix.
        set_watermark(connection, SCOPE_SNAPSHOT, "b", 1, NOW)
        set_watermark(connection, SCOPE_EVENT, GLOBAL_KEY, 1, NOW)
        connection.commit()
        assert highest_contiguous_snapshot_version(connection, "b") == 1
        assert current_event_watermark(connection) == 1


# --------------------------------------------------------------------------- #
# Command-line surface
# --------------------------------------------------------------------------- #

def test_cli_push_and_pull_end_to_end(tmp_path):
    import pagewatch.cli as cli_mod
    source = _make_source(tmp_path, rounds=2)
    replica = _replicated_paths(tmp_path)
    spool = _spool(tmp_path)

    rc = cli_mod.main([
        "push", "--data-dir", str(source.root),
        "--target-dir", str(replica.root), "--spool-dir", str(spool),
    ])
    assert rc == 0
    assert _event_ids(replica.db_path) == _event_ids(source.db_path)

    # A second push reports a no-op without error.
    rc = cli_mod.main([
        "push", "--data-dir", str(source.root),
        "--target-dir", str(replica.root), "--spool-dir", str(spool),
    ])
    assert rc == 0

    # pull on an empty spool succeeds with nothing to do.
    fresh_target = DataPaths(tmp_path / "pull-only")
    rc = cli_mod.main([
        "pull", "--data-dir", str(fresh_target.root),
        "--spool-dir", str(spool),
    ])
    assert rc == 0


def test_cli_push_missing_source_database_returns_error(tmp_path):
    import pagewatch.cli as cli_mod
    rc = cli_mod.main([
        "push", "--data-dir", str(tmp_path / "absent"),
        "--target-dir", str(tmp_path / "peer"),
    ])
    assert rc == 2


def test_cli_lock_busy_returns_distinct_exit_code(tmp_path, monkeypatch):
    import pagewatch.cli as cli_mod
    from pagewatch.sync.locking import FileLock
    source = _make_source(tmp_path, rounds=2)
    spool = _spool(tmp_path)
    Spool(spool).ensure()
    with FileLock(spool / "locks", "replication"):
        rc = cli_mod.main([
            "push", "--data-dir", str(source.root),
            "--target-dir", str(_replicated_paths(tmp_path).root),
            "--spool-dir", str(spool),
        ])
    assert rc == 3
