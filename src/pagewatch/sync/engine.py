"""Replication engine: incremental push/pull over a local spool directory.

Protocol summary
================
1. The source reads its SQLite snapshot chain, event ledger and JSONL outbox
   and turns every missing window into *content-addressed batches* (see
   :mod:`pagewatch.sync.manifest`).
2. Each batch is staged into the shared spool directory, then applied to the
   target inside one SQLite transaction plus one atomic outbox rewrite.
3. Watermarks advance only after a batch is durable.  A failure marks the
   batch ``failed`` with a retryable error message; the next run resumes from
   the last contiguous watermark — never from an empty database.

Concurrency
-----------
A single advisory flock on the spool serializes all sync runs on a machine.
Source reads run on a WAL snapshot: monitoring rounds that commit while a push
is in flight simply belong to the *next* batch, so the advancing high-water
mark can neither lose nor duplicate their events.  Every inserted row carries
its original explicit primary key, and target inserts are guarded by
existence checks, so re-applying a retried batch is a no-op.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from ..alert.outbox import read_outbox
from ..normalize.pipeline import blocks_from_json, content_hash
from ..storage.db import connect, transaction
from .ledger import (
    STATE_APPLIED, STATE_FAILED, ROLE_TARGET,
    get_batch, mark_state, next_batch_seq, record_batch,
)
from .locking import held_lock
from .manifest import (
    ITEM_EVENT, ITEM_GZIP, ITEM_OUTBOX, ITEM_SNAPSHOT,
    BatchItem, BatchManifest, ManifestError, Spool,
    build_manifest, gzip_item, event_item, outbox_item, snapshot_item,
    verify_item_payload, verify_manifest_hash,
)
from .schema import (
    GLOBAL_KEY, ROLE_SOURCE, SCOPE_EVENT, SCOPE_OUTBOX, SCOPE_SNAPSHOT,
    STATE_STAGED, ensure_sync_schema,
)
from .watermarks import (
    bootstrap_snapshot_watermarks, current_event_watermark,
    get_watermark, highest_contiguous_snapshot_version, set_watermark,
)

SNAPSHOT_ROW_COLUMNS = (
    "id, watch_name, version, fetched_at, status_code, url, etag, "
    "last_modified, content_sha256, raw_sha256, normalized_text, "
    "blocks_json, normalizer_version, is_major"
)
EVENT_ROW_COLUMNS = (
    "id, watch_name, from_snapshot_id, to_snapshot_id, created_at, severity, "
    "change_rate, added_lines, removed_lines, regions_json, "
    "keyword_hits_json, focus_hit, summary, diff_text"
)

DEFAULT_MAX_ITEMS = 500
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
SYNC_LOCK_NAME = "replication"


class SyncError(RuntimeError):
    """A retryable replication failure (disk full, corrupt batch, gap, ...)."""


@dataclass
class BatchReport:
    batch_id: str
    state: str
    item_count: int
    error: str | None = None
    already_applied: bool = False


@dataclass
class SyncReport:
    action: str
    spool: Path
    batches: list[BatchReport] = field(default_factory=list)
    watermarks: dict[str, int] = field(default_factory=dict)

    @property
    def transferred_items(self) -> int:
        return sum(batch.item_count for batch in self.batches
                   if not batch.already_applied and batch.state == STATE_APPLIED)

    @property
    def failed(self) -> list[BatchReport]:
        return [batch for batch in self.batches if batch.state == STATE_FAILED]

    def render(self) -> str:
        lines = [f"{self.action}: spool={self.spool}",
                 f"  batches: {len(self.batches)} "
                 f"(applied now: {sum(1 for b in self.batches if b.state == STATE_APPLIED and not b.already_applied)}, "
                 f"already applied: {sum(1 for b in self.batches if b.already_applied)}, "
                 f"failed: {len(self.failed)})",
                 f"  items applied this run: {self.transferred_items}"]
        for scope, value in sorted(self.watermarks.items()):
            lines.append(f"  watermark {scope}: {value}")
        for batch in self.failed:
            lines.append(f"  FAILED {batch.batch_id[:12]}: {batch.error}")
        return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Source-side batch construction
# --------------------------------------------------------------------------- #

def _snapshot_rows_since(connection: sqlite3.Connection,
                         target_versions: dict[str, int]
                         ) -> list[sqlite3.Row]:
    rows = connection.execute(
        f"SELECT {SNAPSHOT_ROW_COLUMNS} FROM snapshots ORDER BY watch_name, version"
    ).fetchall()
    missing: list[sqlite3.Row] = []
    for row in rows:
        if int(row["version"]) > target_versions.get(row["watch_name"], 0):
            missing.append(row)
    return missing


def _event_rows_since(connection: sqlite3.Connection,
                      event_high: int) -> list[sqlite3.Row]:
    return connection.execute(
        f"SELECT {EVENT_ROW_COLUMNS} FROM events WHERE id > ? ORDER BY id",
        (event_high,),
    ).fetchall()


def _outbox_lines_since(outbox_path: Path,
                        outbox_high: int) -> list[tuple[int, bytes]]:
    if not outbox_path.exists():
        return []
    lines: list[tuple[int, bytes]] = []
    with outbox_path.open("rb") as handle:
        for seq, raw in enumerate(handle, start=1):
            if seq <= outbox_high:
                continue
            stripped = raw.rstrip(b"\r\n")
            if stripped:
                lines.append((seq, stripped))
    return lines


def _gzip_bytes(archive_dir: Path, digest: str) -> bytes:
    path = archive_dir / f"{digest}.gz"
    if not path.exists():
        raise SyncError(
            f"source archive missing for content hash {digest}: {path}"
        )
    return path.read_bytes()


def _target_snapshot_versions(target_db: Path) -> dict[str, int]:
    """Contiguous per-watch versions already present on the target."""
    if not target_db.exists():
        return {}
    versions: dict[str, int] = {}
    with connect(target_db) as connection:
        ensure_sync_schema(connection)
        rows = connection.execute(
            "SELECT DISTINCT watch_name FROM snapshots"
        ).fetchall()
        for row in rows:
            name = row["watch_name"]
            versions[name] = highest_contiguous_snapshot_version(connection, name)
    return versions


def _target_event_high(target_db: Path) -> int:
    if not target_db.exists():
        return 0
    with connect(target_db) as connection:
        ensure_sync_schema(connection)
        return current_event_watermark(connection)


def _target_outbox_high(target_outbox: Path) -> int:
    return len(read_outbox(target_outbox)) if target_outbox.exists() else 0


def _build_items(
    connection: sqlite3.Connection, source_archive: Path,
    source_outbox: Path, target_versions: dict[str, int],
    event_high: int, outbox_high: int,
) -> list[BatchItem]:
    """Assemble batch items on one open connection.

    All SQLite reads run on the caller's connection (inside one transaction in
    WAL mode), so snapshots and events come from the same database snapshot.
    The outbox is an append-only JSONL file whose already-fsynced lines never
    change, so reading it on the same logical window stays consistent.
    """
    items: list[BatchItem] = []
    needed_archives: set[str] = set()
    snapshot_rows = _snapshot_rows_since(connection, target_versions)
    event_rows = _event_rows_since(connection, event_high)
    for row in snapshot_rows:
        if row["raw_sha256"]:
            needed_archives.add(row["raw_sha256"])
    for digest in sorted(needed_archives):
        items.append(gzip_item(digest, _gzip_bytes(source_archive, digest)))
    for row in snapshot_rows:
        items.append(snapshot_item(dict(row)))
    for row in event_rows:
        items.append(event_item(dict(row)))
    for seq, line in _outbox_lines_since(source_outbox, outbox_high):
        items.append(outbox_item(seq, line))
    return items


def _chunk_items(items: list[BatchItem], max_items: int,
                 max_bytes: int) -> list[list[BatchItem]]:
    chunks: list[list[BatchItem]] = []
    current: list[BatchItem] = []
    current_bytes = 0
    for item in items:
        size = len(item.payload)
        if current and (len(current) >= max_items
                        or current_bytes + size > max_bytes):
            chunks.append(current)
            current = []
            current_bytes = 0
        current.append(item)
        current_bytes += size
    if current:
        chunks.append(current)
    return chunks


def _batch_watermarks(items: list[BatchItem],
                      base_snapshot: dict[str, int],
                      base_event: int, base_outbox: int) -> dict[str, object]:
    snapshot_versions = dict(base_snapshot)
    event_high = base_event
    outbox_high = base_outbox
    for item in items:
        if item.item_type == ITEM_SNAPSHOT:
            name = str(item.meta["watch_name"])
            snapshot_versions[name] = max(
                snapshot_versions.get(name, 0), int(item.meta["version"])
            )
        elif item.item_type == ITEM_EVENT:
            event_high = max(event_high, int(item.item_id.split(":", 1)[1]))
        elif item.item_type == ITEM_OUTBOX:
            outbox_high = max(outbox_high, int(item.meta["seq"]))
    return {
        "snapshot": snapshot_versions,
        "event": event_high,
        "outbox": outbox_high,
    }


# --------------------------------------------------------------------------- #
# Target-side verification and atomic application
# --------------------------------------------------------------------------- #

def _parse_payloads(spool: Spool, manifest: BatchManifest,
                    batch_dir: Path) -> list[tuple[dict[str, object], object]]:
    parsed: list[tuple[dict[str, object], object]] = []
    for entry in manifest.items:
        payload = spool.read_blob(batch_dir, str(entry["sha256"]))
        verify_item_payload(entry, payload)
        if entry["type"] == ITEM_GZIP:
            parsed.append((entry, None))
        else:
            parsed.append((entry, json.loads(payload)))
    return parsed


def _verify_snapshot_row(row: dict) -> None:
    required = {"id", "watch_name", "version", "content_sha256",
                "normalized_text", "blocks_json", "raw_sha256"}
    missing = required - row.keys()
    if missing:
        raise ManifestError(f"snapshot row missing fields: {sorted(missing)}")
    if content_hash(str(row["normalized_text"])) != row["content_sha256"]:
        raise ManifestError(
            f"snapshot {row['id']} content_sha256 does not match normalized text"
        )
    # blocks_json must be parseable canonical JSON so the block-hash chain
    # survives the round trip.
    blocks_from_json(str(row["blocks_json"]))


def _verify_event_row(row: dict, known_snapshot_ids: set[int]) -> None:
    required = {"id", "from_snapshot_id", "to_snapshot_id", "watch_name"}
    missing = required - row.keys()
    if missing:
        raise ManifestError(f"event row missing fields: {sorted(missing)}")
    for key in ("from_snapshot_id", "to_snapshot_id"):
        if int(row[key]) not in known_snapshot_ids:
            raise ManifestError(
                f"event {row['id']} references missing snapshot {row[key]} "
                "(would create a dangling reference)"
            )


def _precheck_contiguity(connection: sqlite3.Connection,
                         snapshots: list[dict], events: list[dict],
                         outbox_lines: list[dict],
                         outbox_current_count: int) -> None:
    # Snapshots must extend each watch's gap-free version prefix.
    per_watch: dict[str, list[int]] = {}
    for row in snapshots:
        per_watch.setdefault(str(row["watch_name"]), []).append(int(row["version"]))
    for name, versions in per_watch.items():
        base = highest_contiguous_snapshot_version(connection, name)
        expected = base + 1
        for version in sorted(versions):
            if version < expected:
                continue  # already present; insert is skipped idempotently
            if version != expected:
                raise SyncError(
                    f"snapshot gap for watch {name!r}: have contiguous 1..{base}, "
                    f"batch wants version {version}; refusing to bridge a hole"
                )
            expected += 1
    # Events must be contiguous over the global id space.
    base_event = current_event_watermark(connection)
    expected = base_event + 1
    for row in sorted(events, key=lambda item: int(item["id"])):
        if int(row["id"]) < expected:
            continue
        if int(row["id"]) != expected:
            raise SyncError(
                f"event id gap: have 1..{base_event}, batch wants id {row['id']}"
            )
        expected += 1
    # Outbox lines must extend the existing JSONL prefix.
    expected_seq = outbox_current_count + 1
    for line in sorted(outbox_lines, key=lambda item: int(item["seq"])):
        if int(line["seq"]) < expected_seq:
            continue
        if int(line["seq"]) != expected_seq:
            raise SyncError(
                f"outbox line gap: have {outbox_current_count} lines, "
                f"batch wants seq {line['seq']}"
            )
        expected_seq += 1


def _write_gzip_archives(archive_dir: Path, gzip_items: list[dict],
                         batch_dir: Path) -> int:
    archive_dir.mkdir(parents=True, exist_ok=True)
    created = 0
    spool = Spool(batch_dir.parent.parent)
    for entry, row in gzip_items:
        digest = str(entry["meta"]["digest"])
        destination = archive_dir / f"{digest}.gz"
        if destination.exists():
            if destination.read_bytes() != spool.read_blob(
                    batch_dir, str(entry["sha256"])):
                raise SyncError(
                    f"existing archive {destination.name} has different bytes"
                )
            continue
        payload = spool.read_blob(batch_dir, str(entry["sha256"]))
        fd, temp_name = tempfile.mkstemp(prefix=f".{digest}.", suffix=".tmp",
                                         dir=str(archive_dir))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, destination)
            created += 1
        except OSError:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
            raise
    return created


def _atomic_append_outbox(outbox_path: Path, new_lines: list[bytes]) -> None:
    outbox_path.parent.mkdir(parents=True, exist_ok=True)
    existing = outbox_path.read_bytes() if outbox_path.exists() else b""
    fd, temp_name = tempfile.mkstemp(prefix=".alerts.", suffix=".tmp",
                                     dir=str(outbox_path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(existing)
            for line in new_lines:
                handle.write(line + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, outbox_path)
    except OSError:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise


def _insert_snapshot(connection: sqlite3.Connection, row: dict) -> None:
    exists = connection.execute(
        "SELECT 1 FROM snapshots WHERE watch_name=? AND version=?",
        (row["watch_name"], row["version"]),
    ).fetchone()
    if exists:
        # Idempotent retry: require the stored row to be the same content.
        stored = connection.execute(
            "SELECT id, content_sha256 FROM snapshots WHERE watch_name=? AND version=?",
            (row["watch_name"], row["version"]),
        ).fetchone()
        if stored["content_sha256"] != row["content_sha256"]:
            raise SyncError(
                f"snapshot {row['watch_name']} v{row['version']} already exists "
                "with different content"
            )
        return
    connection.execute(
        "INSERT INTO snapshots(id, watch_name, version, fetched_at, status_code, "
        "url, etag, last_modified, content_sha256, raw_sha256, normalized_text, "
        "blocks_json, normalizer_version, is_major) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (int(row["id"]), row["watch_name"], int(row["version"]),
         float(row["fetched_at"]), int(row["status_code"]), row["url"],
         row["etag"], row["last_modified"], row["content_sha256"],
         row["raw_sha256"], row["normalized_text"], row["blocks_json"],
         row["normalizer_version"], int(bool(row["is_major"]))),
    )


def _insert_event(connection: sqlite3.Connection, row: dict) -> None:
    exists = connection.execute(
        "SELECT 1 FROM events WHERE id=?", (int(row["id"]),)
    ).fetchone()
    if exists:
        return
    connection.execute(
        "INSERT INTO events(id, watch_name, from_snapshot_id, to_snapshot_id, "
        "created_at, severity, change_rate, added_lines, removed_lines, "
        "regions_json, keyword_hits_json, focus_hit, summary, diff_text) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (int(row["id"]), row["watch_name"], int(row["from_snapshot_id"]),
         int(row["to_snapshot_id"]), float(row["created_at"]),
         row["severity"], float(row["change_rate"]), int(row["added_lines"]),
         int(row["removed_lines"]), row["regions_json"],
         row["keyword_hits_json"], int(bool(row["focus_hit"])),
         row["summary"], row["diff_text"]),
    )


def apply_batch(*, target_db: Path, target_archive: Path, target_outbox: Path,
                batch_dir: Path, now: float) -> BatchReport:
    """Verify then atomically ingest one staged batch. Never partially applies."""
    spool = Spool(batch_dir.parent.parent)
    try:
        manifest = spool.load_manifest(batch_dir)
        verify_manifest_hash(manifest)
    except ManifestError as exc:
        # The manifest itself is unreadable, so the directory name (the
        # content hash) is the only identity available.
        batch_id = batch_dir.name
        spool.mark_failed(batch_id, str(exc))
        raise SyncError(f"batch {batch_id[:12]} failed: {exc}") from exc

    existing = None
    if target_db.exists():
        with connect(target_db) as connection:
            ensure_sync_schema(connection)
            existing = get_batch(connection, manifest.batch_id)
    if existing and existing["state"] == STATE_APPLIED:
        spool.mark_applied(manifest.batch_id)
        return BatchReport(manifest.batch_id, STATE_APPLIED,
                           len(manifest.items), already_applied=True)

    try:
        parsed = _parse_payloads(spool, manifest, batch_dir)
        gzip_items = [(e, p) for e, p in parsed if e["type"] == ITEM_GZIP]
        snapshots = [p for e, p in parsed if e["type"] == ITEM_SNAPSHOT]
        events = [p for e, p in parsed if e["type"] == ITEM_EVENT]
        outbox_entries = [(e, p) for e, p in parsed if e["type"] == ITEM_OUTBOX]

        for row in snapshots:
            _verify_snapshot_row(row)

        target_db.parent.mkdir(parents=True, exist_ok=True)
        with connect(target_db) as connection:
            ensure_sync_schema(connection)
            known_ids = {
                int(r["id"]) for r in
                connection.execute("SELECT id FROM snapshots").fetchall()
            }
            known_ids.update(int(row["id"]) for row in snapshots)
            for row in events:
                _verify_event_row(row, known_ids)

            outbox_count = _target_outbox_high(target_outbox)
            known_event_ids = {
                int(r["id"]) for r in
                connection.execute("SELECT id FROM events").fetchall()
            }
            known_event_ids.update(int(row["id"]) for row in events)
            for _entry, line in outbox_entries:
                if "event_id" not in line or "id" not in line:
                    raise ManifestError(
                        "outbox line missing id/event_id fields")
                if int(line["event_id"]) not in known_event_ids:
                    raise ManifestError(
                        f"outbox line {line['id']} references missing event "
                        f"{line['event_id']}"
                    )
            _precheck_contiguity(
                connection, snapshots, events,
                [{"seq": int(e["meta"]["seq"])} for e, _p in outbox_entries],
                outbox_count,
            )

            # Probe free space before writing anything, so a full target disk
            # is reported as a retryable batch failure rather than a partial
            # write.  This is advisory (TOCTOU possible); the atomic
            # transaction below remains the real correctness boundary.
            needed = sum(int(e.get("size", 0))
                         for e, _p in gzip_items) + sum(
                len(spool.read_blob(batch_dir, str(e["sha256"])))
                for e, _p in outbox_entries)
            _check_disk_space(target_db.parent, needed)

            new_outbox_lines: list[bytes] = []
            with transaction(connection):
                _write_gzip_archives(target_archive, gzip_items, batch_dir)
                for row in snapshots:
                    _insert_snapshot(connection, row)
                for row in events:
                    _insert_event(connection, row)
                for entry, row in outbox_entries:
                    if int(entry["meta"]["seq"]) > outbox_count:
                        new_outbox_lines.append(
                            spool.read_blob(batch_dir, str(entry["sha256"]))
                        )
                record_batch(
                    connection, role=ROLE_TARGET, state=STATE_STAGED,
                    batch_id=manifest.batch_id, batch_seq=manifest.batch_seq,
                    snap_high=_max_snapshot_high(manifest),
                    event_high=int(manifest.watermarks["event"]),
                    outbox_high=int(manifest.watermarks["outbox"]),
                    item_count=len(manifest.items),
                    manifest_sha256=manifest.manifest_sha256, now=now,
                )
        # Outbox rewrite happens after the DB commit but before watermarks;
        # a crash here is safe because re-application skips existing rows and
        # recomputes the outbox high-water mark from line count.
        if new_outbox_lines:
            _atomic_append_outbox(target_outbox, new_outbox_lines)
        with connect(target_db) as meta_connection:
            ensure_sync_schema(meta_connection)
            with transaction(meta_connection):
                _advance_watermarks(
                    meta_connection, manifest, snapshots, events,
                    outbox_entries, now,
                )
                mark_state(meta_connection, manifest.batch_id,
                           STATE_APPLIED, now)
    except (OSError, sqlite3.Error, SyncError, ManifestError) as exc:
        _record_failure(target_db, manifest, now, exc)
        spool.mark_failed(manifest.batch_id, str(exc))
        raise SyncError(f"batch {manifest.batch_id[:12]} failed: {exc}") from exc

    spool.mark_applied(manifest.batch_id)
    return BatchReport(manifest.batch_id, STATE_APPLIED, len(manifest.items))


def _max_snapshot_high(manifest: BatchManifest) -> int:
    values = list(manifest.watermarks.get("snapshot", {}).values())
    return max((int(v) for v in values), default=0)


def _advance_watermarks(connection: sqlite3.Connection,
                        manifest: BatchManifest, snapshots: list[dict],
                        events: list[dict],
                        outbox_entries: list[tuple[dict, dict]],
                        now: float) -> None:
    for row in snapshots:
        name = str(row["watch_name"])
        current = get_watermark(connection, SCOPE_SNAPSHOT, name) or 0
        set_watermark(connection, SCOPE_SNAPSHOT, name,
                      max(current, int(row["version"])), now)
    if events:
        set_watermark(connection, SCOPE_EVENT, GLOBAL_KEY,
                      int(manifest.watermarks["event"]), now)
    if outbox_entries:
        set_watermark(connection, SCOPE_OUTBOX, GLOBAL_KEY,
                      int(manifest.watermarks["outbox"]), now)


def _record_failure(target_db: Path, manifest: BatchManifest, now: float,
                    exc: BaseException) -> None:
    if not target_db.exists():
        return
    try:
        with connect(target_db) as connection:
            ensure_sync_schema(connection)
            with transaction(connection):
                record_batch(
                    connection, role=ROLE_TARGET, state=STATE_FAILED,
                    batch_id=manifest.batch_id, batch_seq=manifest.batch_seq,
                    snap_high=_max_snapshot_high(manifest),
                    event_high=int(manifest.watermarks["event"]),
                    outbox_high=int(manifest.watermarks["outbox"]),
                    item_count=len(manifest.items),
                    manifest_sha256=manifest.manifest_sha256, now=now,
                    error=f"{type(exc).__name__}: {exc}",
                )
    except sqlite3.Error:
        pass


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def _check_disk_space(path: Path, needed_bytes: int) -> None:
    target = path if path.exists() else path.parent
    target.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(target).free
    if free < needed_bytes:
        raise SyncError(
            f"not enough disk space at {target}: need {needed_bytes} bytes, "
            f"free {free} bytes"
        )


def push(*, source_db: Path, source_archive: Path, source_outbox: Path,
         target_db: Path, target_archive: Path, target_outbox: Path,
         spool_dir: Path, now: float, max_items: int = DEFAULT_MAX_ITEMS,
         max_bytes: int = DEFAULT_MAX_BYTES,
         fail_after_staging: bool = False) -> SyncReport:
    """Incrementally replicate one source data directory onto a target."""
    spool = Spool(spool_dir)
    spool.ensure()
    report = SyncReport(action="push", spool=spool_dir)
    with held_lock(spool.locks_dir, SYNC_LOCK_NAME):
        with connect(source_db) as source:
            ensure_sync_schema(source)
            bootstrap_snapshot_watermarks(source, now)
            source.commit()
            target_versions = _target_snapshot_versions(target_db)
            event_high = _target_event_high(target_db)
            outbox_high = _target_outbox_high(target_outbox)
            # One explicit read transaction pins a single WAL snapshot for the
            # whole window; rounds that commit meanwhile land in the next batch.
            source.execute("BEGIN")
            try:
                items = _build_items(
                    source, source_archive, source_outbox,
                    target_versions, event_high, outbox_high,
                )
            finally:
                source.rollback()
            chunks = _chunk_items(items, max_items, max_bytes)
            staged: list[tuple[BatchManifest, list[BatchItem]]] = []
            base_snapshot = dict(target_versions)
            for chunk in chunks:
                watermarks = _batch_watermarks(
                    chunk, base_snapshot, event_high, outbox_high)
                seq = next_batch_seq(source, ROLE_SOURCE)
                manifest = build_manifest(seq, watermarks, chunk)
                payload_bytes = sum(len(item.payload) for item in chunk)
                _check_disk_space(spool_dir, payload_bytes)
                spool.stage_batch(manifest, chunk)
                with transaction(source):
                    record_batch(
                        source, role=ROLE_SOURCE, state=STATE_STAGED,
                        batch_id=manifest.batch_id, batch_seq=seq,
                        snap_high=_max_snapshot_high(manifest),
                        event_high=int(watermarks["event"]),
                        outbox_high=int(watermarks["outbox"]),
                        item_count=len(chunk),
                        manifest_sha256=manifest.manifest_sha256, now=now,
                    )
                staged.append((manifest, chunk))
                base_snapshot = dict(watermarks["snapshot"])
                event_high = int(watermarks["event"])
                outbox_high = int(watermarks["outbox"])
        if fail_after_staging:
            raise SyncError("simulated failure after staging (resumable)")
        for manifest, _chunk in staged:
            batch_dir = spool.batch_dir(manifest.batch_id)
            if not batch_dir.exists():
                raise SyncError(
                    f"staged batch vanished before apply: {manifest.batch_id}"
                )
            report.batches.append(apply_batch(
                target_db=target_db, target_archive=target_archive,
                target_outbox=target_outbox, batch_dir=batch_dir, now=now,
            ))
            with connect(source_db) as source_meta:
                ensure_sync_schema(source_meta)
                with transaction(source_meta):
                    mark_state(source_meta, manifest.batch_id,
                               STATE_APPLIED, now)
        report.watermarks = _collect_watermarks(target_db, target_outbox)
    return report


def pull(*, target_db: Path, target_archive: Path, target_outbox: Path,
         spool_dir: Path, now: float) -> SyncReport:
    """Apply every batch already staged in the shared spool."""
    spool = Spool(spool_dir)
    spool.ensure()
    report = SyncReport(action="pull", spool=spool_dir)
    with held_lock(spool.locks_dir, SYNC_LOCK_NAME):
        for batch_dir in spool.list_staged():
            try:
                report.batches.append(apply_batch(
                    target_db=target_db, target_archive=target_archive,
                    target_outbox=target_outbox, batch_dir=batch_dir, now=now,
                ))
            except SyncError as exc:
                report.batches.append(BatchReport(
                    batch_dir.name, STATE_FAILED, 0, error=str(exc)))
                break
        report.watermarks = _collect_watermarks(target_db, target_outbox)
    return report


def _collect_watermarks(target_db: Path, target_outbox: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    if target_db.exists():
        with connect(target_db) as connection:
            ensure_sync_schema(connection)
            result["event"] = current_event_watermark(connection)
            rows = connection.execute(
                "SELECT stream_key, last_value FROM sync_watermarks "
                "WHERE scope=?", (SCOPE_SNAPSHOT,),
            ).fetchall()
            for row in rows:
                result[f"snapshot:{row['stream_key']}"] = int(row["last_value"])
    result["outbox"] = _target_outbox_high(target_outbox)
    return result
