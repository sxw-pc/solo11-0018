"""Snapshot version chains, probe records and watch/schedule state."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence

from ..models import Snapshot, WatchSpec
from .db import json_dump

SNAPSHOT_COLUMNS = (
    "id, watch_name, version, fetched_at, status_code, url, etag, last_modified, "
    "content_sha256, raw_sha256, normalized_text, blocks_json, "
    "normalizer_version, is_major"
)


def _snapshot(row: sqlite3.Row) -> Snapshot:
    return Snapshot(
        id=row["id"], watch_name=row["watch_name"], version=row["version"],
        fetched_at=row["fetched_at"], status_code=row["status_code"], url=row["url"],
        etag=row["etag"], last_modified=row["last_modified"],
        content_sha256=row["content_sha256"], raw_sha256=row["raw_sha256"],
        normalized_text=row["normalized_text"], blocks_json=row["blocks_json"],
        normalizer_version=row["normalizer_version"], is_major=bool(row["is_major"]),
    )


def upsert_watch(connection: sqlite3.Connection, watch: WatchSpec) -> None:
    connection.execute(
        "INSERT INTO watches(name, url, tags_json) VALUES(?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET url=excluded.url, tags_json=excluded.tags_json",
        (watch.name, watch.url, json_dump(list(watch.tags))),
    )


def get_watch_state(connection: sqlite3.Connection, name: str) -> dict[str, object] | None:
    row = connection.execute("SELECT * FROM watches WHERE name=?", (name,)).fetchone()
    if row is None:
        return None
    return {
        "name": row["name"], "url": row["url"], "etag": row["etag"],
        "last_modified": row["last_modified"],
        "last_checked_at": row["last_checked_at"],
        "last_success_at": row["last_success_at"],
        "last_status": row["last_status"], "last_error": row["last_error"],
    }


def latest_snapshot(connection: sqlite3.Connection, watch_name: str) -> Snapshot | None:
    row = connection.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE watch_name=? "
        "ORDER BY version DESC LIMIT 1",
        (watch_name,),
    ).fetchone()
    return _snapshot(row) if row else None


def snapshot_by_id(connection: sqlite3.Connection, snapshot_id: int) -> Snapshot | None:
    row = connection.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE id=?", (snapshot_id,)
    ).fetchone()
    return _snapshot(row) if row else None


def list_snapshots(connection: sqlite3.Connection, watch_name: str) -> list[Snapshot]:
    rows = connection.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE watch_name=? ORDER BY version",
        (watch_name,),
    ).fetchall()
    return [_snapshot(row) for row in rows]


def all_snapshots(connection: sqlite3.Connection) -> list[Snapshot]:
    rows = connection.execute(
        f"SELECT {SNAPSHOT_COLUMNS} FROM snapshots ORDER BY watch_name, version"
    ).fetchall()
    return [_snapshot(row) for row in rows]


def append_snapshot(connection: sqlite3.Connection, *, watch_name: str,
                    fetched_at: float, status_code: int, url: str,
                    etag: str | None, last_modified: str | None,
                    content_sha256: str, raw_sha256: str | None,
                    normalized_text: str, blocks_json: str,
                    normalizer_version: str) -> Snapshot:
    row = connection.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next_version "
        "FROM snapshots WHERE watch_name=?",
        (watch_name,),
    ).fetchone()
    version = int(row["next_version"])
    cursor = connection.execute(
        "INSERT INTO snapshots(watch_name, version, fetched_at, status_code, url, "
        "etag, last_modified, content_sha256, raw_sha256, normalized_text, "
        "blocks_json, normalizer_version) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (watch_name, version, fetched_at, status_code, url, etag, last_modified,
         content_sha256, raw_sha256, normalized_text, blocks_json,
         normalizer_version),
    )
    created = snapshot_by_id(connection, int(cursor.lastrowid))
    if created is None:
        raise RuntimeError("failed to read freshly inserted snapshot")
    return created


def mark_snapshot_major(connection: sqlite3.Connection, snapshot_id: int) -> None:
    connection.execute("UPDATE snapshots SET is_major=1 WHERE id=?", (snapshot_id,))


def raw_hashes_referenced(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT DISTINCT raw_sha256 FROM snapshots WHERE raw_sha256 IS NOT NULL"
    ).fetchall()
    return {row["raw_sha256"] for row in rows}


def major_snapshot_ids(connection: sqlite3.Connection) -> set[int]:
    rows = connection.execute(
        "SELECT DISTINCT to_snapshot_id FROM events WHERE severity='major'"
    ).fetchall()
    return {int(row["to_snapshot_id"]) for row in rows}


def delete_snapshots(connection: sqlite3.Connection, ids: Iterable[int]) -> int:
    values = [int(item) for item in ids]
    if not values:
        return 0
    before = connection.total_changes
    connection.executemany("DELETE FROM snapshots WHERE id=?", [(item,) for item in values])
    return connection.total_changes - before


def record_probe(connection: sqlite3.Connection, *, watch_name: str,
                 checked_at: float, status_code: int | None,
                 etag: str | None = None, last_modified: str | None = None,
                 not_modified: bool = False, elapsed_ms: int | None = None,
                 error: str | None = None, raw_sha256: str | None = None) -> int:
    cursor = connection.execute(
        "INSERT INTO probes(watch_name, checked_at, status_code, etag, "
        "last_modified, not_modified, elapsed_ms, error, raw_sha256) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (watch_name, checked_at, status_code, etag, last_modified,
         int(not_modified), elapsed_ms, error, raw_sha256),
    )
    return int(cursor.lastrowid)


def record_fetch_state(connection: sqlite3.Connection, *, name: str, at: float,
                       status_code: int | None, success: bool,
                       etag: str | None = None, last_modified: str | None = None,
                       error: str | None = None,
                       replace_validators: bool = True) -> None:
    if replace_validators:
        connection.execute(
            "UPDATE watches SET etag=?, last_modified=?, last_checked_at=?, "
            "last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END, "
            "last_status=?, last_error=? WHERE name=?",
            (etag, last_modified, at, int(success), at, status_code, error, name),
        )
    else:
        connection.execute(
            "UPDATE watches SET last_checked_at=?, "
            "last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END, "
            "last_status=?, last_error=? WHERE name=?",
            (at, int(success), at, status_code, error, name),
        )


def upsert_schedule(connection: sqlite3.Connection, *, watch_name: str,
                    next_due_at: float, consecutive_failures: int,
                    last_run_at: float, last_outcome: str,
                    updated_at: float | None = None) -> None:
    connection.execute(
        "INSERT INTO schedule_state(watch_name, next_due_at, consecutive_failures, "
        "last_run_at, last_outcome, updated_at) VALUES(?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(watch_name) DO UPDATE SET next_due_at=excluded.next_due_at, "
        "consecutive_failures=excluded.consecutive_failures, "
        "last_run_at=excluded.last_run_at, last_outcome=excluded.last_outcome, "
        "updated_at=excluded.updated_at",
        (watch_name, next_due_at, consecutive_failures, last_run_at,
         last_outcome, updated_at if updated_at is not None else last_run_at),
    )


def get_schedule(connection: sqlite3.Connection,
                 watch_name: str) -> dict[str, object] | None:
    row = connection.execute(
        "SELECT * FROM schedule_state WHERE watch_name=?", (watch_name,)
    ).fetchone()
    return dict(row) if row else None


def list_schedules(connection: sqlite3.Connection) -> dict[str, dict[str, object]]:
    rows = connection.execute("SELECT * FROM schedule_state").fetchall()
    return {row["watch_name"]: dict(row) for row in rows}


def probe_rows(connection: sqlite3.Connection,
               watch_name: str) -> Sequence[sqlite3.Row]:
    return connection.execute(
        "SELECT * FROM probes WHERE watch_name=? ORDER BY id", (watch_name,)
    ).fetchall()
