"""SQLite schema, connection helpers and small metadata utilities."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ..models import DataPaths

SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS watches (
    name TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    etag TEXT,
    last_modified TEXT,
    last_checked_at REAL,
    last_success_at REAL,
    last_status INTEGER,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    fetched_at REAL NOT NULL,
    status_code INTEGER NOT NULL,
    url TEXT NOT NULL,
    etag TEXT,
    last_modified TEXT,
    content_sha256 TEXT NOT NULL,
    raw_sha256 TEXT,
    normalized_text TEXT NOT NULL,
    blocks_json TEXT NOT NULL,
    normalizer_version TEXT NOT NULL,
    is_major INTEGER NOT NULL DEFAULT 0,
    UNIQUE(watch_name, version)
);
CREATE INDEX IF NOT EXISTS idx_snapshots_watch
    ON snapshots(watch_name, version DESC);
CREATE TABLE IF NOT EXISTS probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_name TEXT NOT NULL,
    checked_at REAL NOT NULL,
    status_code INTEGER,
    etag TEXT,
    last_modified TEXT,
    not_modified INTEGER NOT NULL DEFAULT 0,
    elapsed_ms INTEGER,
    error TEXT,
    raw_sha256 TEXT
);
CREATE INDEX IF NOT EXISTS idx_probes_watch ON probes(watch_name, checked_at DESC);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_name TEXT NOT NULL,
    from_snapshot_id INTEGER NOT NULL,
    to_snapshot_id INTEGER NOT NULL,
    created_at REAL NOT NULL,
    severity TEXT NOT NULL,
    change_rate REAL NOT NULL,
    added_lines INTEGER NOT NULL,
    removed_lines INTEGER NOT NULL,
    regions_json TEXT NOT NULL,
    keyword_hits_json TEXT NOT NULL,
    focus_hit INTEGER NOT NULL DEFAULT 0,
    summary TEXT NOT NULL,
    diff_text TEXT NOT NULL,
    rule_trace_json TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_watch ON events(watch_name, created_at);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    created_at REAL NOT NULL,
    dedup_key TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_dedup
    ON alerts(watch_name, severity, created_at DESC);
CREATE TABLE IF NOT EXISTS schedule_state (
    watch_name TEXT PRIMARY KEY,
    next_due_at REAL NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_run_at REAL,
    last_outcome TEXT,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    kind TEXT NOT NULL,
    watch_name TEXT,
    message TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_log(at);
CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class _ClosingConnection(sqlite3.Connection):
    """Connection whose context manager also closes the handle."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=15,
                                 factory=_ClosingConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=15000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def init_db(db_path: str | Path) -> None:
    with connect(db_path) as connection:
        connection.executescript(SCHEMA)
        _migrate(connection)
        connection.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
        connection.commit()


def _migrate(connection: sqlite3.Connection) -> None:
    """Bring databases created by an older version up to current.

    Only additive changes are performed, so historical rows keep their values
    and severity.
    """
    event_columns = {
        row["name"] for row in
        connection.execute("PRAGMA table_info(events)").fetchall()
    }
    if "rule_trace_json" not in event_columns:
        connection.execute(
            "ALTER TABLE events ADD COLUMN rule_trace_json TEXT NOT NULL "
            "DEFAULT ''"
        )


def init_paths(paths: DataPaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.archive_dir.mkdir(parents=True, exist_ok=True)
    paths.outbox_path.parent.mkdir(parents=True, exist_ok=True)
    paths.out_dir.mkdir(parents=True, exist_ok=True)
    init_db(paths.db_path)


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def json_load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def set_stat(connection: sqlite3.Connection, key: str, value: Any) -> None:
    connection.execute(
        "INSERT INTO stats(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def get_stat(connection: sqlite3.Connection, key: str,
             default: str | None = None) -> str | None:
    row = connection.execute("SELECT value FROM stats WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default
