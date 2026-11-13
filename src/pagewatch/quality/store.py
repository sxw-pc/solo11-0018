"""Persistence and tolerant read-side helpers for quality assurance.

The readers in this module never assume a column or table exists: databases
created by older pagewatch releases miss the quality tables and may miss
individual columns.  Anything that cannot be inspected is reported back as
"unavailable" so the check layer can emit an UNKNOWN verdict instead of
crashing the round.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ..storage.db import (
    connect, json_dump, json_load, table_columns, table_exists, transaction,
)
from .models import (
    QualityReport, WatchQualityState, WATCH_OK,
)

SNAPSHOT_TABLE = "snapshots"
EVENT_TABLE = "events"
ALERT_TABLE = "alerts"
REPORT_TABLE = "quality_reports"
STATE_TABLE = "quality_watch_states"

# Columns each table needs for the structural checks.
SNAPSHOT_REQUIRED = {
    "id", "watch_name", "version", "fetched_at", "content_sha256",
    "raw_sha256", "is_major",
}
EVENT_REQUIRED = {
    "id", "watch_name", "from_snapshot_id", "to_snapshot_id",
    "created_at", "severity",
}
ALERT_REQUIRED = {"id", "watch_name", "severity", "event_id", "created_at"}


class TableUnavailable(Exception):
    """Raised when an old database lacks a table or one of its columns."""

    def __init__(self, table: str, missing: set[str]) -> None:
        self.table = table
        self.missing = missing
        super().__init__(f"table {table!r} not inspectable (missing {sorted(missing)})")


def _ensure_table(connection: sqlite3.Connection, table: str,
                  required: set[str]) -> set[str]:
    if not table_exists(connection, table):
        raise TableUnavailable(table, set(required))
    columns = table_columns(connection, table)
    missing = required - columns
    if missing:
        raise TableUnavailable(table, missing)
    return columns


def load_snapshots(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_table(connection, SNAPSHOT_TABLE, SNAPSHOT_REQUIRED)
    rows = connection.execute(
        "SELECT id, watch_name, version, fetched_at, content_sha256, "
        "raw_sha256, is_major FROM snapshots "
        "ORDER BY watch_name, version, id"
    ).fetchall()
    return [dict(row) for row in rows]


def load_events(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_table(connection, EVENT_TABLE, EVENT_REQUIRED)
    rows = connection.execute(
        "SELECT id, watch_name, from_snapshot_id, to_snapshot_id, "
        "created_at, severity FROM events ORDER BY id"
    ).fetchall()
    return [dict(row) for row in rows]


def load_alerts(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    _ensure_table(connection, ALERT_TABLE, ALERT_REQUIRED)
    rows = connection.execute(
        "SELECT id, watch_name, severity, event_id, created_at "
        "FROM alerts ORDER BY id"
    ).fetchall()
    return [dict(row) for row in rows]


def next_round_id(connection: sqlite3.Connection) -> str:
    """Allocate a deterministic, collision-free round identifier."""
    if not table_exists(connection, REPORT_TABLE):
        sequence = 0
    else:
        row = connection.execute(
            f"SELECT COUNT(*) AS amount FROM {REPORT_TABLE}"
        ).fetchone()
        sequence = int(row["amount"])
    return f"round-{sequence + 1:06d}"


def save_report(connection: sqlite3.Connection, report: QualityReport) -> int:
    cursor = connection.execute(
        f"INSERT INTO {REPORT_TABLE}(round_id, checked_at, status, content_hash, "
        "watch_names_json, checks_json, findings_json, quarantined_json) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        (
            report.round_id, report.checked_at, report.status,
            report.content_hash, json_dump(list(report.watch_names)),
            json_dump([item.as_dict() for item in report.checks]),
            json_dump([item.as_dict() for item in report.findings]),
            json_dump(list(report.quarantined)),
        ),
    )
    return int(cursor.lastrowid)


def load_report(connection: sqlite3.Connection,
                round_id: str) -> dict[str, Any] | None:
    if not table_exists(connection, REPORT_TABLE):
        return None
    row = connection.execute(
        f"SELECT * FROM {REPORT_TABLE} WHERE round_id=?", (round_id,)
    ).fetchone()
    if row is None:
        return None
    value = dict(row)
    for key in ("watch_names_json", "checks_json", "findings_json",
                "quarantined_json"):
        value[key.replace("_json", "")] = json_load(value.pop(key), [])
    return value


def latest_report_round_id(connection: sqlite3.Connection) -> str | None:
    if not table_exists(connection, REPORT_TABLE):
        return None
    row = connection.execute(
        f"SELECT round_id FROM {REPORT_TABLE} ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["round_id"] if row else None


def list_watch_quality_states(connection: sqlite3.Connection
                              ) -> dict[str, WatchQualityState]:
    if not table_exists(connection, STATE_TABLE):
        return {}
    rows = connection.execute(
        f"SELECT * FROM {STATE_TABLE} ORDER BY watch_name"
    ).fetchall()
    return {
        row["watch_name"]: WatchQualityState(
            watch_name=row["watch_name"], status=row["status"],
            consecutive_failures=int(row["consecutive_failures"]),
            rebaseline_version=row["rebaseline_version"],
            first_failed_round_id=row["first_failed_round_id"],
            last_report_round_id=row["last_report_round_id"],
            updated_at=row["updated_at"],
        )
        for row in rows
    }


def get_watch_quality_state(connection: sqlite3.Connection,
                            watch_name: str) -> WatchQualityState | None:
    return list_watch_quality_states(connection).get(watch_name)


def upsert_watch_quality_state(connection: sqlite3.Connection, *,
                               watch_name: str, status: str,
                               consecutive_failures: int,
                               rebaseline_version: int | None,
                               first_failed_round_id: str | None,
                               last_report_round_id: str | None,
                               updated_at: float) -> None:
    connection.execute(
        f"INSERT INTO {STATE_TABLE}(watch_name, status, consecutive_failures, "
        "rebaseline_version, first_failed_round_id, last_report_round_id, "
        "updated_at) VALUES(?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(watch_name) DO UPDATE SET status=excluded.status, "
        "consecutive_failures=excluded.consecutive_failures, "
        "rebaseline_version=excluded.rebaseline_version, "
        "first_failed_round_id=excluded.first_failed_round_id, "
        "last_report_round_id=excluded.last_report_round_id, "
        "updated_at=excluded.updated_at",
        (watch_name, status, consecutive_failures, rebaseline_version,
         first_failed_round_id, last_report_round_id, updated_at),
    )


def persist_round_outcome(db_path: str | Path, report: QualityReport,
                          states: dict[str, WatchQualityState]) -> None:
    """Write the report and the new per-watch states in one transaction.

    Old databases that were never re-initialised get the quality tables
    created on demand here; the source-of-truth ledger tables are untouched.
    """
    from ..storage.db import init_db

    init_db(db_path)  # idempotent: CREATE TABLE IF NOT EXISTS, no data changes
    with connect(db_path) as connection, transaction(connection):
        save_report(connection, report)
        for state in states.values():
            upsert_watch_quality_state(
                connection, watch_name=state.watch_name, status=state.status,
                consecutive_failures=state.consecutive_failures,
                rebaseline_version=state.rebaseline_version,
                first_failed_round_id=state.first_failed_round_id,
                last_report_round_id=state.last_report_round_id,
                updated_at=state.updated_at,
            )
        connection.execute(
            "INSERT INTO audit_log(at, kind, watch_name, message, details_json) "
            "VALUES(?, ?, NULL, ?, ?)",
            (report.checked_at, f"quality.{report.status}",
             f"round {report.round_id} verification: {report.status}",
             json_dump({
                 "round_id": report.round_id,
                 "quarantined": list(report.quarantined),
                 "findings": len(report.findings),
                 "content_hash": report.content_hash,
             })),
        )


def ok_state(watch_name: str, round_id: str, now: float,
             previous: WatchQualityState | None) -> WatchQualityState:
    return WatchQualityState(
        watch_name=watch_name, status=WATCH_OK, consecutive_failures=0,
        # A clean round does not erase an existing rebaseline anchor; the
        # anchor bounds chain checks until the chain proves itself again.
        rebaseline_version=previous.rebaseline_version if previous else None,
        first_failed_round_id=None, last_report_round_id=round_id,
        updated_at=now,
    )
