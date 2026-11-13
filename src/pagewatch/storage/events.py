"""Change-event, alert, audit and status-ledger persistence."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence

from ..models import AlertRecord, ChangeEvent
from .db import json_dump, json_load

EVENT_COLUMNS = (
    "id, watch_name, from_snapshot_id, to_snapshot_id, created_at, severity, "
    "change_rate, added_lines, removed_lines, regions_json, keyword_hits_json, "
    "focus_hit, summary, diff_text, rule_trace_json"
)


def _event(row: sqlite3.Row) -> ChangeEvent:
    return ChangeEvent(
        id=row["id"], watch_name=row["watch_name"],
        from_snapshot_id=row["from_snapshot_id"],
        to_snapshot_id=row["to_snapshot_id"], created_at=row["created_at"],
        severity=row["severity"], change_rate=row["change_rate"],
        added_lines=row["added_lines"], removed_lines=row["removed_lines"],
        regions=tuple(json_load(row["regions_json"], [])),
        keyword_hits=tuple(json_load(row["keyword_hits_json"], [])),
        focus_hit=bool(row["focus_hit"]), summary=row["summary"],
        diff_text=row["diff_text"],
        rule_trace=row["rule_trace_json"] or "",
    )


def _alert(row: sqlite3.Row) -> AlertRecord:
    return AlertRecord(
        id=row["id"], watch_name=row["watch_name"], severity=row["severity"],
        event_id=row["event_id"], created_at=row["created_at"],
        dedup_key=row["dedup_key"], message=row["message"],
    )


def insert_event(connection: sqlite3.Connection, *, watch_name: str,
                 from_snapshot_id: int, to_snapshot_id: int, created_at: float,
                 severity: str, change_rate: float, added_lines: int,
                 removed_lines: int, regions: Sequence[str],
                 keyword_hits: Sequence[str], focus_hit: bool, summary: str,
                 diff_text: str, rule_trace: str = "") -> ChangeEvent:
    cursor = connection.execute(
        "INSERT INTO events(watch_name, from_snapshot_id, to_snapshot_id, "
        "created_at, severity, change_rate, added_lines, removed_lines, "
        "regions_json, keyword_hits_json, focus_hit, summary, diff_text, "
        "rule_trace_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (watch_name, from_snapshot_id, to_snapshot_id, created_at, severity,
         change_rate, added_lines, removed_lines, json_dump(list(regions)),
         json_dump(list(keyword_hits)), int(focus_hit), summary, diff_text,
         rule_trace),
    )
    row = connection.execute(
        f"SELECT {EVENT_COLUMNS} FROM events WHERE id=?", (cursor.lastrowid,)
    ).fetchone()
    if row is None:
        raise RuntimeError("failed to read freshly inserted event")
    return _event(row)


def get_event(connection: sqlite3.Connection, event_id: int) -> ChangeEvent | None:
    row = connection.execute(
        f"SELECT {EVENT_COLUMNS} FROM events WHERE id=?", (event_id,)
    ).fetchone()
    return _event(row) if row else None


def list_events(connection: sqlite3.Connection, *, since: float | None = None,
                until: float | None = None,
                watch_names: Iterable[str] | None = None) -> list[ChangeEvent]:
    clauses: list[str] = []
    params: list[object] = []
    if since is not None:
        clauses.append("created_at >= ?")
        params.append(since)
    if until is not None:
        clauses.append("created_at <= ?")
        params.append(until)
    names = list(watch_names or [])
    if names:
        clauses.append(f"watch_name IN ({', '.join('?' for _ in names)})")
        params.extend(names)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    rows = connection.execute(
        f"SELECT {EVENT_COLUMNS} FROM events{where} "
        "ORDER BY created_at, watch_name, id",
        params,
    ).fetchall()
    return [_event(row) for row in rows]


def count_events(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT severity, COUNT(*) AS amount FROM events GROUP BY severity"
    ).fetchall()
    return {row["severity"]: int(row["amount"]) for row in rows}


def insert_alert(connection: sqlite3.Connection, *, watch_name: str,
                 severity: str, event_id: int, created_at: float,
                 message: str, cooldown_seconds: int) -> AlertRecord | None:
    """Insert an alert unless the same watch/severity is cooling down."""
    dedup_key = f"{watch_name}:{severity}"
    if cooldown_seconds > 0:
        row = connection.execute(
            "SELECT id FROM alerts WHERE watch_name=? AND severity=? "
            "AND created_at >= ? ORDER BY created_at DESC LIMIT 1",
            (watch_name, severity, created_at - cooldown_seconds),
        ).fetchone()
        if row is not None:
            return None
    cursor = connection.execute(
        "INSERT INTO alerts(watch_name, severity, event_id, created_at, "
        "dedup_key, message) VALUES(?, ?, ?, ?, ?, ?)",
        (watch_name, severity, event_id, created_at, dedup_key, message),
    )
    row = connection.execute(
        "SELECT * FROM alerts WHERE id=?", (cursor.lastrowid,)
    ).fetchone()
    return _alert(row) if row else None


def list_alerts(connection: sqlite3.Connection) -> list[AlertRecord]:
    rows = connection.execute("SELECT * FROM alerts ORDER BY id").fetchall()
    return [_alert(row) for row in rows]


def add_audit(connection: sqlite3.Connection, *, at: float, kind: str,
              message: str, watch_name: str | None = None,
              details: dict[str, object] | None = None) -> int:
    cursor = connection.execute(
        "INSERT INTO audit_log(at, kind, watch_name, message, details_json) "
        "VALUES(?, ?, ?, ?, ?)",
        (at, kind, watch_name, message, json_dump(details or {})),
    )
    return int(cursor.lastrowid)


def list_audit(connection: sqlite3.Connection, kind: str | None = None) -> list[dict[str, object]]:
    if kind:
        rows = connection.execute(
            "SELECT * FROM audit_log WHERE kind=? ORDER BY id", (kind,)
        ).fetchall()
    else:
        rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
    return [dict(row) for row in rows]
