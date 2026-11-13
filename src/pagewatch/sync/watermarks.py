"""Contiguous-watermark bookkeeping for the three replicated streams.

A watermark means *every value up to and including ``last_value`` has been
durably handled* — nothing is recorded for sparse progress.  When the local
ledger has no row yet (fresh replica, or a legacy database upgraded in place),
the watermark is derived from the data itself:

* snapshots: per-watch maximum ``version`` (0 when the watch is unknown);
* events:    global maximum ``events.id`` (0 when the table is empty);
* outbox:    number of complete JSONL lines, because the JSONL file has no ids.

Deriving rather than assuming zero is what lets an old, populated database join
the mesh without re-shipping its whole history, while a genuinely empty
replica still starts from 0.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ..alert.outbox import read_outbox
from .schema import GLOBAL_KEY, SCOPE_EVENT, SCOPE_SNAPSHOT


def get_watermark(connection: sqlite3.Connection, scope: str,
                  stream_key: str) -> int | None:
    row = connection.execute(
        "SELECT last_value FROM sync_watermarks WHERE scope=? AND stream_key=?",
        (scope, stream_key),
    ).fetchone()
    return int(row["last_value"]) if row else None


def set_watermark(connection: sqlite3.Connection, scope: str, stream_key: str,
                  value: int, now: float) -> None:
    connection.execute(
        "INSERT INTO sync_watermarks(scope, stream_key, last_value, updated_at) "
        "VALUES(?, ?, ?, ?) ON CONFLICT(scope, stream_key) DO UPDATE SET "
        "last_value=excluded.last_value, updated_at=excluded.updated_at",
        (scope, stream_key, int(value), now),
    )


def all_watermarks(connection: sqlite3.Connection) -> dict[tuple[str, str], int]:
    rows = connection.execute(
        "SELECT scope, stream_key, last_value FROM sync_watermarks"
    ).fetchall()
    return {(row["scope"], row["stream_key"]): int(row["last_value"])
            for row in rows}


def _max_snapshot_versions(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT watch_name, MAX(version) AS v FROM snapshots GROUP BY watch_name"
    ).fetchall()
    return {row["watch_name"]: int(row["v"]) for row in rows}


def bootstrap_snapshot_watermarks(connection: sqlite3.Connection,
                                  now: float) -> dict[str, int]:
    """Seed per-watch snapshot watermarks from existing rows.

    Only watches without a watermark row are touched, so the operation is safe
    to call repeatedly and never rolls replication backwards.
    """
    seeded: dict[str, int] = {}
    for watch_name, version in _max_snapshot_versions(connection).items():
        if get_watermark(connection, SCOPE_SNAPSHOT, watch_name) is None:
            set_watermark(connection, SCOPE_SNAPSHOT, watch_name, version, now)
            seeded[watch_name] = version
    return seeded


def current_event_watermark(connection: sqlite3.Connection) -> int:
    stored = get_watermark(connection, SCOPE_EVENT, GLOBAL_KEY)
    if stored is not None:
        return stored
    row = connection.execute("SELECT COALESCE(MAX(id), 0) AS v FROM events").fetchone()
    return int(row["v"])


def current_outbox_watermark(outbox_path: str | Path) -> int:
    """Number of complete JSONL lines; the outbox stream's contiguous mark."""
    return len(read_outbox(outbox_path))


def snapshot_watermark_map(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        key[1]: value
        for key, value in all_watermarks(connection).items()
        if key[0] == SCOPE_SNAPSHOT
    }


def highest_contiguous_snapshot_version(
    connection: sqlite3.Connection, watch_name: str,
) -> int:
    """Largest N such that versions 1..N all exist for a watch.

    The replica only ever accepts gap-free prefixes, so a hole (e.g. a manually
    deleted row) makes replication resume *after* the last contiguous version
    instead of blindly inserting beyond the gap and creating dangling event
    references.
    """
    stored = get_watermark(connection, SCOPE_SNAPSHOT, watch_name)
    if stored is not None:
        return stored
    rows = connection.execute(
        "SELECT version FROM snapshots WHERE watch_name=? ORDER BY version",
        (watch_name,),
    ).fetchall()
    contiguous = 0
    for row in rows:
        if int(row["version"]) != contiguous + 1:
            break
        contiguous += 1
    return contiguous
