"""Durable batch ledger: which batches exist, their state and retry markers."""

from __future__ import annotations

import sqlite3

from .schema import ROLE_TARGET, STATE_APPLIED, STATE_FAILED


def record_batch(connection: sqlite3.Connection, *, role: str, state: str,
                 batch_id: str, batch_seq: int, snap_high: int,
                 event_high: int, outbox_high: int, item_count: int,
                 manifest_sha256: str, now: float,
                 error: str | None = None) -> None:
    connection.execute(
        "INSERT INTO sync_batches(batch_id, role, state, batch_seq, snap_high, "
        "event_high, outbox_high, item_count, manifest_sha256, error, "
        "created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(batch_id) DO UPDATE SET state=excluded.state, "
        "error=excluded.error, updated_at=excluded.updated_at, "
        "item_count=excluded.item_count",
        (batch_id, role, state, batch_seq, snap_high, event_high, outbox_high,
         item_count, manifest_sha256, error, now, now),
    )


def mark_state(connection: sqlite3.Connection, batch_id: str, state: str,
               now: float, error: str | None = None) -> None:
    connection.execute(
        "UPDATE sync_batches SET state=?, error=?, updated_at=? WHERE batch_id=?",
        (state, error, now, batch_id),
    )


def get_batch(connection: sqlite3.Connection,
              batch_id: str) -> dict[str, object] | None:
    row = connection.execute(
        "SELECT * FROM sync_batches WHERE batch_id=?", (batch_id,)
    ).fetchone()
    return dict(row) if row else None


def is_applied(connection: sqlite3.Connection, batch_id: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sync_batches WHERE batch_id=? AND state=?",
        (batch_id, STATE_APPLIED),
    ).fetchone()
    return row is not None


def next_batch_seq(connection: sqlite3.Connection, role: str) -> int:
    row = connection.execute(
        "SELECT COALESCE(MAX(batch_seq), 0) + 1 AS next_seq "
        "FROM sync_batches WHERE role=?",
        (role,),
    ).fetchone()
    return int(row["next_seq"])


def list_batches(connection: sqlite3.Connection,
                 role: str | None = None) -> list[dict[str, object]]:
    if role is None:
        rows = connection.execute(
            "SELECT * FROM sync_batches ORDER BY batch_seq"
        ).fetchall()
    else:
        rows = connection.execute(
            "SELECT * FROM sync_batches WHERE role=? ORDER BY batch_seq",
            (role,),
        ).fetchall()
    return [dict(row) for row in rows]


def failed_batches(connection: sqlite3.Connection,
                   role: str = ROLE_TARGET) -> list[dict[str, object]]:
    rows = connection.execute(
        "SELECT * FROM sync_batches WHERE role=? AND state=? ORDER BY batch_seq",
        (role, STATE_FAILED),
    ).fetchall()
    return [dict(row) for row in rows]


# Re-exported for callers that only import the ledger.
__all__ = [
    "record_batch", "mark_state", "get_batch", "is_applied",
    "next_batch_seq", "list_batches", "failed_batches",
    "ROLE_TARGET", "STATE_APPLIED", "STATE_FAILED",
]
