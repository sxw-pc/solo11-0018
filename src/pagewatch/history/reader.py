"""Read-only SQLite access helpers for history queries.

The monitoring pipeline keeps writing while audit consumers pull history.  A
snapshot round must therefore see one consistent ``MAX(events.id)`` and be
unaffected by later commits.  Opening the database with ``PRAGMA
query_only=ON`` guarantees the history path cannot mutate the ledger even by
accident, and a deferred read transaction pins the watermark for the whole
page so concurrent WAL writers never disturb it.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

READONLY_URI_PREFIX = "file:"


def connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    """Open ``db_path`` read-only; never creates the file."""
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"database not found: {path}")
    uri = f"{READONLY_URI_PREFIX}{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=15000")
    return connection


@contextmanager
def read_snapshot(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Pin one read view for the duration of a page query.

    The history path issues a small number of independent SELECTs.  Wrapping
    them in a deferred transaction means concurrent ``run-once`` commits in
    WAL mode cannot change ``MAX(id)`` or the visible row set between the
    watermark read and the page read.
    """
    if connection.in_transaction:
        yield connection
        return
    connection.execute("BEGIN")
    try:
        yield connection
    finally:
        connection.execute("ROLLBACK")
