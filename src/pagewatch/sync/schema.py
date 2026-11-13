"""Additive schema for the snapshot/event/outbox replication link.

Nothing in this module alters a pre-existing table.  Old databases created
before the sync feature simply lack these two tables; :func:`ensure_sync_schema`
creates them on demand (during ``push``/``pull``) so every legacy command keeps
working unchanged.  Existing rows are not touched: when a watermark row is
absent the initial watermark is *derived* from the current maximum version /
row id, which makes the first sync start exactly where the old data ends.
"""

from __future__ import annotations

import sqlite3

# Bumped only if a future release changes sync semantics.  Stored in ``meta``
# alongside the base schema version; the base SCHEMA_VERSION in storage.db is
# deliberately left untouched.
SYNC_SCHEMA_VERSION = 1

SYNC_SCHEMA = """
CREATE TABLE IF NOT EXISTS sync_watermarks (
    scope TEXT NOT NULL,
    stream_key TEXT NOT NULL,
    last_value INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (scope, stream_key)
);
CREATE TABLE IF NOT EXISTS sync_batches (
    batch_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    batch_seq INTEGER NOT NULL,
    snap_high INTEGER NOT NULL,
    event_high INTEGER NOT NULL,
    outbox_high INTEGER NOT NULL,
    item_count INTEGER NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sync_batches_state
    ON sync_batches(role, state, batch_seq);
"""

# Watermark scopes.  ``snapshot`` rows are keyed per watch name; ``event`` and
# ``outbox`` each have a single global stream keyed by GLOBAL_KEY.
SCOPE_SNAPSHOT = "snapshot"
SCOPE_EVENT = "event"
SCOPE_OUTBOX = "outbox"
GLOBAL_KEY = "*"

# Batch lifecycle states.
STATE_STAGED = "staged"
STATE_APPLIED = "applied"
STATE_FAILED = "failed"

ROLE_SOURCE = "source"
ROLE_TARGET = "target"


def ensure_sync_schema(connection: sqlite3.Connection) -> None:
    """Idempotently create sync tables on a database of any age.

    A brand-new peer directory has no database at all, so the base schema is
    applied first; every base statement uses ``IF NOT EXISTS`` and existing
    tables/columns are never touched, which keeps legacy databases and all
    pre-existing commands working unchanged.
    """
    from ..storage.db import SCHEMA as BASE_SCHEMA

    connection.executescript(BASE_SCHEMA)
    connection.executescript(SYNC_SCHEMA)
    connection.execute(
        "INSERT INTO meta(key, value) VALUES('sync_schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SYNC_SCHEMA_VERSION),),
    )


def has_sync_schema(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_watermarks'"
    ).fetchone()
    return row is not None
