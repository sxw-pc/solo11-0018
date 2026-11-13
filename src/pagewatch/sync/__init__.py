"""Incremental, resumable replication of snapshots, events and the outbox.

The public surface is intentionally small: :func:`push` ships missing windows
from a local data directory to a peer directory through a content-addressed
spool, and :func:`pull` applies batches another process staged there.  Both
are safe to interrupt and retry; see :mod:`pagewatch.sync.engine` and
:mod:`pagewatch.sync.manifest` for the protocol and its trade-offs.
"""

from __future__ import annotations

from .engine import (
    DEFAULT_MAX_BYTES, DEFAULT_MAX_ITEMS, SyncError, SyncReport,
    apply_batch, pull, push,
)
from .schema import (
    SCOPE_EVENT, SCOPE_OUTBOX, SCOPE_SNAPSHOT, SYNC_SCHEMA_VERSION,
    ensure_sync_schema,
)

__all__ = [
    "push", "pull", "apply_batch", "SyncError", "SyncReport",
    "ensure_sync_schema", "SYNC_SCHEMA_VERSION",
    "SCOPE_EVENT", "SCOPE_OUTBOX", "SCOPE_SNAPSHOT",
    "DEFAULT_MAX_BYTES", "DEFAULT_MAX_ITEMS",
]
