"""Stable-cursor historical queries and incremental JSONL export.

This package is deliberately read-only towards the monitoring pipeline: it
never inserts or mutates events, snapshots or watches.  It only reads the
``events`` table plus the lifecycle metadata already stored in ``watches``.

Public entry points:

* :class:`~pagewatch.history.query.HistoryQuery` - filters (watch names,
  tags, severity union, half-open time range);
* :func:`~pagewatch.history.query.query_events` - one page of results plus a
  continuation cursor;
* :func:`~pagewatch.history.export.export_jsonl` - cursor-driven JSONL export
  keyed by ``(watch_name, event_seq)``.
"""

from .cursor import Cursor
from .errors import (
    CursorError, CursorFilterMismatch, CursorMalformed, CursorOutOfRange,
    ExportError, HistoryError, InvalidFilterError, QueryError, RangeError,
)
from .export import (
    ExportBatch, event_record, export_jsonl, read_cursor_file, record_key,
    write_cursor_file, write_cursor_state,
)
from .query import (
    DEFAULT_LIMIT, MAX_LIMIT, HistoryQuery, HistoryResult, WatchDegradation,
    query_events,
)
from .tags import (
    STATE_ACTIVE, STATE_CLOSED, STATE_DELETED, STATE_UNKNOWN, TagSnapshot,
    WatchMeta, load_tag_snapshot,
)

__all__ = [
    "Cursor",
    "CursorError",
    "CursorFilterMismatch",
    "CursorMalformed",
    "CursorOutOfRange",
    "ExportBatch",
    "ExportError",
    "HistoryError",
    "HistoryQuery",
    "HistoryResult",
    "InvalidFilterError",
    "QueryError",
    "RangeError",
    "STATE_ACTIVE",
    "STATE_CLOSED",
    "STATE_DELETED",
    "STATE_UNKNOWN",
    "TagSnapshot",
    "WatchDegradation",
    "WatchMeta",
    "event_record",
    "export_jsonl",
    "load_tag_snapshot",
    "query_events",
    "read_cursor_file",
    "record_key",
    "write_cursor_file",
    "write_cursor_state",
]
