"""Filtering and deterministic keyset pagination for change history.

Ordering key
------------

Pages are ordered by the triple ``(created_at, watch_name, id)``.  ``id`` is
``INTEGER PRIMARY KEY AUTOINCREMENT``, so it is monotonic, unique and never
reused even after retention deletes old rows.  ``watch_name`` in the middle
guarantees a defined order when several watches change at the very same
instant.

Cursor modes
------------

* **Live round** - the cursor carries ``watermark`` (inclusive high-water id
  captured when the round started) and ``position`` (the last emitted
  ``(created_at, watch_name, id)``).  The query reads only ``id <= watermark``
  and rows strictly after ``position``; concurrent inserts get higher ids and
  are invisible to the running round, so they cannot push already-returned
  rows around or make the page containing them skip data.
* **End of round** - when no more rows exist under the watermark the new
  cursor drops ``position``/``watermark`` and keeps only ``after`` bounded
  from below by that watermark.  The next pull therefore starts a *fresh*
  snapshot and picks up events written concurrently during the previous
  round without repeating any of them.
* **Tag rounds** additionally freeze ``resolved_names`` for the whole round
  so a watch being renamed, re-tagged, closed or deleted mid-round cannot
  make rows appear or disappear underneath the reader.

Range semantics are half-open ``[since, until)``, applied to ``created_at``.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable

from ..models import ChangeEvent
from ..storage.events import EVENT_COLUMNS, _event
from .cursor import Cursor
from .errors import (CursorFilterMismatch, CursorOutOfRange,
                     InvalidFilterError, RangeError)
from .reader import read_snapshot
from .tags import TagSnapshot, load_tag_snapshot

VALID_SEVERITIES = ("noise", "minor", "major")
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


def _finite(value: object) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(float(value)))


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: list[str] = []
    for item in items:
        if item not in seen:
            seen.append(item)
    return tuple(seen)


def _sort_key(event: ChangeEvent) -> tuple[float, str, int]:
    return (float(event.created_at), event.watch_name, int(event.id))


@dataclass(frozen=True)
class HistoryQuery:
    """Immutable filter set.  Multi-value filters are unions (OR)."""

    watch_names: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    severities: tuple[str, ...] = ()
    since: float | None = None
    until: float | None = None
    tag_match: str = "any"

    def __post_init__(self) -> None:
        object.__setattr__(self, "watch_names",
                           _dedupe(self.watch_names or ()))
        object.__setattr__(self, "tags", _dedupe(self.tags or ()))
        object.__setattr__(self, "severities",
                           _dedupe(self.severities or ()))
        if self.tag_match not in {"any", "all"}:
            raise InvalidFilterError(
                "tag_match must be 'any' or 'all'",
                detail={"tag_match": self.tag_match})
        for field_name, value in (("since", self.since), ("until", self.until)):
            if value is not None and not _finite(value):
                raise InvalidFilterError(
                    f"{field_name} must be a finite epoch-second number",
                    detail={"field": field_name, "value": repr(value)})
        if (self.since is not None and self.until is not None
                and float(self.since) >= float(self.until)):
            raise RangeError(
                "time range must satisfy since < until (half-open "
                "[since, until))",
                detail={"since": self.since, "until": self.until})
        for severity in self.severities:
            if severity not in VALID_SEVERITIES:
                raise InvalidFilterError(
                    f"unknown severity {severity!r}",
                    detail={"severity": severity,
                            "allowed": list(VALID_SEVERITIES)})
        for tag in self.tags:
            if not isinstance(tag, str) or not tag.strip():
                raise InvalidFilterError(
                    "tags must be non-empty strings",
                    detail={"tag": repr(tag)})
        for name in self.watch_names:
            if not isinstance(name, str) or not name.strip():
                raise InvalidFilterError(
                    "watch_names must be non-empty strings",
                    detail={"watch_name": repr(name)})

    def with_window(self, since: float | None,
                    until: float | None) -> "HistoryQuery":
        return HistoryQuery(
            watch_names=self.watch_names, tags=self.tags,
            severities=self.severities, since=since, until=until,
            tag_match=self.tag_match)

    def fingerprint(self) -> str:
        """Digest of the membership filters that define a cursor scope.

        The time window is deliberately excluded: it is validated against the
        cursor's recorded position at use time and reported as
        ``cursor_out_of_range``, which distinguishes "the token belongs to a
        different selection" from "the token points outside this window".
        """
        canonical = {
            "watch_names": sorted(self.watch_names),
            "tags": sorted(self.tags),
            "tag_match": self.tag_match,
            "severities": sorted(self.severities),
        }
        digest = hashlib.sha256(json.dumps(
            canonical, ensure_ascii=False, sort_keys=True,
            separators=(",", ":")).encode("utf-8")).hexdigest()
        return "q1:" + digest[:32]

    def as_dict(self) -> dict[str, object]:
        return {
            "watch_names": list(self.watch_names),
            "tags": list(self.tags),
            "tag_match": self.tag_match,
            "severities": list(self.severities),
            "since": self.since, "until": self.until,
        }


@dataclass(frozen=True)
class WatchDegradation:
    name: str
    state: str
    source: str
    tags: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {"watch_name": self.name, "state": self.state,
                "source": self.source, "tags": list(self.tags),
                "degraded": True}


@dataclass(frozen=True)
class HistoryResult:
    events: tuple[ChangeEvent, ...]
    next_cursor: str
    has_more: bool
    exhausted: bool
    query: HistoryQuery
    watermark: int
    next_after: int
    tag_snapshot: TagSnapshot | None = field(default=None, repr=False)
    degraded_watches: tuple[WatchDegradation, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "events": [event.as_json() for event in self.events],
            "next_cursor": self.next_cursor,
            "has_more": self.has_more,
            "exhausted": self.exhausted,
            "watermark": self.watermark,
            "next_after": self.next_after,
            "query": self.query.as_dict(),
            "degraded_watches": [item.as_dict()
                                 for item in self.degraded_watches],
        }


def high_water(connection: sqlite3.Connection) -> int:
    """High-water event id for a new snapshot (read-only, one statement)."""
    row = connection.execute(
        "SELECT COALESCE(MAX(id), 0) AS top FROM events").fetchone()
    return int(row["top"])


def _effective_names(query: HistoryQuery,
                     snapshot: TagSnapshot,
                     resolved: tuple[str, ...] | None) -> set[str] | None:
    if not query.watch_names and not query.tags:
        return None
    names = set(query.watch_names)
    if query.tags:
        tag_names = (set(resolved) if resolved is not None
                     else set(snapshot.names_for_tags(query.tags)))
        if query.tag_match == "all" and not query.watch_names:
            # "all" applies only to tag sets: the intersection of watches
            # matching every requested tag.
            tag_names = {
                name for name in tag_names
                if all(tag in snapshot.meta(name).tags for tag in query.tags)}
        names.update(tag_names)
    return names


def _build_sql(query: HistoryQuery, *, names: set[str] | None,
               empty_scope: bool, live_watermark: int | None,
               lower_after: int,
               position: tuple[float, str, int] | None,
               limit: int) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if live_watermark is not None:
        clauses.append("id <= ?")
        params.append(live_watermark)
    if lower_after:
        # Exclusive checkpoint carried across rounds.  Events written after
        # the previous round always have a higher AUTOINCREMENT id.
        clauses.append("id > ?")
        params.append(lower_after)
    if empty_scope:
        clauses.append("0")
    elif names is not None:
        ordered = sorted(names)
        clauses.append(f"watch_name IN ({', '.join('?' for _ in ordered)})")
        params.extend(ordered)
    if query.severities:
        clauses.append(
            f"severity IN ({', '.join('?' for _ in query.severities)})")
        params.extend(query.severities)
    if query.since is not None:
        clauses.append("created_at >= ?")
        params.append(float(query.since))
    if query.until is not None:
        clauses.append("created_at < ?")
        params.append(float(query.until))
    if position is not None:
        # Strict successor in (created_at, watch_name, id) lexicographic
        # order.  An explicit OR chain is used because SQLite cannot index
        # a three-column row-value comparison against booleans portably.
        clauses.append(
            "(created_at > ? OR (created_at = ? AND "
            "(watch_name > ? OR (watch_name = ? AND id > ?))))")
        params.extend([position[0], position[0], position[1], position[1],
                       position[2]])
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    sql = (f"SELECT {EVENT_COLUMNS} FROM events{where} "
           "ORDER BY created_at, watch_name, id LIMIT ?")
    params.append(limit)
    return sql, params


def _validate_cursor(cursor: Cursor, query: HistoryQuery,
                     watermark: int) -> None:
    if cursor.query_fingerprint != query.fingerprint():
        raise CursorFilterMismatch(
            "cursor was issued for a different filter set",
            detail={"cursor_query": cursor.query_fingerprint,
                    "this_query": query.fingerprint()})
    if cursor.watermark is not None and cursor.watermark > watermark:
        raise CursorOutOfRange(
            "cursor snapshot is ahead of this database (was the database "
            "replaced or truncated?)",
            detail={"cursor_watermark": cursor.watermark,
                    "database_watermark": watermark})
    if query.since is not None and cursor.after > 0:
        anchor = (cursor.position[0] if cursor.position is not None
                  else None)
        if anchor is not None and anchor < float(query.since):
            raise CursorOutOfRange(
                "cursor position precedes the requested 'since' bound",
                detail={"cursor_at": anchor, "since": query.since})
        if anchor is None and cursor.watermark is not None:
            raise CursorOutOfRange(
                "cursor is unusable inside a bounded 'since' window",
                detail={"cursor_after": cursor.after, "since": query.since})
    if (query.until is not None and cursor.position is not None
            and cursor.position[0] >= float(query.until)):
        raise CursorOutOfRange(
            "cursor position is at or beyond the requested 'until' bound",
            detail={"cursor_at": cursor.position[0], "until": query.until})


def _degradations(query: HistoryQuery, names: set[str] | None,
                  snapshot: TagSnapshot) -> tuple[WatchDegradation, ...]:
    if not (query.tags or query.watch_names) or names is None:
        return ()
    out: list[WatchDegradation] = []
    for name in sorted(names):
        meta = snapshot.meta(name)  # missing metadata -> state "unknown"
        if meta.degraded:
            out.append(WatchDegradation(name=name, state=meta.state,
                                        source=meta.source, tags=meta.tags))
    return tuple(out)


def query_events(connection: sqlite3.Connection, query: HistoryQuery, *,
                 cursor: str | None = None, limit: int = DEFAULT_LIMIT,
                 tag_snapshot: TagSnapshot | None = None) -> HistoryResult:
    """Return one page plus a cursor for the next page.

    This function never writes.  Callers own connection lifetime and should
    open the database read-only for audit use.
    """
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise InvalidFilterError("limit must be an integer",
                                 detail={"limit": repr(limit)})
    if not 1 <= limit <= MAX_LIMIT:
        raise InvalidFilterError(f"limit must be between 1 and {MAX_LIMIT}",
                                 detail={"limit": limit})
    with read_snapshot(connection):
        snapshot = (tag_snapshot if tag_snapshot is not None
                    else load_tag_snapshot(connection))
        watermark = high_water(connection)
        decoded = Cursor.decode(cursor) if cursor is not None else None
        if decoded is not None:
            _validate_cursor(decoded, query, watermark)

        if decoded is not None and decoded.finished:
            # End-of-round cursor: begin a fresh snapshot over new rows.
            live_watermark = watermark
            position = None
            lower_after = decoded.after
        elif decoded is not None:
            live_watermark = decoded.watermark
            position = decoded.position
            lower_after = decoded.after
        else:
            # First page of a round: pin the same inclusive high-water mark
            # that governs every following page, so an insert racing this
            # first SELECT cannot leak into the round.
            live_watermark = watermark
            position = None
            lower_after = 0

        resolved = (decoded.resolved_names if decoded is not None
                    and decoded.resolved_names is not None
                    else (tuple(sorted(snapshot.names_for_tags(query.tags)))
                          if query.tags else None))
        names = _effective_names(query, snapshot, resolved)
        # If a tag query resolves to no watches, exclude everything explicitly
        # instead of silently dropping the filter and returning the world.
        empty_scope = bool(names is not None and not names)
        sql, params = _build_sql(query, names=names,
                                 empty_scope=empty_scope,
                                 live_watermark=live_watermark,
                                 lower_after=lower_after,
                                 position=position, limit=limit + 1)
        rows = connection.execute(sql, params).fetchall()
    events = [_event(row) for row in rows[:limit]]
    has_more = len(rows) > limit
    top = live_watermark if live_watermark is not None else watermark
    if events and has_more:
        # Same frozen round continues: rows with id > watermark cannot join.
        next_cursor = Cursor(query_fingerprint=query.fingerprint(),
                             after=lower_after, watermark=top,
                             position=_sort_key(events[-1]),
                             resolved_names=resolved).encode()
        next_after = top
    elif events:
        # Round drained: next pull starts a new snapshot after `top` so it
        # sees concurrent inserts exactly once and never repeats this page.
        next_after = max(top, lower_after)
        next_cursor = Cursor(query_fingerprint=query.fingerprint(),
                             after=next_after).encode()
    else:
        # Nothing matched.  `top` is our checkpoint for later inserts.
        next_after = max(top, lower_after)
        next_cursor = Cursor(query_fingerprint=query.fingerprint(),
                             after=next_after).encode()

    return HistoryResult(
        events=tuple(events), next_cursor=next_cursor, has_more=has_more,
        exhausted=not has_more, query=query, watermark=watermark,
        next_after=next_after, tag_snapshot=snapshot,
        degraded_watches=_degradations(query, names, snapshot),
    )
