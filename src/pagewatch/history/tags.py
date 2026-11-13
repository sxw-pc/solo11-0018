"""Watch-name/tag resolution with graceful lifecycle degradation.

Tag filters are resolved against a *snapshot* of watch metadata taken when a
round starts.  Two sources are merged:

* the database ``watches`` table (last known name/tags; pages never delete
  this row implicitly);
* optionally, the current watchlist YAML.  When provided it is authoritative
  for lifecycle: listed+enabled -> ``active``, listed+disabled -> ``closed``,
  absent -> ``deleted`` (the row is kept so history remains queryable).

Absent a watchlist, a watch that still has a ``watches`` row is ``active`` and
one that only appears in ``events`` is ``unknown``.  In every non-active state
the query module sets ``degraded=true`` and still returns the full historical
record instead of dropping or erroring.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

from ..models import WatchSpec
from ..storage.db import json_load

STATE_ACTIVE = "active"
STATE_CLOSED = "closed"
STATE_DELETED = "deleted"
STATE_UNKNOWN = "unknown"


@dataclass(frozen=True)
class WatchMeta:
    name: str
    tags: tuple[str, ...] = ()
    state: str = STATE_ACTIVE
    source: str = "database"

    @property
    def degraded(self) -> bool:
        return self.state != STATE_ACTIVE


@dataclass(frozen=True)
class TagSnapshot:
    """Immutable view of watch metadata used for one pagination round."""

    metas: Mapping[str, WatchMeta]

    def meta(self, name: str) -> WatchMeta:
        found = self.metas.get(name)
        if found is not None:
            return found
        # No metadata at all: history is still fully queryable by explicit
        # name, but tags/lifecycle are unknowable.
        return WatchMeta(name=name, state=STATE_UNKNOWN, source="events")

    def names_for_tags(self, tags: Iterable[str]) -> frozenset[str]:
        wanted = set(tags)
        if not wanted:
            return frozenset()
        return frozenset(meta.name for meta in self.metas.values()
                         if wanted.intersection(meta.tags))

    def unknown_tags(self, tags: Iterable[str]) -> list[str]:
        wanted = set(tags)
        known = {tag for meta in self.metas.values() for tag in meta.tags}
        return sorted(wanted - known)

    def as_dict(self) -> dict[str, dict[str, object]]:
        return {
            name: {"tags": list(meta.tags), "state": meta.state,
                   "source": meta.source, "degraded": meta.degraded}
            for name, meta in sorted(self.metas.items())
        }


def _database_metas(connection: sqlite3.Connection) -> dict[str, WatchMeta]:
    rows = connection.execute("SELECT name, tags_json FROM watches").fetchall()
    return {
        row["name"]: WatchMeta(
            name=row["name"],
            tags=tuple(str(tag) for tag in json_load(row["tags_json"], [])),
            state=STATE_ACTIVE, source="database",
        )
        for row in rows
    }


def load_tag_snapshot(connection: sqlite3.Connection,
                     watches: Iterable[WatchSpec] | None = None) -> TagSnapshot:
    """Build a metadata snapshot from the DB and an optional watchlist.

    ``watches=None`` means the caller had no watchlist; lifecycle beyond
    presence in the ``watches`` table is then reported as ``unknown``.
    """
    database = _database_metas(connection)
    if watches is None:
        return TagSnapshot(metas=MappingProxyType(database))

    configured = {watch.name: watch for watch in watches}
    metas: dict[str, WatchMeta] = {}
    for name, watch in configured.items():
        state = STATE_ACTIVE if watch.enabled else STATE_CLOSED
        metas[name] = WatchMeta(name=name, tags=tuple(watch.tags),
                                state=state, source="config")
    for name, meta in database.items():
        if name in metas:
            continue
        # Removed from the current watchlist but the events/watch row remain.
        metas[name] = WatchMeta(name=name, tags=meta.tags,
                                state=STATE_DELETED, source="database")
    return TagSnapshot(metas=MappingProxyType(metas))
