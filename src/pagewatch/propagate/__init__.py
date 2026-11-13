"""Change-propagation attribution across watch items.

This package is a read-only analysis layer on top of the existing stores:
it reads the event ledger (``events``), the snapshot chain (``snapshots``,
specifically the stored normalized blocks) and nothing else.  It never
fetches, never re-normalizes pages, and never writes back into those stores.

Degradation contract
--------------------
Attribution must never fail on thin history:

* zero events            -> ``degraded`` with reason ``no_events``
* a single watch in scope -> ``degraded`` with reason ``single_watch_scope``
* an event whose snapshots were pruned by retention -> the event is listed
  under ``unattributed`` with reason ``missing_snapshot``; other events are
  still correlated normally

In every degraded or partial case the result stays well-formed: ``links``
and ``clusters`` are simply empty (or partial) and the result-level
``confidence`` is ``None`` when no link exists.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ..clock import utc_iso
from ..storage.events import list_events
from ..storage.snapshots import snapshot_by_id
from .engine import attribute
from .fingerprint import build_fingerprint
from .model import (
    DEFAULT_MAX_SPAN_SECONDS,
    DEFAULT_MIN_BLOCK_CHARS,
    DEFAULT_WINDOW_SECONDS,
    AttributionResult,
    EventFingerprint,
    PropagationCluster,
    PropagationLink,
    UnattributedEvent,
)

__all__ = [
    "AttributionResult",
    "DEFAULT_MAX_SPAN_SECONDS",
    "DEFAULT_MIN_BLOCK_CHARS",
    "DEFAULT_WINDOW_SECONDS",
    "EventFingerprint",
    "PropagationCluster",
    "PropagationLink",
    "UnattributedEvent",
    "run_attribution",
]


def run_attribution(connection: sqlite3.Connection, *,
                    since: float | None = None,
                    until: float | None = None,
                    watch_names: Iterable[str] | None = None,
                    window_seconds: float = DEFAULT_WINDOW_SECONDS,
                    max_span_seconds: float = DEFAULT_MAX_SPAN_SECONDS,
                    min_block_chars: int = DEFAULT_MIN_BLOCK_CHARS,
                    generated_at: str | None = None) -> AttributionResult:
    """Attribute cross-watch propagation for the events in scope.

    The function is pure with respect to the database: it only reads.  All
    ordering derives from event-carried timestamps, so concurrent or
    out-of-order event insertion does not change the outcome.
    """
    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    if max_span_seconds < window_seconds:
        raise ValueError("max_span_seconds must be >= window_seconds")
    if min_block_chars < 1:
        raise ValueError("min_block_chars must be >= 1")
    events = list_events(connection, since=since, until=until,
                         watch_names=watch_names)
    watch_count = len({event.watch_name for event in events})
    fingerprints: list[EventFingerprint] = []
    unattributed: dict[int, UnattributedEvent] = {}
    for event in events:
        fingerprint = build_fingerprint(
            event,
            snapshot_by_id(connection, event.from_snapshot_id),
            snapshot_by_id(connection, event.to_snapshot_id),
            min_block_chars=min_block_chars,
        )
        if fingerprint is None:
            unattributed[event.id] = UnattributedEvent(
                event.id, event.watch_name, "missing_snapshot")
        elif not fingerprint.has_content:
            unattributed[event.id] = UnattributedEvent(
                event.id, event.watch_name, "no_eligible_blocks")
        else:
            fingerprints.append(fingerprint)
    scope_reasons: list[str] = []
    if not events:
        scope_reasons.append("no_events")
    elif watch_count < 2:
        scope_reasons.append("single_watch_scope")
    partial_reasons: list[str] = []
    if any(item.reason == "missing_snapshot"
           for item in unattributed.values()):
        partial_reasons.append("missing_snapshots")
    if scope_reasons:
        # No cross-watch correlation is possible; every fingerprinted event
        # is reported as unattributed with the scope reason.
        reason = scope_reasons[0]
        for fingerprint in fingerprints:
            unattributed[fingerprint.event_id] = UnattributedEvent(
                fingerprint.event_id, fingerprint.watch_name, reason)
        links: tuple[PropagationLink, ...] = ()
        clusters: tuple[PropagationCluster, ...] = ()
    else:
        links, clusters, cut_event_ids = attribute(
            fingerprints, window_seconds=window_seconds,
            max_span_seconds=max_span_seconds)
        clustered = {event_id for cluster in clusters
                     for event_id in cluster.event_ids}
        for fingerprint in fingerprints:
            if fingerprint.event_id in clustered:
                continue
            reason = ("beyond_max_span"
                      if fingerprint.event_id in cut_event_ids
                      else "no_shared_content")
            unattributed[fingerprint.event_id] = UnattributedEvent(
                fingerprint.event_id, fingerprint.watch_name, reason)
    degrade_reasons = scope_reasons + partial_reasons
    order = {event.id: index for index, event in enumerate(events)}
    ordered_unattributed = tuple(sorted(
        unattributed.values(), key=lambda item: order[item.event_id]))
    if generated_at is None:
        generated_at = utc_iso(max((event.created_at for event in events),
                                   default=0.0))
    return AttributionResult(
        generated_at=generated_at,
        window_seconds=float(window_seconds),
        max_span_seconds=float(max_span_seconds),
        min_block_chars=int(min_block_chars),
        event_count=len(events),
        watch_count=watch_count,
        degraded=bool(degrade_reasons),
        degrade_reasons=tuple(degrade_reasons),
        links=links,
        clusters=clusters,
        unattributed=ordered_unattributed,
    )
