"""Cross-watch correlation, direction inference and span-capped clustering.

Ordering guarantees
-------------------
Events are ordered exclusively by the timestamps they carry
(``created_at``), never by storage/insertion order (event ids): a monitoring
round fetches watches concurrently, so the ledger's row order is not the
order in which changes happened.  Every iteration, sort key and tie-break in
this module is explicit, which makes the whole attribution deterministic:
re-running it over the same data yields byte-identical output.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from .model import (
    MAX_SHARED_SAMPLES,
    NAME_FALLBACK_WEIGHT,
    PATH_PREFIX_WEIGHT,
    SAMPLE_MAX_CHARS,
    TIME_DECAY_FLOOR,
    TIMESTAMP_WEIGHT,
    TIMESTAMP_WEIGHT_SPAN,
    EventFingerprint,
    PropagationCluster,
    PropagationLink,
)


@dataclass(frozen=True)
class _RawLink:
    kind: str
    event_a_id: int
    event_b_id: int
    source_event_id: int
    source_watch: str
    source_created_at: float
    downstream_event_id: int
    downstream_watch: str
    downstream_created_at: float
    shared_count: int
    shared_samples: tuple[str, ...]
    time_gap_seconds: float
    confidence: float
    direction_confidence: float
    direction_basis: tuple[str, ...]

    def sort_key(self) -> tuple:
        earlier = min(self.source_created_at, self.downstream_created_at)
        return (earlier, self.kind, self.source_watch, self.downstream_watch,
                self.source_event_id, self.downstream_event_id)


def _path_segments(url: str | None) -> tuple[str, ...]:
    if not url:
        return ()
    try:
        path = urlsplit(url).path
    except ValueError:
        return ()
    return tuple(segment for segment in path.split("/") if segment)


def _is_proper_prefix(shorter: tuple[str, ...], longer: tuple[str, ...]) -> bool:
    return len(shorter) < len(longer) and longer[: len(shorter)] == shorter


def _confidence(shared_count: int, size_a: int, size_b: int,
                time_gap: float, window_seconds: float) -> float:
    """Containment of the smaller change set, decayed by the time gap.

    The decay is linear from 1.0 (simultaneous events) to
    ``TIME_DECAY_FLOOR`` at the window edge: content overlap is the primary
    signal and temporal distance may at most halve it.
    """
    containment = shared_count / max(1, min(size_a, size_b))
    decay = 1.0 - (1.0 - TIME_DECAY_FLOOR) * (time_gap / window_seconds)
    return round(min(containment, 1.0) * decay, 6)


def _direction(fp_a: EventFingerprint, fp_b: EventFingerprint,
               window_seconds: float) -> tuple[EventFingerprint, EventFingerprint,
                                               float, tuple[str, ...]]:
    """Pick a source watch from the signals the events already carry.

    Signals, strongest first: the event's own timestamp (the earlier change
    is the likely origin), then the site path prefix (a shallower page such
    as ``/portal`` is the likely origin of content also seen on
    ``/portal/terms``), and finally the watch name as a purely deterministic
    tie-break.  The basis records which signals actually fired.
    """
    weight_a = 0.0
    weight_b = 0.0
    basis: list[str] = []
    gap = abs(fp_a.created_at - fp_b.created_at)
    if gap > 0:
        weight = TIMESTAMP_WEIGHT + TIMESTAMP_WEIGHT_SPAN * min(
            gap / window_seconds, 1.0)
        if fp_a.created_at < fp_b.created_at:
            weight_a += weight
        else:
            weight_b += weight
        basis.append("timestamp")
    segments_a = _path_segments(fp_a.url)
    segments_b = _path_segments(fp_b.url)
    if segments_a and segments_b:
        if _is_proper_prefix(segments_a, segments_b):
            weight_a += PATH_PREFIX_WEIGHT
            basis.append("path_prefix")
        elif _is_proper_prefix(segments_b, segments_a):
            weight_b += PATH_PREFIX_WEIGHT
            basis.append("path_prefix")
    if weight_a == weight_b:
        if fp_a.watch_name <= fp_b.watch_name:
            weight_a += NAME_FALLBACK_WEIGHT
        else:
            weight_b += NAME_FALLBACK_WEIGHT
        basis.append("name_order_fallback")
    if weight_a >= weight_b:
        return fp_a, fp_b, round(min(weight_a, 1.0), 6), tuple(basis)
    return fp_b, fp_a, round(min(weight_b, 1.0), 6), tuple(basis)


def _samples(shared: list[str], *texts_by_hash: dict[str, str]) -> tuple[str, ...]:
    samples: list[str] = []
    for key in shared:
        text = ""
        for mapping in texts_by_hash:
            if key in mapping:
                text = mapping[key]
                break
        collapsed = " ".join(text.split())
        if len(collapsed) > SAMPLE_MAX_CHARS:
            collapsed = collapsed[: SAMPLE_MAX_CHARS - 1] + "…"
        samples.append(collapsed)
        if len(samples) >= MAX_SHARED_SAMPLES:
            break
    return tuple(samples)


def _pair_links(fp_a: EventFingerprint, fp_b: EventFingerprint,
                time_gap: float, window_seconds: float) -> list[_RawLink]:
    candidates: list[tuple[str, list[str], int, int]] = []
    shared_added = sorted(set(fp_a.added) & set(fp_b.added))
    if shared_added:
        candidates.append(("added", shared_added,
                           len(fp_a.added), len(fp_b.added)))
    shared_removed = sorted(set(fp_a.removed) & set(fp_b.removed))
    if shared_removed:
        candidates.append(("removed", shared_removed,
                           len(fp_a.removed), len(fp_b.removed)))
    moved = sorted((set(fp_a.removed) & set(fp_b.added))
                   | (set(fp_a.added) & set(fp_b.removed)))
    if moved:
        total_a = len(fp_a.added) + len(fp_a.removed)
        total_b = len(fp_b.added) + len(fp_b.removed)
        candidates.append(("moved", moved, total_a, total_b))
    if not candidates:
        return []
    source, downstream, direction_confidence, basis = _direction(
        fp_a, fp_b, window_seconds)
    links: list[_RawLink] = []
    for kind, shared, size_a, size_b in candidates:
        links.append(_RawLink(
            kind=kind, event_a_id=fp_a.event_id, event_b_id=fp_b.event_id,
            source_event_id=source.event_id, source_watch=source.watch_name,
            source_created_at=source.created_at,
            downstream_event_id=downstream.event_id,
            downstream_watch=downstream.watch_name,
            downstream_created_at=downstream.created_at,
            shared_count=len(shared),
            shared_samples=_samples(shared, fp_a.added, fp_a.removed,
                                    fp_b.added, fp_b.removed),
            time_gap_seconds=round(time_gap, 6),
            confidence=_confidence(len(shared), size_a, size_b,
                                   time_gap, window_seconds),
            direction_confidence=direction_confidence,
            direction_basis=basis,
        ))
    return links


def build_links(fingerprints: list[EventFingerprint], *,
                window_seconds: float) -> list[_RawLink]:
    """All pairwise links inside the sliding time window.

    Iteration follows the event-carried timestamps; because the outer list is
    sorted by ``created_at`` the inner loop can stop at the window edge.
    """
    ordered = sorted(fingerprints, key=lambda fp: (
        fp.created_at, fp.watch_name, fp.event_id))
    links: list[_RawLink] = []
    for index, fp_a in enumerate(ordered):
        for fp_b in ordered[index + 1:]:
            gap = fp_b.created_at - fp_a.created_at
            if gap > window_seconds:
                break
            if fp_a.watch_name == fp_b.watch_name:
                continue
            links.extend(_pair_links(fp_a, fp_b, gap, window_seconds))
    return links


def _components(event_ids: list[int],
                links: list[_RawLink]) -> list[list[int]]:
    parent = {event_id: event_id for event_id in event_ids}

    def find(event_id: int) -> int:
        while parent[event_id] != event_id:
            parent[event_id] = parent[parent[event_id]]
            event_id = parent[event_id]
        return event_id

    for link in links:
        root_a, root_b = find(link.event_a_id), find(link.event_b_id)
        if root_a != root_b:
            parent[max(root_a, root_b)] = min(root_a, root_b)
    groups: dict[int, list[int]] = {}
    for event_id in event_ids:
        groups.setdefault(find(event_id), []).append(event_id)
    return [groups[root] for root in sorted(groups)]


def _sorted_group(event_ids: list[int],
                  fp_by_id: dict[int, EventFingerprint]) -> list[int]:
    return sorted(event_ids, key=lambda event_id: (
        fp_by_id[event_id].created_at, fp_by_id[event_id].watch_name,
        event_id))


def _split_by_span(groups: list[list[int]], links: list[_RawLink],
                   fp_by_id: dict[int, EventFingerprint], *,
                   max_span_seconds: float) -> tuple[list[list[int]], set[int]]:
    """Cap cluster spans by cutting links across the largest time gap.

    A chain of pairwise in-window links could otherwise stretch a cluster
    arbitrarily far ("infinite" cross-round association).  Cutting at the
    largest gap — earliest gap on ties — is deterministic, and events that
    lose their last link to a cut are reported back so the caller can mark
    them ``beyond_max_span`` instead of a generic "no match".
    """
    queue: list[list[int]] = [
        _sorted_group(group, fp_by_id) for group in groups]
    final: list[list[int]] = []
    cut_event_ids: set[int] = set()
    while queue:
        group = queue.pop(0)
        if len(group) < 2:
            final.append(group)
            continue
        times = [fp_by_id[event_id].created_at for event_id in group]
        if times[-1] - times[0] <= max_span_seconds:
            final.append(group)
            continue
        best_index, best_gap = 0, -1.0
        for index in range(len(group) - 1):
            gap = times[index + 1] - times[index]
            if gap > best_gap:
                best_index, best_gap = index, gap
        left = set(group[: best_index + 1])
        right = set(group[best_index + 1:])
        group_set = left | right
        kept: list[_RawLink] = []
        for link in links:
            if link.event_a_id not in group_set or link.event_b_id not in group_set:
                continue
            if (link.event_a_id in left) == (link.event_b_id in left):
                kept.append(link)
            else:
                cut_event_ids.add(link.event_a_id)
                cut_event_ids.add(link.event_b_id)
        for side in (sorted(left), sorted(right)):
            side_set = set(side)
            side_links = [link for link in kept
                          if link.event_a_id in side_set
                          and link.event_b_id in side_set]
            for component in _components(side, side_links):
                queue.append(_sorted_group(component, fp_by_id))
    return final, cut_event_ids


def _cluster_sort_key(event_ids: list[int],
                      fp_by_id: dict[int, EventFingerprint]) -> tuple:
    fps = [fp_by_id[event_id] for event_id in event_ids]
    return (min(fp.created_at for fp in fps),
            min(fp.watch_name for fp in fps),
            min(fp.event_id for fp in fps))


def attribute(fingerprints: list[EventFingerprint], *,
              window_seconds: float,
              max_span_seconds: float) -> tuple[tuple[PropagationLink, ...],
                                                tuple[PropagationCluster, ...],
                                                set[int]]:
    """Correlate fingerprints into directed links and propagation clusters."""
    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    if max_span_seconds < window_seconds:
        raise ValueError("max_span_seconds must be >= window_seconds")
    fp_by_id = {fp.event_id: fp for fp in fingerprints}
    raw_links = build_links(fingerprints, window_seconds=window_seconds)
    if not raw_links:
        return (), (), set()
    linked_ids = sorted({link.event_a_id for link in raw_links}
                        | {link.event_b_id for link in raw_links})
    groups = _components(linked_ids, raw_links)
    groups, cut_event_ids = _split_by_span(
        groups, raw_links, fp_by_id, max_span_seconds=max_span_seconds)
    clustered_groups = [group for group in groups if len(group) >= 2]
    clustered_groups.sort(key=lambda group: _cluster_sort_key(group, fp_by_id))
    final_links: list[PropagationLink] = []
    clusters: list[PropagationCluster] = []
    for cluster_index, group in enumerate(clustered_groups, start=1):
        group_set = set(group)
        group_links = sorted(
            (link for link in raw_links
             if link.event_a_id in group_set and link.event_b_id in group_set),
            key=lambda link: link.sort_key(),
        )
        link_ids: list[str] = []
        for raw in group_links:
            link_id = f"link-{len(final_links) + 1}"
            link_ids.append(link_id)
            final_links.append(PropagationLink(
                id=link_id, kind=raw.kind,
                source_event_id=raw.source_event_id,
                source_watch=raw.source_watch,
                downstream_event_id=raw.downstream_event_id,
                downstream_watch=raw.downstream_watch,
                shared_count=raw.shared_count,
                shared_samples=raw.shared_samples,
                time_gap_seconds=raw.time_gap_seconds,
                confidence=raw.confidence,
                direction_confidence=raw.direction_confidence,
                direction_basis=raw.direction_basis,
            ))
        watches = sorted({fp_by_id[event_id].watch_name for event_id in group})
        source_counts = {watch: 0 for watch in watches}
        for raw in group_links:
            source_counts[raw.source_watch] += 1
        first_seen = {watch: min(
            fp_by_id[event_id].created_at for event_id in group
            if fp_by_id[event_id].watch_name == watch) for watch in watches}
        root_watch = min(watches, key=lambda watch: (
            -source_counts[watch], first_seen[watch], watch))
        clusters.append(PropagationCluster(
            id=f"cluster-{cluster_index}",
            root_watch=root_watch,
            watches=tuple(watches),
            event_ids=tuple(sorted(group)),
            link_ids=tuple(link_ids),
            started_at=min(fp_by_id[event_id].created_at
                           for event_id in group),
            ended_at=max(fp_by_id[event_id].created_at
                         for event_id in group),
            confidence=max(raw.confidence for raw in group_links),
        ))
    return tuple(final_links), tuple(clusters), cut_event_ids
