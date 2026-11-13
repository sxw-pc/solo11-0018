"""Pure retention-policy evaluation for the cascade cleanup.

A single compaction plan decides the fate of four linked stores together:

* snapshot rows (``snapshots`` table)
* change-event rows that reference those snapshots (``events`` table)
* alert rows of those events (``alerts`` table) and their ``alerts.jsonl``
  mirror lines
* unreferenced, expired raw gzip archives

Events and alerts use exactly the same judgment as snapshots: an event is
removed iff one of its endpoint snapshots is removed, and an alert is removed
iff its event is removed.  A time lower bound (``not_after``) excludes every
snapshot produced after the compaction round started, so concurrent writers
can never have their fresh snapshots treated as garbage.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

from ..models import AlertRecord, ChangeEvent, Snapshot


@dataclass(frozen=True)
class RetentionPlan:
    snapshot_ids: tuple[int, ...] = ()
    event_ids: tuple[int, ...] = ()
    alert_ids: tuple[int, ...] = ()
    archives: tuple[Path, ...] = ()
    not_after: float = 0.0
    archive_bytes: int = 0
    snapshot_bytes: int = 0

    @property
    def empty(self) -> bool:
        return not (self.snapshot_ids or self.event_ids or self.alert_ids
                    or self.archives)

    @property
    def total_bytes(self) -> int:
        return self.archive_bytes + self.snapshot_bytes

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshots": len(self.snapshot_ids),
            "events": len(self.event_ids),
            "alerts": len(self.alert_ids),
            "archives": len(self.archives),
            "not_after": self.not_after,
            "archive_bytes": self.archive_bytes,
            "snapshot_bytes": self.snapshot_bytes,
            "total_bytes": self.total_bytes,
        }


def snapshot_bytes(snapshot: Snapshot) -> int:
    return (len(snapshot.normalized_text.encode("utf-8"))
            + len(snapshot.blocks_json.encode("utf-8")))


def snapshots_to_prune(snapshots: Iterable[Snapshot], major_ids: set[int],
                       keep_last: int,
                       major_keep_forever: bool = True) -> list[Snapshot]:
    ordered = sorted(snapshots, key=lambda item: item.version, reverse=True)
    protected = {snapshot.id for snapshot in ordered[:max(0, keep_last)]}
    candidates: list[Snapshot] = []
    for snapshot in ordered:
        if snapshot.id in protected:
            continue
        if major_keep_forever and (snapshot.id in major_ids or snapshot.is_major):
            continue
        candidates.append(snapshot)
    return sorted(candidates, key=lambda item: (item.watch_name, item.version))


def archive_files(archive_dir: str | Path) -> list[Path]:
    directory = Path(archive_dir)
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob("*.gz") if path.is_file())


def archives_to_prune(archive_dir: str | Path, referenced_hashes: set[str],
                      now: float, max_age_days: int,
                      not_after: float | None = None) -> list[Path]:
    cutoff = now - max(0, int(max_age_days)) * 86_400
    candidates: list[Path] = []
    for path in archive_files(archive_dir):
        digest = path.name[:-3]
        if digest in referenced_hashes:
            continue
        try:
            modified = path.stat().st_mtime
        except OSError:
            continue
        # Protect archives written by an in-flight fetch whose snapshot has
        # not been committed yet (matters when max_age_days is 0).
        if not_after is not None and modified >= not_after:
            continue
        if modified <= cutoff:
            candidates.append(path)
    return candidates


def _events_to_remove(events: Iterable[ChangeEvent],
                      retained_snapshot_ids: set[int]) -> set[int]:
    """Events go iff an endpoint snapshot is not among the retained rows.

    Using the retained set (rather than the pruned set) also repairs legacy
    damage: an event whose snapshot vanished in an earlier, non-cascade run is
    dangling already and is removed on the first new compaction.
    """
    return {event.id for event in events
            if event.from_snapshot_id not in retained_snapshot_ids
            or event.to_snapshot_id not in retained_snapshot_ids}


def build_cascade_plan(snapshots: Iterable[Snapshot],
                       events: Iterable[ChangeEvent],
                       alerts: Iterable[AlertRecord], major_ids: set[int], *,
                       keep_last: int, major_keep_forever: bool,
                       archive_dir: str | Path, now: float, not_after: float,
                       archive_max_age_days: int) -> RetentionPlan:
    """Compute every cleanup decision from one consistent database read."""
    all_snapshots = list(snapshots)
    events = list(events)
    alerts = list(alerts)

    # Time lower bound: anything fetched at/after the bound belongs to the
    # current or a later round and is unconditionally retained.
    scoped = [snapshot for snapshot in all_snapshots
              if snapshot.fetched_at < not_after]
    grouped: dict[str, list[Snapshot]] = {}
    for snapshot in scoped:
        grouped.setdefault(snapshot.watch_name, []).append(snapshot)
    pruned: list[Snapshot] = []
    for group in grouped.values():
        pruned.extend(snapshots_to_prune(group, major_ids, keep_last,
                                         major_keep_forever))
    pruned_snapshot_ids = {snapshot.id for snapshot in pruned}
    retained_snapshot_ids = {snapshot.id for snapshot in all_snapshots
                             if snapshot.id not in pruned_snapshot_ids}

    # Same judgment口径 for events: an event survives iff both endpoint
    # snapshots survive (which also sweeps up pre-existing dangling events).
    removed_event_ids = _events_to_remove(events, retained_snapshot_ids)
    kept_event_ids = {event.id for event in events} - removed_event_ids

    # Alerts follow their events; legacy dangling alerts (event already gone)
    # are reconciled away as well so the outbox matches the events ledger.
    removed_alert_ids = {alert.id for alert in alerts
                         if alert.event_id not in kept_event_ids}

    # Archive references are computed over ALL retained snapshots, including
    # bound-excluded ones, so a referenced gzip can never be deleted.
    referenced = {snapshot.raw_sha256 for snapshot in all_snapshots
                  if snapshot.id not in pruned_snapshot_ids
                  and snapshot.raw_sha256}
    archives = archives_to_prune(archive_dir, referenced, now,
                                 archive_max_age_days, not_after)
    archive_size = 0
    for path in archives:
        try:
            archive_size += path.stat().st_size
        except OSError:
            pass
    return RetentionPlan(
        snapshot_ids=tuple(sorted(pruned_snapshot_ids)),
        event_ids=tuple(sorted(removed_event_ids)),
        alert_ids=tuple(sorted(removed_alert_ids)),
        archives=tuple(archives),
        not_after=not_after,
        archive_bytes=archive_size,
        snapshot_bytes=sum(snapshot_bytes(item) for item in pruned),
    )


def build_plan(snapshots: Iterable[Snapshot], major_ids: set[int], *,
               keep_last: int, major_keep_forever: bool,
               archive_dir: str | Path, now: float,
               archive_max_age_days: int) -> RetentionPlan:
    """Snapshot/archive-only plan without an event ledger (no time bound)."""
    return build_cascade_plan(
        snapshots, (), (), major_ids, keep_last=keep_last,
        major_keep_forever=major_keep_forever, archive_dir=archive_dir,
        now=now, not_after=float("inf"),
        archive_max_age_days=archive_max_age_days,
    )


def format_plan(plan: RetentionPlan) -> str:
    if plan.empty:
        return "nothing to clean (plan is empty)"
    lines = [
        f"snapshots: {len(plan.snapshot_ids)} "
        f"({', '.join(str(item) for item in plan.snapshot_ids) or '-'})",
        f"events: {len(plan.event_ids)} "
        f"({', '.join(str(item) for item in plan.event_ids) or '-'})",
        f"alerts: {len(plan.alert_ids)} "
        f"({', '.join(str(item) for item in plan.alert_ids) or '-'})",
        f"archives: {len(plan.archives)}",
    ]
    lines.extend(f"  archive {path.name} ({path.stat().st_size} bytes)"
                 if path.exists() else f"  archive {path.name} (missing)"
                 for path in plan.archives)
    lines.append(f"estimated reclaimable: {plan.total_bytes} bytes "
                 f"(archive {plan.archive_bytes}, rows {plan.snapshot_bytes})")
    return "\n".join(lines)
