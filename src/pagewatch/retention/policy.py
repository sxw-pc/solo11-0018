"""Pure retention-policy evaluation for snapshots and raw archives."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

from ..models import Snapshot


@dataclass(frozen=True)
class RetentionPlan:
    snapshot_ids: tuple[int, ...] = ()
    archives: tuple[Path, ...] = ()
    archive_bytes: int = 0
    snapshot_bytes: int = 0

    @property
    def empty(self) -> bool:
        return not self.snapshot_ids and not self.archives

    @property
    def total_bytes(self) -> int:
        return self.archive_bytes + self.snapshot_bytes

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshots": len(self.snapshot_ids),
            "archives": len(self.archives),
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
                      now: float, max_age_days: int) -> list[Path]:
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
        if modified <= cutoff:
            candidates.append(path)
    return candidates


def build_plan(snapshots: Iterable[Snapshot], major_ids: set[int], *,
               keep_last: int, major_keep_forever: bool,
               archive_dir: str | Path, now: float,
               archive_max_age_days: int) -> RetentionPlan:
    """Compute both cleanup classes from an unchanged database snapshot."""
    all_snapshots = list(snapshots)
    grouped: dict[str, list[Snapshot]] = {}
    for snapshot in all_snapshots:
        grouped.setdefault(snapshot.watch_name, []).append(snapshot)
    pruned: list[Snapshot] = []
    for group in grouped.values():
        pruned.extend(snapshots_to_prune(group, major_ids, keep_last,
                                         major_keep_forever))
    pruned_ids = {snapshot.id for snapshot in pruned}
    referenced = {snapshot.raw_sha256 for snapshot in all_snapshots
                  if snapshot.id not in pruned_ids and snapshot.raw_sha256}
    archives = archives_to_prune(archive_dir, referenced, now, archive_max_age_days)
    archive_size = 0
    for path in archives:
        try:
            archive_size += path.stat().st_size
        except OSError:
            pass
    return RetentionPlan(
        snapshot_ids=tuple(sorted(pruned_ids)),
        archives=tuple(archives),
        archive_bytes=archive_size,
        snapshot_bytes=sum(snapshot_bytes(item) for item in pruned),
    )


def format_plan(plan: RetentionPlan) -> str:
    if plan.empty:
        return "nothing to clean (plan is empty)"
    lines = [
        f"snapshots: {len(plan.snapshot_ids)} "
        f"({', '.join(str(item) for item in plan.snapshot_ids) or '-'})",
        f"archives: {len(plan.archives)}",
    ]
    lines.extend(f"  archive {path.name} ({path.stat().st_size} bytes)"
                 if path.exists() else f"  archive {path.name} (missing)"
                 for path in plan.archives)
    lines.append(f"estimated reclaimable: {plan.total_bytes} bytes "
                 f"(archive {plan.archive_bytes}, rows {plan.snapshot_bytes})")
    return "\n".join(lines)
