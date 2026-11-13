"""Dry-run and execute snapshot/archive cleanup."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..models import Rules
from ..storage.db import connect, get_stat, set_stat, transaction
from ..storage.events import add_audit
from ..storage.snapshots import all_snapshots, delete_snapshots, major_snapshot_ids
from .policy import RetentionPlan, build_plan


@dataclass(frozen=True)
class CompactionResult:
    plan: RetentionPlan
    executed: bool
    removed_snapshots: int
    removed_archives: int
    removed_bytes: int


def plan_from_database(db_path: str | Path, archive_dir: str | Path,
                       rules: Rules, now: float) -> RetentionPlan:
    with connect(db_path) as connection:
        snapshots = all_snapshots(connection)
        majors = major_snapshot_ids(connection)
    return build_plan(
        snapshots, majors, keep_last=rules.keep_last,
        major_keep_forever=rules.major_keep_forever,
        archive_dir=archive_dir, now=now,
        archive_max_age_days=rules.archive_max_age_days,
    )


def execute_plan(db_path: str | Path, plan: RetentionPlan, *,
                 now: float) -> CompactionResult:
    removed_archives = 0
    removed_bytes = 0
    for path in plan.archives:
        try:
            size = path.stat().st_size
            path.unlink()
            removed_archives += 1
            removed_bytes += size
        except FileNotFoundError:
            continue
    with connect(db_path) as connection, transaction(connection):
        removed_snapshots = delete_snapshots(connection, plan.snapshot_ids)
        previous_rows = int(get_stat(connection, "compact.removed_snapshots_total")
                            or 0)
        previous_bytes = int(get_stat(connection, "compact.removed_bytes_total")
                             or 0)
        set_stat(connection, "compact.removed_snapshots_total",
                 previous_rows + removed_snapshots)
        set_stat(connection, "compact.removed_bytes_total",
                 previous_bytes + removed_bytes + plan.snapshot_bytes)
        add_audit(connection, at=now, kind="compact", message="compaction executed",
                  details={**plan.as_dict(), "removed_archives": removed_archives,
                           "removed_snapshots": removed_snapshots})
    return CompactionResult(
        plan=plan, executed=True, removed_snapshots=removed_snapshots,
        removed_archives=removed_archives,
        removed_bytes=removed_bytes + plan.snapshot_bytes,
    )


def compact(db_path: str | Path, archive_dir: str | Path, rules: Rules,
            now: float, *, dry_run: bool = False) -> CompactionResult:
    plan = plan_from_database(db_path, archive_dir, rules, now)
    if dry_run:
        return CompactionResult(plan=plan, executed=False, removed_snapshots=0,
                                removed_archives=0, removed_bytes=0)
    return execute_plan(db_path, plan, now=now)
