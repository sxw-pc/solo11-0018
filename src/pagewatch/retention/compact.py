"""Cascade compaction: one decision set for snapshots, events, alerts, archives.

Execution order is chosen so every failure leaves a consistent, re-runnable
state:

1. Plan from one consistent read snapshot (explicit read transaction), with a
   ``not_after`` lower bound excluding snapshots a concurrent fetch may create.
2. Stage the replacement ``alerts.jsonl`` in a temporary file under the outbox
   lock -- the original file is untouched until ``os.replace``.
3. Delete alerts, events and snapshots plus stats/audit inside a single SQLite
   transaction.  Any error rolls the whole database side back; the staged temp
   file is discarded and the original outbox is still in place.
4. Atomically replace the outbox (still holding the lock), then unlink gzip
   archives last.  Both steps are idempotent, so a crash here is repaired by
   simply re-running compaction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..alert.outbox import outbox_lock, stage_rewrite
from ..models import Rules
from ..storage.db import connect, get_stat, set_stat, transaction
from ..storage.events import (
    add_audit, delete_alerts, delete_events, list_alerts, list_events,
)
from ..storage.snapshots import (
    all_snapshots, delete_snapshots, major_snapshot_ids,
)
from .policy import RetentionPlan, build_cascade_plan


@dataclass(frozen=True)
class CompactionResult:
    plan: RetentionPlan
    executed: bool
    removed_snapshots: int
    removed_events: int
    removed_alerts: int
    removed_archives: int
    removed_bytes: int


def plan_from_database(db_path: str | Path, archive_dir: str | Path,
                       rules: Rules, now: float,
                       not_after: float | None = None) -> RetentionPlan:
    bound = now if not_after is None else not_after
    with connect(db_path) as connection:
        # Explicit BEGIN makes the four reads one consistent WAL snapshot even
        # if the scheduler commits a fetch between them.
        connection.execute("BEGIN")
        try:
            snapshots = all_snapshots(connection)
            events = list_events(connection)
            alerts = list_alerts(connection)
            majors = major_snapshot_ids(connection)
        finally:
            connection.execute("ROLLBACK")
    return build_cascade_plan(
        snapshots, events, alerts, majors, keep_last=rules.keep_last,
        major_keep_forever=rules.major_keep_forever,
        archive_dir=archive_dir, now=now, not_after=bound,
        archive_max_age_days=rules.archive_max_age_days,
    )


def _assert_no_dangling_references(connection) -> None:
    dangling_event = connection.execute(
        "SELECT 1 FROM events e WHERE NOT EXISTS ("
        "SELECT 1 FROM snapshots s WHERE s.id=e.from_snapshot_id) "
        "OR NOT EXISTS ("
        "SELECT 1 FROM snapshots s WHERE s.id=e.to_snapshot_id) LIMIT 1"
    ).fetchone()
    dangling_alert = connection.execute(
        "SELECT 1 FROM alerts a WHERE NOT EXISTS ("
        "SELECT 1 FROM events e WHERE e.id=a.event_id) LIMIT 1"
    ).fetchone()
    if dangling_event is not None or dangling_alert is not None:
        raise RuntimeError(
            "compaction would leave dangling event/alert references; "
            "rolling back")


def _stage_and_commit(db_path: str | Path, outbox_path: str | Path,
                      plan: RetentionPlan, now: float) -> tuple[int, int, int]:
    """Stage the outbox rewrite, then apply all database changes atomically."""
    outbox_path = Path(outbox_path)
    with outbox_lock(outbox_path):
        # The lock blocks runner appends for the whole stage+commit window.
        # Re-read the alert ledger inside the lock: an alert the runner
        # committed before acquiring the lock must be retained even though the
        # plan was computed earlier.
        staged_temp: Path | None = None
        with connect(db_path) as connection:
            rows = connection.execute("SELECT id FROM alerts").fetchall()
            keep_alert_ids = {int(row["id"]) for row in rows
                              if int(row["id"]) not in plan.alert_ids}
            staged_temp = stage_rewrite(outbox_path, keep_alert_ids)
        try:
            with connect(db_path) as connection, transaction(connection):
                removed_alerts = delete_alerts(connection, plan.alert_ids)
                removed_events = delete_events(connection, plan.event_ids)
                removed_snapshots = delete_snapshots(
                    connection, plan.snapshot_ids)
                _assert_no_dangling_references(connection)
                _update_stats(connection, plan, removed_snapshots,
                              removed_events, removed_alerts)
                add_audit(
                    connection, at=now, kind="compact",
                    message="cascade compaction executed",
                    details={**plan.as_dict(),
                             "removed_archives": len(plan.archives),
                             "removed_snapshots": removed_snapshots,
                             "removed_events": removed_events,
                             "removed_alerts": removed_alerts},
                )
            try:
                os.replace(staged_temp, outbox_path)
            except OSError:
                # Atomic replace either happened or did not; the original
                # outbox is still intact and a re-run converges.
                if staged_temp and os.path.exists(staged_temp):
                    os.unlink(staged_temp)
                raise
        except BaseException:
            if staged_temp and os.path.exists(staged_temp):
                os.unlink(staged_temp)
            raise
    return removed_snapshots, removed_events, removed_alerts


def _update_stats(connection, plan: RetentionPlan, removed_snapshots: int,
                  removed_events: int, removed_alerts: int) -> None:
    previous_rows = int(get_stat(connection, "compact.removed_snapshots_total")
                        or 0)
    previous_bytes = int(get_stat(connection, "compact.removed_bytes_total")
                         or 0)
    previous_events = int(get_stat(connection, "compact.removed_events_total")
                          or 0)
    previous_alerts = int(get_stat(connection, "compact.removed_alerts_total")
                          or 0)
    set_stat(connection, "compact.removed_snapshots_total",
             previous_rows + removed_snapshots)
    set_stat(connection, "compact.removed_bytes_total",
             previous_bytes + plan.snapshot_bytes)
    set_stat(connection, "compact.removed_events_total",
             previous_events + removed_events)
    set_stat(connection, "compact.removed_alerts_total",
             previous_alerts + removed_alerts)


def _unlink_archives(archives: tuple[Path, ...]) -> tuple[int, int]:
    removed_archives = 0
    removed_bytes = 0
    first_error: OSError | None = None
    for path in archives:
        try:
            size = path.stat().st_size
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:  # keep going; a re-run finishes the remainder
            if first_error is None:
                first_error = exc
            continue
        removed_archives += 1
        removed_bytes += size
    if first_error is not None:
        raise first_error
    return removed_archives, removed_bytes


def _reconcile_outbox(db_path: str | Path, outbox_path: str | Path,
                      plan: RetentionPlan) -> None:
    """Rewrite the outbox so its lines match the current alert table.

    Runs even when the plan deletes nothing: the jsonl mirror can still carry
    orphan lines from an older run, and reconciliation is idempotent.
    """
    outbox_path = Path(outbox_path)
    with outbox_lock(outbox_path):
        with connect(db_path) as connection:
            rows = connection.execute("SELECT id FROM alerts").fetchall()
            keep_alert_ids = {int(row["id"]) for row in rows
                              if int(row["id"]) not in plan.alert_ids}
        staged_temp = stage_rewrite(outbox_path, keep_alert_ids)
        try:
            os.replace(staged_temp, outbox_path)
        except BaseException:
            if os.path.exists(staged_temp):
                os.unlink(staged_temp)
            raise


def execute_plan(db_path: str | Path, outbox_path: str | Path,
                 plan: RetentionPlan, *, now: float) -> CompactionResult:
    if plan.snapshot_ids or plan.event_ids or plan.alert_ids:
        removed_snapshots, removed_events, removed_alerts = _stage_and_commit(
            db_path, outbox_path, plan, now)
    else:
        # Nothing to delete in the database, but the jsonl mirror can still
        # contain orphan lines from before cascade compaction existed.
        _reconcile_outbox(db_path, outbox_path, plan)
        removed_snapshots = removed_events = removed_alerts = 0
    removed_archives, archive_bytes = _unlink_archives(plan.archives)
    return CompactionResult(
        plan=plan, executed=True, removed_snapshots=removed_snapshots,
        removed_events=removed_events, removed_alerts=removed_alerts,
        removed_archives=removed_archives,
        removed_bytes=archive_bytes + plan.snapshot_bytes,
    )


def compact(db_path: str | Path, archive_dir: str | Path,
            outbox_path: str | Path, rules: Rules, now: float, *,
            dry_run: bool = False,
            not_after: float | None = None) -> CompactionResult:
    plan = plan_from_database(db_path, archive_dir, rules, now,
                              not_after=not_after)
    if dry_run:
        return CompactionResult(plan=plan, executed=False, removed_snapshots=0,
                                removed_events=0, removed_alerts=0,
                                removed_archives=0, removed_bytes=0)
    return execute_plan(db_path, outbox_path, plan, now=now)
