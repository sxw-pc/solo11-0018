"""Round orchestration for the quality gate.

The verifier runs *after* every worker of a round has joined, so all of their
transactions are committed before inspection starts.  Verification opens its
own read-only connection and never writes snapshots or raw data; only the
final report and the per-watch quarantine states are persisted, through a
separate read-write connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..models import Rules
from ..storage.db import connect, connect_read_only
from .checks import run_all_checks
from .models import (
    FAIL, PASS, UNKNOWN, WATCH_QUARANTINED, WATCH_REBASELINED,
    QualityReport, WatchQualityState,
)
from .store import (
    list_watch_quality_states, next_round_id, ok_state, persist_round_outcome,
)


@dataclass(frozen=True)
class RoundVerification:
    report: QualityReport
    states: dict[str, WatchQualityState]

    @property
    def quarantined_names(self) -> tuple[str, ...]:
        return self.report.quarantined


def _overall_status(checks) -> str:
    if any(item.status == FAIL for item in checks):
        return FAIL
    if any(item.status == UNKNOWN for item in checks):
        return UNKNOWN
    return PASS


def _failing_watches(findings, watch_names: tuple[str, ...]) -> dict[str, list[str]]:
    failures: dict[str, list[str]] = {name: [] for name in watch_names}
    for finding in findings:
        if finding.severity != FAIL or finding.watch_name is None:
            continue
        if finding.watch_name in failures:
            failures[finding.watch_name].append(finding.check)
    return failures


def verify_round(*, db_path: str | Path, outbox_path: str | Path,
                 rules: Rules, watch_names: tuple[str, ...],
                 checked_at: float, round_id: str | None = None,
                 persist: bool = True) -> RoundVerification:
    """Run the full read-only verification and advance quarantine states.

    ``watch_names`` scopes every check to the watches that participated in
    this round; ledger rows of other watches are ignored except as
    snapshot-resolution context.
    """
    ordered_names = tuple(sorted(watch_names))

    # Allocate the round id and read prior states through a normal connection;
    # this happens before the read-only inspection opens.
    with connect(db_path) as connection:
        allocated_round = round_id or next_round_id(connection)
        previous_states = list_watch_quality_states(connection)
        rebaseline_versions = {
            name: state.rebaseline_version
            for name, state in previous_states.items()
        }

    # Strictly read-only inspection on its own connection.  WAL guarantees a
    # consistent view of everything committed before this point; every worker
    # has already joined before verify_round is called.
    with connect_read_only(db_path) as read_connection:
        checks, findings = run_all_checks(
            read_connection, watch_names=ordered_names,
            outbox_path=Path(outbox_path),
            alert_severities=rules.alert_severities,
            cooldown_seconds=rules.alert_cooldown_seconds,
            rebaseline_versions=rebaseline_versions,
        )

    failures_by_watch = _failing_watches(findings, ordered_names)
    quarantined = tuple(sorted(
        name for name, failed_checks in failures_by_watch.items()
        if failed_checks))

    report = QualityReport(
        round_id=allocated_round, checked_at=checked_at,
        status=_overall_status(checks), watch_names=ordered_names,
        checks=checks, findings=findings, quarantined=quarantined,
    ).with_hash()

    next_states = _advance_states(
        previous_states=previous_states, failures_by_watch=failures_by_watch,
        watch_names=ordered_names, round_id=allocated_round, now=checked_at,
        rebaseline_limit=max(1, rules.quarantine_rebaseline_limit),
    )

    if persist:
        persist_round_outcome(db_path, report, next_states)
    return RoundVerification(report=report, states=next_states)


def _advance_states(*, previous_states: dict[str, WatchQualityState],
                    failures_by_watch: dict[str, list[str]],
                    watch_names: tuple[str, ...], round_id: str, now: float,
                    rebaseline_limit: int) -> dict[str, WatchQualityState]:
    states: dict[str, WatchQualityState] = {}
    for name in watch_names:
        previous = previous_states.get(name)
        failed_checks = failures_by_watch.get(name) or []
        if not failed_checks:
            states[name] = ok_state(name, round_id, now, previous)
            continue
        consecutive = (previous.consecutive_failures
                       if previous and previous.status in
                       (WATCH_QUARANTINED, WATCH_REBASELINED) else 0) + 1
        first_failed = (previous.first_failed_round_id
                        if previous and previous.first_failed_round_id
                        else round_id)
        anchor = previous.rebaseline_version if previous else None
        if consecutive >= rebaseline_limit:
            # Bounded quarantine: mark for a forced rebaseline at the next run
            # so a permanently broken chain cannot accumulate dirty events.
            states[name] = WatchQualityState(
                watch_name=name, status=WATCH_REBASELINED,
                consecutive_failures=consecutive,
                rebaseline_version=anchor,
                first_failed_round_id=first_failed,
                last_report_round_id=round_id, updated_at=now)
        else:
            states[name] = WatchQualityState(
                watch_name=name, status=WATCH_QUARANTINED,
                consecutive_failures=consecutive,
                rebaseline_version=anchor,
                first_failed_round_id=first_failed,
                last_report_round_id=round_id, updated_at=now)
    return states
