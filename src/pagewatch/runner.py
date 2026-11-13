"""Single-watch monitoring transaction: fetch, normalize, diff, classify, alert."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .alert.outbox import append_alert
from .alert.rules import alert_message, should_alert
from .clock import Clock, SystemClock
from .config import effective_rules
from .diff.blockdiff import diff_blocks
from .diff.classify import classify, reasons
from .diff.metrics import compute_metrics
from .diff.textdiff import unified_diff
from .fetch.allowlist import AllowlistError
from .fetch.archive import write_archive
from .fetch.client import FetchClient, FetchError, FetchResult
from .models import Rules, WatchSpec
from .normalize.pipeline import (
    blocks_from_json, blocks_to_json, normalize_html,
)
from .quality.models import WATCH_QUARANTINED, WATCH_REBASELINED
from .quality.store import get_watch_quality_state, upsert_watch_quality_state
from .schedule.backoff import next_due_at
from .storage.db import connect, transaction
from .storage.events import add_audit, insert_alert, insert_event
from .storage.snapshots import (
    append_snapshot, get_schedule, get_watch_state, latest_snapshot,
    mark_snapshot_major, record_fetch_state, record_probe, upsert_schedule,
    upsert_watch,
)


@dataclass(frozen=True)
class WatchRunResult:
    watch_name: str
    outcome: str
    severity: str | None = None
    event_id: int | None = None
    alert_id: int | None = None
    snapshot_id: int | None = None
    error: str | None = None
    quarantined: bool = False
    rebaselined: bool = False


class Runner:
    def __init__(self, *, db_path: str | Path, archive_dir: str | Path,
                 outbox_path: str | Path, rules: Rules,
                 clock: Clock | None = None,
                 client_factory: Callable[[Rules], FetchClient] | None = None) -> None:
        self.db_path = Path(db_path)
        self.archive_dir = Path(archive_dir)
        self.outbox_path = Path(outbox_path)
        self.rules = rules
        self.clock = clock or SystemClock()
        self.client_factory = client_factory or self._default_client

    @staticmethod
    def _default_client(rules: Rules) -> FetchClient:
        return FetchClient(timeout_seconds=rules.timeout_seconds,
                           max_bytes=rules.max_bytes)

    def run(self, watch: WatchSpec) -> WatchRunResult:
        rules = effective_rules(self.rules, watch)
        checked_at = self.clock.now()
        with connect(self.db_path) as connection:
            upsert_watch(connection, watch)
            connection.commit()
            state = get_watch_state(connection, watch.name) or {}
            quality_state = get_watch_quality_state(connection, watch.name)
            try:
                result = self.client_factory(rules).fetch(
                    watch.url, etag=state.get("etag"),
                    last_modified=state.get("last_modified"),
                )
            except (AllowlistError, FetchError) as exc:
                return self._fail(connection, watch, checked_at, exc)
            record_probe(
                connection, watch_name=watch.name, checked_at=checked_at,
                status_code=result.status_code, etag=result.etag,
                last_modified=result.last_modified,
                not_modified=result.not_modified, elapsed_ms=result.elapsed_ms,
                raw_sha256=result.raw_sha256,
            )
            if result.not_modified:
                record_fetch_state(
                    connection, name=watch.name, at=checked_at,
                    status_code=304, success=True, replace_validators=False,
                )
                self._schedule(connection, watch, checked_at, success=True,
                               outcome="not_modified")
                connection.commit()
                return WatchRunResult(
                    watch.name, "not_modified",
                    quarantined=quality_state is not None
                    and quality_state.status == WATCH_QUARANTINED)
            record_fetch_state(
                connection, name=watch.name, at=checked_at,
                status_code=result.status_code, success=True,
                etag=result.etag, last_modified=result.last_modified,
            )
            try:
                return self._process(connection, watch, rules, result,
                                     checked_at, quality_state)
            except Exception as exc:  # keep one bad page from aborting a round
                add_audit(connection, at=checked_at, kind="processing.error",
                          watch_name=watch.name,
                          message=f"{type(exc).__name__}: {exc}")
                self._schedule(connection, watch, checked_at, success=False,
                               outcome="processing_error")
                connection.commit()
                return WatchRunResult(watch.name, "error", error=str(exc))

    def _archive(self, result: FetchResult) -> str | None:
        if not result.body:
            return None
        _, digest, _ = write_archive(result.body, self.archive_dir)
        return digest

    def _process(self, connection, watch: WatchSpec, rules: Rules,
                 result: FetchResult, checked_at: float,
                 quality_state=None) -> WatchRunResult:
        page = normalize_html(
            result.text,
            include_selectors=watch.include_selectors,
            exclude_selectors=watch.exclude_selectors,
            noise_patterns=watch.noise_patterns,
            focus_selectors=rules.focus_selectors,
        )
        previous = latest_snapshot(connection, watch.name)
        raw_hash = self._archive(result)
        if previous is None:
            snapshot = append_snapshot(
                connection, watch_name=watch.name, fetched_at=checked_at,
                status_code=result.status_code, url=watch.url, etag=result.etag,
                last_modified=result.last_modified,
                content_sha256=page.content_hash, raw_sha256=raw_hash,
                normalized_text=page.text, blocks_json=blocks_to_json(page.blocks),
                normalizer_version=page.normalizer_version,
            )
            self._clear_rebaseline_anchor(connection, watch.name, checked_at,
                                          quality_state)
            self._schedule(connection, watch, checked_at, success=True,
                           outcome="first")
            connection.commit()
            return WatchRunResult(watch.name, "first", snapshot_id=snapshot.id)
        if page.content_hash == previous.content_sha256:
            self._schedule(connection, watch, checked_at, success=True,
                           outcome="unchanged")
            connection.commit()
            return WatchRunResult(
                watch.name, "unchanged", snapshot_id=previous.id,
                quarantined=quality_state is not None
                and quality_state.status == WATCH_QUARANTINED)

        rebaseline = (quality_state is not None
                      and quality_state.status == WATCH_REBASELINED)
        quarantined = (quality_state is not None
                       and quality_state.status == WATCH_QUARANTINED)

        if rebaseline:
            return self._rebaseline(
                connection, watch, rules, result, checked_at, page, previous,
                raw_hash, quality_state)
        if quarantined:
            return self._quarantined_change(
                connection, watch, rules, result, checked_at, page, previous,
                raw_hash, quality_state)
        return self._normal_change(
            connection, watch, rules, result, checked_at, page, previous,
            raw_hash)

    def _rebaseline(self, connection, watch: WatchSpec, rules: Rules,
                    result: FetchResult, checked_at: float, page,
                    previous, raw_hash, quality_state) -> WatchRunResult:
        """Force a new chain anchor after repeated structural failures.

        The current content is archived as a fresh snapshot but no event or
        alert is derived from the broken chain.  The quality state moves back
        to a normal quarantine anchor pointing at the new version so future
        checks only inspect the healthy tail of the chain.
        """
        with transaction(connection):
            snapshot = append_snapshot(
                connection, watch_name=watch.name, fetched_at=checked_at,
                status_code=result.status_code, url=watch.url, etag=result.etag,
                last_modified=result.last_modified,
                content_sha256=page.content_hash, raw_sha256=raw_hash,
                normalized_text=page.text, blocks_json=blocks_to_json(page.blocks),
                normalizer_version=page.normalizer_version,
            )
            add_audit(
                connection, at=checked_at, kind="quality.rebaseline",
                watch_name=watch.name,
                message=("forced rebaseline after "
                         f"{quality_state.consecutive_failures} consecutive "
                         "failed verification rounds"),
                details={"previous_snapshot_id": previous.id,
                         "new_snapshot_id": snapshot.id,
                         "first_failed_round_id":
                             quality_state.first_failed_round_id},
            )
            upsert_watch_quality_state(
                connection, watch_name=watch.name, status=WATCH_QUARANTINED,
                consecutive_failures=0,
                rebaseline_version=snapshot.version,
                first_failed_round_id=None,
                last_report_round_id=quality_state.last_report_round_id,
                updated_at=checked_at,
            )
            self._schedule(connection, watch, checked_at, success=True,
                           outcome="rebaseline")
        return WatchRunResult(watch.name, "rebaseline",
                              snapshot_id=snapshot.id, quarantined=True,
                              rebaselined=True)

    def _quarantined_change(self, connection, watch: WatchSpec, rules: Rules,
                            result: FetchResult, checked_at: float, page,
                            previous, raw_hash,
                            quality_state) -> WatchRunResult:
        """Keep archiving a quarantined watch but skip graded alerting."""
        old_blocks = blocks_from_json(previous.blocks_json)
        changes = diff_blocks(old_blocks, page.blocks)
        metrics = compute_metrics(old_blocks, page.blocks, changes,
                                  watch.keywords)
        severity = classify(metrics, rules)
        with transaction(connection):
            snapshot = append_snapshot(
                connection, watch_name=watch.name, fetched_at=checked_at,
                status_code=result.status_code, url=watch.url, etag=result.etag,
                last_modified=result.last_modified,
                content_sha256=page.content_hash, raw_sha256=raw_hash,
                normalized_text=page.text, blocks_json=blocks_to_json(page.blocks),
                normalizer_version=page.normalizer_version,
            )
            insert_event(
                connection, watch_name=watch.name,
                from_snapshot_id=previous.id, to_snapshot_id=snapshot.id,
                created_at=checked_at, severity=severity,
                change_rate=metrics.change_rate,
                added_lines=metrics.added_lines, removed_lines=metrics.removed_lines,
                regions=metrics.regions, keyword_hits=metrics.keyword_hits,
                focus_hit=metrics.focus_hit,
                summary=f"quarantined: {self._summary(severity, metrics, rules)}",
                diff_text=unified_diff(previous.normalized_text, page.text,
                                       rules.diff_max_chars),
            )
            add_audit(
                connection, at=checked_at, kind="quality.quarantined_run",
                watch_name=watch.name,
                message=("watch under quarantine: change archived without "
                         "graded alerting"),
                details={"snapshot_id": snapshot.id, "severity": severity},
            )
            self._schedule(connection, watch, checked_at, success=True,
                           outcome=f"quarantined:{severity}")
        return WatchRunResult(watch.name, "quarantined", severity=severity,
                              snapshot_id=snapshot.id, quarantined=True)

    def _normal_change(self, connection, watch: WatchSpec, rules: Rules,
                       result: FetchResult, checked_at: float, page,
                       previous, raw_hash) -> WatchRunResult:
        old_blocks = blocks_from_json(previous.blocks_json)
        changes = diff_blocks(old_blocks, page.blocks)
        metrics = compute_metrics(old_blocks, page.blocks, changes, watch.keywords)
        severity = classify(metrics, rules)
        summary = self._summary(severity, metrics, rules)
        diff_text = unified_diff(previous.normalized_text, page.text,
                                 rules.diff_max_chars)
        alert = None
        with transaction(connection):
            snapshot = append_snapshot(
                connection, watch_name=watch.name, fetched_at=checked_at,
                status_code=result.status_code, url=watch.url, etag=result.etag,
                last_modified=result.last_modified,
                content_sha256=page.content_hash, raw_sha256=raw_hash,
                normalized_text=page.text, blocks_json=blocks_to_json(page.blocks),
                normalizer_version=page.normalizer_version,
            )
            event = insert_event(
                connection, watch_name=watch.name,
                from_snapshot_id=previous.id, to_snapshot_id=snapshot.id,
                created_at=checked_at, severity=severity,
                change_rate=metrics.change_rate,
                added_lines=metrics.added_lines, removed_lines=metrics.removed_lines,
                regions=metrics.regions, keyword_hits=metrics.keyword_hits,
                focus_hit=metrics.focus_hit, summary=summary, diff_text=diff_text,
            )
            if severity == "major":
                mark_snapshot_major(connection, snapshot.id)
            if should_alert(severity, rules):
                alert = insert_alert(
                    connection, watch_name=watch.name, severity=severity,
                    event_id=event.id, created_at=checked_at,
                    message=alert_message(watch.name, severity, event.id,
                                          metrics.change_rate),
                    cooldown_seconds=rules.alert_cooldown_seconds,
                )
            self._schedule(connection, watch, checked_at, success=True,
                           outcome=f"changed:{severity}")
        if alert is not None:
            try:
                append_alert(self.outbox_path, alert)
            except OSError as exc:
                with transaction(connection):
                    add_audit(connection, at=checked_at, kind="outbox.error",
                              watch_name=watch.name, message=str(exc))
        return WatchRunResult(
            watch.name, "changed", severity=severity, event_id=event.id,
            alert_id=alert.id if alert else None, snapshot_id=snapshot.id,
        )

    @staticmethod
    def _clear_rebaseline_anchor(connection, watch_name: str, now: float,
                                 quality_state) -> None:
        """Drop a stale rebaseline anchor when a fresh baseline is written.

        The watch's lifecycle status is owned by the next verification round;
        here we only stop chain checks from being scoped to an old anchor.
        """
        if quality_state is None or quality_state.rebaseline_version is None:
            return
        upsert_watch_quality_state(
            connection, watch_name=watch_name, status=quality_state.status,
            consecutive_failures=quality_state.consecutive_failures,
            rebaseline_version=None,
            first_failed_round_id=quality_state.first_failed_round_id,
            last_report_round_id=quality_state.last_report_round_id,
            updated_at=now,
        )

    @staticmethod
    def _summary(severity: str, metrics, rules: Rules) -> str:
        detail = ", ".join(reasons(metrics, rules, severity)) or "content-change"
        return (f"{severity}: +{metrics.added_lines}/-{metrics.removed_lines} lines, "
                f"rate={metrics.change_rate:.3f}, {detail}")

    def _schedule(self, connection, watch: WatchSpec, now: float, *,
                  success: bool, outcome: str) -> None:
        current = get_schedule(connection, watch.name)
        failures = 0 if success else int((current or {}).get("consecutive_failures", 0)) + 1
        rules = effective_rules(self.rules, watch)
        upsert_schedule(
            connection, watch_name=watch.name,
            next_due_at=next_due_at(
                now=now, interval_seconds=watch.interval_seconds,
                failures=failures, succeeded=success, watch_name=watch.name,
                jitter_seconds=rules.jitter_seconds,
            ),
            consecutive_failures=failures, last_run_at=now,
            last_outcome=outcome, updated_at=now,
        )

    def _fail(self, connection, watch: WatchSpec, checked_at: float,
              error: Exception) -> WatchRunResult:
        message = f"{type(error).__name__}: {error}"
        record_probe(connection, watch_name=watch.name, checked_at=checked_at,
                     status_code=None, error=message)
        record_fetch_state(connection, name=watch.name, at=checked_at,
                           status_code=None, success=False, error=message,
                           replace_validators=False)
        if isinstance(error, AllowlistError):
            add_audit(connection, at=checked_at, kind="allowlist.rejected",
                      watch_name=watch.name, message=str(error),
                      details={"url": watch.url})
        else:
            add_audit(connection, at=checked_at, kind="fetch.error",
                      watch_name=watch.name, message=message,
                      details={"url": watch.url})
        self._schedule(connection, watch, checked_at, success=False,
                       outcome="allowlist_error" if isinstance(error, AllowlistError)
                       else "fetch_error")
        connection.commit()
        return WatchRunResult(watch.name, "error", error=message)
