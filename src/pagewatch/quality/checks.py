"""Deterministic, read-only structural checks for one monitoring round.

Every check reads a plain in-memory snapshot of the ledger captured through
the tolerant readers in :mod:`pagewatch.quality.store`.  Nothing here writes,
fetches or samples: identical database contents always produce identical
findings, ordered by stable keys so reports are byte-for-byte reproducible.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import (
    ALL_CHECKS, CHECK_ALERT_PAIRING, CHECK_EVENT_REFERENCES,
    CHECK_MAJOR_RETENTION, CHECK_OUTBOX_PAIRING, CHECK_SNAPSHOT_CHAIN,
    FAIL, Finding, PASS, UNKNOWN, CheckResult,
)
from .store import (
    TableUnavailable, load_alerts, load_events, load_snapshots,
)


def _finding(check: str, code: str, severity: str, message: str,
             watch_name: str | None = None,
             evidence: dict[str, Any] | None = None) -> Finding:
    return Finding(check=check, code=code, severity=severity, message=message,
                   watch_name=watch_name, evidence=evidence or {})


def _result(check: str, findings: list[Finding]) -> CheckResult:
    if any(item.severity == FAIL for item in findings):
        status = FAIL
    elif any(item.severity == UNKNOWN for item in findings):
        status = UNKNOWN
    else:
        status = PASS
    return CheckResult(check=check, status=status,
                       findings=tuple(sorted(
                           findings,
                           key=lambda item: (item.code,
                                             item.watch_name or "",
                                             json.dumps(item.evidence,
                                                        sort_keys=True)))))


def _scoped(values: list[dict[str, Any]], watch_names: set[str]
            ) -> list[dict[str, Any]]:
    return [row for row in values if row["watch_name"] in watch_names]


def check_snapshot_chain(snapshots: list[dict[str, Any]], *,
                         watch_names: set[str],
                         events: list[dict[str, Any]],
                         rebaseline_versions: dict[str, int | None]
                         ) -> CheckResult:
    """Version continuity per watch, plus event adjacency along the chain."""
    findings: list[Finding] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _scoped(snapshots, watch_names):
        grouped.setdefault(row["watch_name"], []).append(row)

    for name in sorted(grouped):
        chain = sorted(grouped[name], key=lambda item: (item["version"],
                                                        item["id"]))
        anchor = rebaseline_versions.get(name)
        if anchor is not None:
            chain = [item for item in chain if item["version"] >= anchor]
        if not chain:
            continue
        versions = [int(item["version"]) for item in chain]
        expected = versions[0]
        for version in versions:
            if version != expected:
                findings.append(_finding(
                    CHECK_SNAPSHOT_CHAIN, "snapshot_version_gap", FAIL,
                    f"watch {name!r}: snapshot chain gap, expected version "
                    f"{expected} but found {version}",
                    watch_name=name,
                    evidence={"expected_version": expected,
                              "found_version": version}))
                expected = version
            expected += 1

        id_to_version = {int(item["id"]): int(item["version"])
                         for item in chain}
        version_sequence = set(versions)
        for event in sorted(_scoped(events, watch_names),
                            key=lambda item: int(item["id"])):
            if event["watch_name"] != name:
                continue
            from_id = int(event["from_snapshot_id"])
            to_id = int(event["to_snapshot_id"])
            if from_id not in id_to_version or to_id not in id_to_version:
                continue  # dangling references belong to the reference check
            from_version = id_to_version[from_id]
            to_version = id_to_version[to_id]
            if to_version not in version_sequence or from_version not in version_sequence:
                continue
            if to_version <= from_version or to_version != from_version + 1:
                findings.append(_finding(
                    CHECK_SNAPSHOT_CHAIN, "event_chain_not_adjacent", FAIL,
                    f"watch {name!r}: event {event['id']} jumps from snapshot "
                    f"v{from_version} to v{to_version} instead of the next "
                    "chain version",
                    watch_name=name,
                    evidence={"event_id": event["id"],
                              "from_version": from_version,
                              "to_version": to_version}))
    return _result(CHECK_SNAPSHOT_CHAIN, findings)


def check_event_references(events: list[dict[str, Any]], *,
                           watch_names: set[str],
                           snapshots: list[dict[str, Any]],
                           rebaseline_versions: dict[str, int | None] | None = None
                           ) -> CheckResult:
    """Every event endpoint must resolve to a snapshot of the same watch.

    Events entirely before a watch's rebaseline anchor describe the damaged
    history that the rebaseline deliberately cut off; they are outside the
    live chain and must not keep the watch quarantined.
    """
    findings: list[Finding] = []
    anchors = rebaseline_versions or {}
    snapshot_ids = {int(item["id"]): item for item in snapshots}
    for event in sorted(_scoped(events, watch_names),
                        key=lambda item: int(item["id"])):
        anchor = anchors.get(event["watch_name"])
        if anchor is not None and _event_predates_anchor(
                event, anchor, snapshot_ids):
            # Damaged history cut off by a forced rebaseline: the live chain
            # starts at the anchor, so this event can no longer be judged.
            continue
        for endpoint in ("from_snapshot_id", "to_snapshot_id"):
            target_id = int(event[endpoint])
            target = snapshot_ids.get(target_id)
            if target is None:
                findings.append(_finding(
                    CHECK_EVENT_REFERENCES, "event_snapshot_missing", FAIL,
                    f"event {event['id']} references missing snapshot "
                    f"{target_id} via {endpoint}",
                    watch_name=event["watch_name"],
                    evidence={"event_id": event["id"], endpoint: target_id}))
            elif target["watch_name"] != event["watch_name"]:
                findings.append(_finding(
                    CHECK_EVENT_REFERENCES, "event_snapshot_wrong_watch", FAIL,
                    f"event {event['id']} ({event['watch_name']!r}) references "
                    f"snapshot {target_id} belonging to watch "
                    f"{target['watch_name']!r}",
                    watch_name=event["watch_name"],
                    evidence={"event_id": event["id"],
                              "snapshot_id": target_id,
                              "snapshot_watch": target["watch_name"]}))
    return _result(CHECK_EVENT_REFERENCES, findings)


def _event_predates_anchor(event: dict[str, Any], anchor_version: int,
                           snapshot_ids: dict[int, dict[str, Any]]) -> bool:
    """True when both endpoints are at versions below the rebaseline anchor.

    A surviving endpoint is compared by its version; a missing endpoint is
    treated as historical (its version can only be below the anchor, since
    the anchor snapshot itself is present).
    """
    for endpoint in ("from_snapshot_id", "to_snapshot_id"):
        endpoint_id = int(event[endpoint])
        endpoint_row = snapshot_ids.get(endpoint_id)
        if endpoint_row is not None and int(endpoint_row["version"]) >= anchor_version:
            return False
    return True


def _alert_eligible(event: dict[str, Any], alert_severities: tuple[str, ...]
                    ) -> bool:
    return event["severity"] in set(alert_severities)


def check_alert_pairing(events: list[dict[str, Any]], alerts: list[dict[str, Any]],
                        *, watch_names: set[str],
                        alert_severities: tuple[str, ...],
                        cooldown_seconds: int) -> CheckResult:
    """Bidirectional pairing between alerting events and alert records."""
    findings: list[Finding] = []
    scoped_events = _scoped(events, watch_names)
    scoped_alerts = _scoped(alerts, watch_names)
    event_by_id = {int(item["id"]): item for item in scoped_events}
    alerted_event_ids = {int(item["event_id"]) for item in scoped_alerts}

    for alert in sorted(scoped_alerts, key=lambda item: int(item["id"])):
        source = event_by_id.get(int(alert["event_id"]))
        if source is None:
            if int(alert["event_id"]) in {int(item["id"]) for item in events}:
                reason = "alert_source_other_watch"
                message = ("alert {id} points at event {event} owned by another "
                           "watch")
            else:
                reason = "alert_source_missing"
                message = "alert {id} points at event {event} which does not exist"
            findings.append(_finding(
                CHECK_ALERT_PAIRING, reason, FAIL,
                message.format(id=alert["id"], event=alert["event_id"]),
                watch_name=alert["watch_name"],
                evidence={"alert_id": alert["id"],
                          "event_id": alert["event_id"]}))

    for event in sorted(scoped_events, key=lambda item: int(item["id"])):
        if not _alert_eligible(event, alert_severities):
            continue
        if int(event["id"]) in alerted_event_ids:
            continue
        suppressed_by = None
        if cooldown_seconds > 0:
            window_start = float(event["created_at"]) - cooldown_seconds
            candidates = sorted(
                (int(item["id"]) for item in scoped_alerts
                 if item["watch_name"] == event["watch_name"]
                 and item["severity"] == event["severity"]
                 and window_start <= float(item["created_at"])
                 <= float(event["created_at"])))
            suppressed_by = candidates[0] if candidates else None
        if suppressed_by is not None:
            findings.append(_finding(
                CHECK_ALERT_PAIRING, "alert_suppressed_by_cooldown", PASS,
                f"alerting event {event['id']} has no alert record: covered by "
                f"cooldown of alert {suppressed_by}",
                watch_name=event["watch_name"],
                evidence={"event_id": event["id"],
                          "cooldown_alert_id": suppressed_by}))
        else:
            findings.append(_finding(
                CHECK_ALERT_PAIRING, "alert_record_missing", FAIL,
                f"alerting event {event['id']} ({event['severity']}) has no "
                "matching alert record and is not within a cooldown window",
                watch_name=event["watch_name"],
                evidence={"event_id": event["id"],
                          "severity": event["severity"]}))
    return _result(CHECK_ALERT_PAIRING, findings)


def _read_outbox_lines(path: Path) -> tuple[list[dict[str, Any]], list[Finding]]:
    rows: list[dict[str, Any]] = []
    problems: list[Finding] = []
    if not path.exists():
        return rows, problems
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except (TypeError, ValueError) as exc:
                problems.append(_finding(
                    CHECK_OUTBOX_PAIRING, "outbox_unparseable_line", FAIL,
                    f"outbox line {line_number} is not valid JSON: {exc}",
                    evidence={"line_number": line_number}))
                continue
            if not isinstance(payload, dict) or "id" not in payload:
                problems.append(_finding(
                    CHECK_OUTBOX_PAIRING, "outbox_malformed_line", FAIL,
                    f"outbox line {line_number} lacks an alert id",
                    evidence={"line_number": line_number}))
                continue
            rows.append(payload)
    return rows, problems


def check_outbox_pairing(alerts: list[dict[str, Any]], *,
                         watch_names: set[str], outbox_path: Path
                         ) -> CheckResult:
    """Every ledger alert must be in the JSONL outbox and vice versa."""
    findings: list[Finding]
    outbox_rows, findings = _read_outbox_lines(outbox_path)
    scoped_alerts = _scoped(alerts, watch_names)
    ledger_by_id = {int(item["id"]): item for item in scoped_alerts}
    outbox_ids = {int(item["id"]) for item in outbox_rows}

    for alert in sorted(scoped_alerts, key=lambda item: int(item["id"])):
        if int(alert["id"]) not in outbox_ids:
            findings.append(_finding(
                CHECK_OUTBOX_PAIRING, "outbox_entry_missing", FAIL,
                f"alert {alert['id']} exists in the ledger but not in the "
                "outbox JSONL",
                watch_name=alert["watch_name"],
                evidence={"alert_id": alert["id"]}))

    ledger_ids = set(ledger_by_id)
    for payload in sorted(outbox_rows, key=lambda item: int(item["id"])):
        alert_id = int(payload["id"])
        if alert_id in ledger_ids:
            continue
        if alert_id in {int(item["id"]) for item in alerts}:
            code, message = (
                "outbox_entry_other_watch",
                f"outbox alert {alert_id} belongs to a watch outside this round",
            )
        else:
            code, message = (
                "outbox_orphan_entry",
                f"outbox alert {alert_id} has no matching ledger record",
            )
        findings.append(_finding(
            CHECK_OUTBOX_PAIRING, code, FAIL, message,
            watch_name=str(payload.get("watch_name")) or None,
            evidence={"alert_id": alert_id}))
    return _result(CHECK_OUTBOX_PAIRING, findings)


def check_major_retention(events: list[dict[str, Any]], *,
                          watch_names: set[str],
                          snapshots: list[dict[str, Any]],
                          rebaseline_versions: dict[str, int | None] | None = None
                          ) -> CheckResult:
    """Major-event target snapshots must survive retention compaction."""
    findings: list[Finding] = []
    anchors = rebaseline_versions or {}
    snapshot_ids = {int(item["id"]): item for item in snapshots}
    seen: set[int] = set()
    for event in sorted(_scoped(events, watch_names),
                        key=lambda item: int(item["id"])):
        if event["severity"] != "major":
            continue
        target_id = int(event["to_snapshot_id"])
        if target_id in seen:
            continue
        seen.add(target_id)
        if target_id in snapshot_ids:
            continue
        # A missing major target predating a rebaseline is absorbed history:
        # the anchor replaced that chain rather than losing protected data.
        if anchors.get(event["watch_name"]) is not None and _event_predates_anchor(
                event, anchors[event["watch_name"]], snapshot_ids):
            continue
        findings.append(_finding(
            CHECK_MAJOR_RETENTION, "major_target_snapshot_missing", FAIL,
            f"major event {event['id']} targets snapshot {target_id} which "
            "is no longer present after retention compaction",
            watch_name=event["watch_name"],
            evidence={"event_id": event["id"],
                      "snapshot_id": target_id}))
    return _result(CHECK_MAJOR_RETENTION, findings)


def _unknown_check(check: str, exc: TableUnavailable) -> CheckResult:
    return CheckResult(
        check=check, status=UNKNOWN,
        findings=(_finding(
            check, "table_unavailable", UNKNOWN,
            f"check skipped: {exc}",
            evidence={"table": exc.table, "missing_columns": sorted(exc.missing)}),),
    )


def run_all_checks(read_connection, *, watch_names: tuple[str, ...],
                   outbox_path: Path, alert_severities: tuple[str, ...],
                   cooldown_seconds: int,
                   rebaseline_versions: dict[str, int | None]
                   ) -> tuple[tuple[CheckResult, ...], tuple[Finding, ...]]:
    """Execute all checks against one read-only connection.

    Returns the check results plus a flattened list of findings.  Tables
    missing on an old database degrade the affected check to UNKNOWN rather
    than aborting the whole verification.
    """
    scope = set(watch_names)
    readers = (
        (CHECK_SNAPSHOT_CHAIN, lambda: load_snapshots(read_connection)),
        (CHECK_EVENT_REFERENCES, lambda: load_events(read_connection)),
        (CHECK_ALERT_PAIRING, lambda: load_alerts(read_connection)),
    )
    loaded: dict[str, list[dict[str, Any]]] = {}
    unavailable: dict[str, TableUnavailable] = {}
    for check, reader in readers:
        try:
            loaded[check] = reader()
        except TableUnavailable as exc:
            unavailable[check] = exc

    snapshots = loaded.get(CHECK_SNAPSHOT_CHAIN, [])
    events = loaded.get(CHECK_EVENT_REFERENCES, [])
    alerts = loaded.get(CHECK_ALERT_PAIRING, [])

    results: list[CheckResult] = []
    for check in ALL_CHECKS:
        if check == CHECK_SNAPSHOT_CHAIN:
            if check in unavailable:
                results.append(_unknown_check(check, unavailable[check]))
            else:
                results.append(check_snapshot_chain(
                    snapshots, watch_names=scope, events=events,
                    rebaseline_versions=rebaseline_versions))
        elif check == CHECK_EVENT_REFERENCES:
            if check in unavailable or CHECK_SNAPSHOT_CHAIN in unavailable:
                results.append(_unknown_check(
                    check, unavailable.get(check)
                    or unavailable[CHECK_SNAPSHOT_CHAIN]))
            else:
                results.append(check_event_references(
                    events, watch_names=scope, snapshots=snapshots,
                    rebaseline_versions=rebaseline_versions))
        elif check == CHECK_ALERT_PAIRING:
            if check in unavailable:
                results.append(_unknown_check(check, unavailable[check]))
            else:
                results.append(check_alert_pairing(
                    events, alerts, watch_names=scope,
                    alert_severities=alert_severities,
                    cooldown_seconds=cooldown_seconds))
        elif check == CHECK_OUTBOX_PAIRING:
            if CHECK_ALERT_PAIRING in unavailable:
                results.append(_unknown_check(
                    check, unavailable[CHECK_ALERT_PAIRING]))
            else:
                results.append(check_outbox_pairing(
                    alerts, watch_names=scope, outbox_path=Path(outbox_path)))
        elif check == CHECK_MAJOR_RETENTION:
            # Both inputs feed this check; a missing ledger means the
            # retention verdict cannot be judged, never a false failure.
            blocking = unavailable.get(
                CHECK_EVENT_REFERENCES) or unavailable.get(
                CHECK_SNAPSHOT_CHAIN)
            if blocking is not None:
                results.append(_unknown_check(check, blocking))
            else:
                results.append(check_major_retention(
                    events, watch_names=scope, snapshots=snapshots,
                    rebaseline_versions=rebaseline_versions))

    flattened = tuple(finding for result in results
                      for finding in result.findings)
    return tuple(results), flattened
