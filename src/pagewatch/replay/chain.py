"""Rebuild an ordered, traceable per-watch chain for selected rounds.

The chain mirrors the actual execution order of one monitoring pass:
``probe -> archive -> snapshot -> normalize -> diff -> classify -> alert``.
Every stage is reported explicitly: legitimately absent stages are
``skipped`` with a reason, expected-but-absent or inconsistent stages are
breakpoints (``missing``/``invalid``), and links that had to be inferred
from legacy records are ``degraded`` with the inference reason.

The module is strictly read-only: it opens the database with
``mode=ro&immutable=1`` + ``PRAGMA query_only`` and never touches the
ledgers or the data directory.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ..clock import utc_iso
from ..models import AlertRecord, ChangeEvent, Snapshot
from ..normalize.pipeline import block_hash, content_hash
from ..storage.events import list_alerts, list_events
from ..storage.snapshots import all_snapshots
from .rounds import Round
from .workspace import ReplayWorkspace

STAGE_ORDER = ("probe", "archive", "snapshot", "normalize", "diff",
               "classify", "alert")

OK = "ok"
SKIPPED = "skipped"
MISSING = "missing"
INVALID = "invalid"
DEGRADED = "degraded"

BROKEN_STATUSES = frozenset({MISSING, INVALID})


def connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    """Open the ledger read-only; replay must never write to it.

    Uses ``immutable=1`` so SQLite does not create ``-shm``/``-wal``
    sidecar files in the data directory.  A non-empty ``-wal`` file means
    un-checkpointed data (a monitoring round in progress or interrupted);
    reading with ``immutable=1`` would silently miss it, so refuse.
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"database not found: {path}")
    wal_path = path.with_name(path.name + "-wal")
    if wal_path.exists() and wal_path.stat().st_size > 0:
        raise RuntimeError(
            f"database has un-checkpointed WAL data ({wal_path.name}); a "
            "monitoring round may be in progress — rerun replay once it "
            "completes")
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


@dataclass(frozen=True)
class StageRecord:
    stage: str
    status: str
    reason: str | None
    details: dict[str, Any]


@dataclass(frozen=True)
class WatchChain:
    watch_name: str
    status: str  # "ok" | "degraded" | "broken"
    stages: tuple[StageRecord, ...]
    breakpoints: tuple[dict[str, str], ...]
    degradations: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class RoundReplay:
    index: int
    start: float
    end: float
    chains: tuple[WatchChain, ...]


@dataclass(frozen=True)
class ReplayReport:
    generated_at: str
    status: str  # "ok" | "degraded" | "broken"
    rounds: tuple[RoundReplay, ...]


class _ChainBuilder:
    """Accumulates stages for one (round, watch) pair in canonical order."""

    def __init__(self, watch_name: str) -> None:
        self.watch_name = watch_name
        self.stages: list[StageRecord] = []
        self.breakpoints: list[dict[str, str]] = []
        self.degradations: list[dict[str, str]] = []

    def add(self, stage: str, status: str, reason: str | None = None,
            details: dict[str, Any] | None = None) -> None:
        expected = STAGE_ORDER[len(self.stages)]
        if stage != expected:
            raise AssertionError(
                f"chain stages out of order: expected {expected}, got {stage}")
        self.stages.append(StageRecord(stage, status, reason, details or {}))
        entry = {"stage": stage, "reason": reason or ""}
        if status in BROKEN_STATUSES:
            self.breakpoints.append(entry)
        elif status == DEGRADED:
            self.degradations.append(entry)

    def build(self) -> WatchChain:
        if len(self.stages) != len(STAGE_ORDER):
            raise AssertionError("incomplete chain")
        if self.breakpoints:
            status = "broken"
        elif self.degradations:
            status = "degraded"
        else:
            status = "ok"
        return WatchChain(
            watch_name=self.watch_name, status=status,
            stages=tuple(self.stages), breakpoints=tuple(self.breakpoints),
            degradations=tuple(self.degradations),
        )


class _ReplayBuilder:
    def __init__(self, archive_dir: Path, workspace: ReplayWorkspace,
                 probes: list[dict[str, Any]], snapshots: list[Snapshot],
                 events: list[ChangeEvent], alerts: list[AlertRecord]) -> None:
        self.archive_dir = archive_dir
        self.workspace = workspace
        self.probes = probes
        self.snapshots = snapshots
        self.events = events
        self.alerts = alerts
        self.snapshot_index = {snap.id: snap for snap in snapshots}
        self.event_ids = {event.id for event in events}
        self.alerts_by_event: dict[int, list[AlertRecord]] = {}
        for alert in alerts:
            self.alerts_by_event.setdefault(alert.event_id, []).append(alert)

    # -- per-round assembly ----------------------------------------------

    def build_round(self, rnd: Round,
                    watch_filter: set[str]) -> RoundReplay:
        def in_window(moment: float) -> bool:
            return rnd.start <= moment <= rnd.end

        probes = [row for row in self.probes if in_window(row["checked_at"])]
        snapshots = [snap for snap in self.snapshots
                     if in_window(snap.fetched_at)]
        events = [event for event in self.events
                  if in_window(event.created_at)]
        alerts = [alert for alert in self.alerts
                  if in_window(alert.created_at)]
        watch_names = sorted(
            {row["watch_name"] for row in probes}
            | {snap.watch_name for snap in snapshots}
            | {event.watch_name for event in events}
            | {alert.watch_name for alert in alerts}
        )
        if watch_filter:
            watch_names = [name for name in watch_names
                           if name in watch_filter]
        chains = tuple(
            self.build_chain(
                watch_name,
                round_start=rnd.start,
                probes=[row for row in probes
                        if row["watch_name"] == watch_name],
                snapshots=[snap for snap in snapshots
                           if snap.watch_name == watch_name],
                events=[event for event in events
                        if event.watch_name == watch_name],
                alerts=[alert for alert in alerts
                        if alert.watch_name == watch_name],
            )
            for watch_name in watch_names
        )
        return RoundReplay(index=rnd.index, start=rnd.start, end=rnd.end,
                           chains=chains)

    # -- per-watch chain ---------------------------------------------------

    def build_chain(self, watch_name: str, *,
                    round_start: float,
                    probes: list[dict[str, Any]],
                    snapshots: list[Snapshot],
                    events: list[ChangeEvent],
                    alerts: list[AlertRecord]) -> WatchChain:
        builder = _ChainBuilder(watch_name)
        probe = probes[-1] if probes else None
        snapshot = snapshots[-1] if snapshots else None
        event = events[-1] if events else None
        self._probe_stage(builder, probe, probes, snapshots, events, alerts)
        self._archive_stage(builder, probe, snapshot)
        self._snapshot_stage(builder, probe, snapshot, snapshots, event,
                             watch_name, round_start)
        self._normalize_stage(builder, snapshot)
        self._diff_stage(builder, snapshot, event, events)
        self._classify_stage(builder, snapshot, event)
        self._alert_stage(builder, event, alerts)
        return builder.build()

    def _probe_stage(self, builder: _ChainBuilder,
                     probe: dict[str, Any] | None,
                     probes: list[dict[str, Any]],
                     snapshots: list[Snapshot],
                     events: list[ChangeEvent],
                     alerts: list[AlertRecord]) -> None:
        if probe is None:
            evidence = []
            if snapshots:
                evidence.append(f"snapshot #{snapshots[-1].id}")
            if events:
                evidence.append(f"event #{events[-1].id}")
            if alerts:
                evidence.append(f"alert #{alerts[-1].id}")
            builder.add(
                "probe", MISSING,
                "no probe record for this watch in this round (but "
                + " and ".join(evidence) + " was recorded)")
            return
        builder.add("probe", OK, details={
            "probe_id": probe["id"],
            "checked_at": probe["checked_at"],
            "checked_at_iso": utc_iso(probe["checked_at"]),
            "status_code": probe["status_code"],
            "not_modified": bool(probe["not_modified"]),
            "elapsed_ms": probe["elapsed_ms"],
            "error": probe["error"],
            "raw_sha256": probe["raw_sha256"],
            "probe_count": len(probes),
        })

    def _archive_stage(self, builder: _ChainBuilder,
                       probe: dict[str, Any] | None,
                       snapshot: Snapshot | None) -> None:
        raw_hash = None
        if probe is not None and probe["raw_sha256"]:
            raw_hash = probe["raw_sha256"]
        elif snapshot is not None and snapshot.raw_sha256:
            raw_hash = snapshot.raw_sha256
        if probe is not None and probe["error"]:
            builder.add("archive", SKIPPED,
                        "fetch failed; no response body archived")
            return
        if probe is not None and probe["not_modified"]:
            builder.add("archive", SKIPPED,
                        "304 not modified; no response body archived")
            return
        if raw_hash is None:
            builder.add("archive", SKIPPED,
                        "no raw archive hash recorded for this round")
            return
        relative = f"archive/{raw_hash}.gz"
        details: dict[str, Any] = {"raw_sha256": raw_hash, "path": relative}
        archive_path = self.archive_dir / f"{raw_hash}.gz"
        if not archive_path.exists():
            builder.add("archive", MISSING,
                        f"archive file missing: {relative}", details)
            return
        try:
            actual, size = self.workspace.extract_archive(
                archive_path, raw_hash)
        except Exception as exc:  # corrupt gzip, unreadable file, ...
            builder.add("archive", INVALID,
                        f"archive cannot be decompressed: "
                        f"{type(exc).__name__}: {exc}", details)
            return
        details["size_bytes"] = size
        if actual != raw_hash:
            builder.add("archive", INVALID,
                        "archive hash mismatch: content addressing expects "
                        f"{raw_hash} but decompressed bytes hash to {actual}",
                        details)
            return
        details["verified"] = True
        builder.add("archive", OK, details=details)

    def _snapshot_stage(self, builder: _ChainBuilder,
                        probe: dict[str, Any] | None,
                        snapshot: Snapshot | None,
                        snapshots: list[Snapshot],
                        event: ChangeEvent | None,
                        watch_name: str,
                        round_start: float) -> None:
        if snapshot is None:
            if probe is not None and probe["error"]:
                builder.add("snapshot", SKIPPED,
                            "fetch failed; no snapshot created")
            elif probe is not None and probe["not_modified"]:
                builder.add(
                    "snapshot", SKIPPED,
                    "not modified; previous version retained",
                    self._previous_details(watch_name, round_start))
            elif event is not None:
                builder.add(
                    "snapshot", MISSING,
                    f"event #{event.id} was recorded but no snapshot exists "
                    "for this round")
            elif probe is not None:
                builder.add(
                    "snapshot", SKIPPED,
                    "normalized content unchanged; no new snapshot",
                    self._previous_details(watch_name, round_start))
            else:
                builder.add("snapshot", MISSING,
                            "no snapshot record for this watch in this round")
            return
        details: dict[str, Any] = {
            "snapshot_id": snapshot.id,
            "version": snapshot.version,
            "fetched_at": snapshot.fetched_at,
            "fetched_at_iso": utc_iso(snapshot.fetched_at),
            "status_code": snapshot.status_code,
            "content_sha256": snapshot.content_sha256,
            "raw_sha256": snapshot.raw_sha256,
            "normalizer_version": snapshot.normalizer_version,
            "is_major": snapshot.is_major,
            "snapshot_count": len(snapshots),
        }
        if (probe is not None and probe["raw_sha256"]
                and snapshot.raw_sha256
                and probe["raw_sha256"] != snapshot.raw_sha256):
            builder.add(
                "snapshot", INVALID,
                "snapshot raw hash does not match the probe raw hash "
                f"({snapshot.raw_sha256} != {probe['raw_sha256']})",
                details)
            return
        builder.add("snapshot", OK, details=details)

    def _previous_details(self, watch_name: str,
                          round_start: float) -> dict[str, Any]:
        """Reference to the newest snapshot before this round's window."""
        previous = [snap for snap in self.snapshots
                    if snap.watch_name == watch_name
                    and snap.fetched_at < round_start]
        if not previous:
            return {}
        latest = max(previous, key=lambda snap: (snap.fetched_at, snap.id))
        return {"previous_snapshot": {
            "snapshot_id": latest.id,
            "version": latest.version,
            "content_sha256": latest.content_sha256,
        }}

    def _normalize_stage(self, builder: _ChainBuilder,
                         snapshot: Snapshot | None) -> None:
        if snapshot is None:
            builder.add("normalize", SKIPPED, "no new snapshot this round")
            return
        base: dict[str, Any] = {
            "content_sha256": snapshot.content_sha256,
            "normalizer_version": snapshot.normalizer_version,
        }
        if content_hash(snapshot.normalized_text) != snapshot.content_sha256:
            builder.add("normalize", INVALID,
                        "stored content_sha256 does not match the stored "
                        "normalized text", base)
            return
        try:
            raw_blocks = json.loads(snapshot.blocks_json)
        except ValueError:
            raw_blocks = None
        if not isinstance(raw_blocks, list):
            builder.add("normalize", INVALID,
                        "snapshot blocks_json is not valid JSON", base)
            return
        if not raw_blocks:
            if snapshot.normalized_text:
                builder.add(
                    "normalize", DEGRADED,
                    "no blocks archived; only the whole-text content hash "
                    "is available",
                    {**base, "block_count": 0, "source": "unavailable",
                     "block_hashes": []})
            else:
                builder.add(
                    "normalize", OK,
                    details={**base, "block_count": 0, "source": "archived",
                             "block_hashes": []})
            return
        try:
            entries = [_block_entry(item) for item in raw_blocks]
        except (KeyError, TypeError, ValueError) as exc:
            builder.add("normalize", INVALID,
                        f"snapshot blocks_json contains malformed block "
                        f"entries: {exc}", base)
            return
        details = {**base, "block_count": len(entries)}
        if all(entry["archived_hash"] for entry in entries):
            mismatch = [entry for entry in entries
                        if entry["archived_hash"] != block_hash(entry["text"])]
            details["source"] = "archived"
            details["block_hashes"] = [_public_block(entry, entry["archived_hash"])
                                       for entry in entries]
            if mismatch:
                bad = mismatch[0]
                builder.add(
                    "normalize", INVALID,
                    "archived block hash mismatch at block index "
                    f"{bad['index']}", details)
            else:
                builder.add("normalize", OK, details=details)
            return
        details["source"] = "inferred"
        details["block_hashes"] = [
            _public_block(entry, block_hash(entry["text"]))
            for entry in entries
        ]
        builder.add(
            "normalize", DEGRADED,
            "archived blocks lack a hash field (legacy format); block "
            "hashes inferred from block text (sha256)", details)

    def _diff_stage(self, builder: _ChainBuilder,
                    snapshot: Snapshot | None,
                    event: ChangeEvent | None,
                    events: list[ChangeEvent]) -> None:
        if event is None:
            builder.add("diff", SKIPPED,
                        "no change event recorded this round")
            return
        details: dict[str, Any] = {
            "event_id": event.id,
            "change_rate": event.change_rate,
            "added_lines": event.added_lines,
            "removed_lines": event.removed_lines,
            "regions": list(event.regions),
            "keyword_hits": list(event.keyword_hits),
            "focus_hit": event.focus_hit,
            "from_snapshot_id": event.from_snapshot_id,
            "to_snapshot_id": event.to_snapshot_id,
            "from_snapshot_retained":
                event.from_snapshot_id in self.snapshot_index,
            "event_count": len(events),
        }
        if event.to_snapshot_id not in self.snapshot_index:
            builder.add(
                "diff", INVALID,
                f"event #{event.id} references missing to-snapshot "
                f"#{event.to_snapshot_id}", details)
            return
        if snapshot is not None and event.to_snapshot_id != snapshot.id:
            builder.add(
                "diff", INVALID,
                f"event #{event.id} targets snapshot "
                f"#{event.to_snapshot_id} but this round created snapshot "
                f"#{snapshot.id}", details)
            return
        builder.add("diff", OK, details=details)

    def _classify_stage(self, builder: _ChainBuilder,
                        snapshot: Snapshot | None,
                        event: ChangeEvent | None) -> None:
        if event is None:
            builder.add("classify", SKIPPED,
                        "no change event recorded this round")
            return
        details: dict[str, Any] = {
            "event_id": event.id,
            "severity": event.severity,
            "summary": event.summary,
        }
        if (event.severity == "major" and snapshot is not None
                and not snapshot.is_major):
            builder.add("classify", INVALID,
                        "event severity is major but the round snapshot is "
                        "not flagged is_major", details)
            return
        builder.add("classify", OK, details=details)

    def _alert_stage(self, builder: _ChainBuilder,
                     event: ChangeEvent | None,
                     alerts: list[AlertRecord]) -> None:
        orphans = [alert for alert in alerts
                   if alert.event_id not in self.event_ids]
        if event is None:
            if orphans:
                builder.add(
                    "alert", INVALID,
                    f"alert #{orphans[0].id} references missing event "
                    f"#{orphans[0].event_id}",
                    {"alert_id": orphans[0].id,
                     "event_id": orphans[0].event_id})
            else:
                builder.add("alert", SKIPPED, "no event; no alert expected")
            return
        linked = self.alerts_by_event.get(event.id, [])
        if not linked:
            builder.add(
                "alert", SKIPPED,
                "no alert recorded (severity not alertable or suppressed "
                "by cooldown)")
            return
        alert = linked[-1]
        builder.add("alert", OK, details={
            "alert_id": alert.id,
            "severity": alert.severity,
            "dedup_key": alert.dedup_key,
            "created_at": alert.created_at,
            "created_at_iso": utc_iso(alert.created_at),
            "message": alert.message,
            "alert_count": len(linked),
        })


def _block_entry(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise TypeError("block entry is not an object")
    return {
        "index": int(item["index"]),
        "text": str(item["text"]),
        "region": str(item.get("region", "")),
        "is_focus": bool(item.get("is_focus", False)),
        "is_noise": bool(item.get("is_noise", False)),
        "archived_hash": str(item.get("hash", "")),
    }


def _public_block(entry: dict[str, Any], hash_value: str) -> dict[str, Any]:
    return {
        "index": entry["index"],
        "hash": hash_value,
        "region": entry["region"],
        "is_focus": entry["is_focus"],
        "is_noise": entry["is_noise"],
    }


def build_replay(db_path: str | Path, archive_dir: str | Path, *,
                 rounds: Sequence[Round],
                 watch_names: Sequence[str] = (),
                 workspace: ReplayWorkspace,
                 generated_at: str | None = None) -> ReplayReport:
    """Build the replay report for the selected rounds.

    Reads the ledger strictly read-only; archive verification decompresses
    into ``workspace`` only.  The result is a pure function of the stored
    data, so replays of the same records are byte-for-byte reproducible.
    """
    with closing(connect_readonly(db_path)) as connection:
        probes = [dict(row) for row in connection.execute(
            "SELECT * FROM probes ORDER BY id").fetchall()]
        snapshots = all_snapshots(connection)
        events = list_events(connection)
        alerts = list_alerts(connection)
    builder = _ReplayBuilder(Path(archive_dir), workspace, probes,
                             snapshots, events, alerts)
    watch_filter = set(watch_names)
    round_replays = tuple(
        builder.build_round(rnd, watch_filter) for rnd in rounds)
    chains = [chain for rnd in round_replays for chain in rnd.chains]
    if any(chain.status == "broken" for chain in chains):
        status = "broken"
    elif any(chain.status == "degraded" for chain in chains):
        status = "degraded"
    else:
        status = "ok"
    if generated_at is None:
        latest = max((rnd.end for rnd in round_replays), default=0.0)
        generated_at = utc_iso(latest)
    return ReplayReport(generated_at=generated_at, status=status,
                        rounds=round_replays)
