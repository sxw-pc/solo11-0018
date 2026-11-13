"""Typed data contracts for round-level quality assurance.

Everything in here is plain, JSON-serialisable data so that a verification
conclusion can be persisted, re-loaded and hashed without any runtime state.
The hash deliberately excludes ``checked_at``: re-running the same checks
against the same database snapshot must reproduce the same conclusion hash.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from typing import Any

# Check identifiers — part of the persisted contract, do not rename.
CHECK_SNAPSHOT_CHAIN = "snapshot_chain"
CHECK_EVENT_REFERENCES = "event_snapshot_references"
CHECK_ALERT_PAIRING = "alert_event_pairing"
CHECK_OUTBOX_PAIRING = "outbox_alert_pairing"
CHECK_MAJOR_RETENTION = "major_target_retention"

ALL_CHECKS = (
    CHECK_SNAPSHOT_CHAIN,
    CHECK_EVENT_REFERENCES,
    CHECK_ALERT_PAIRING,
    CHECK_OUTBOX_PAIRING,
    CHECK_MAJOR_RETENTION,
)

# Finding / check / report verdicts.
PASS = "pass"
FAIL = "fail"
UNKNOWN = "unknown"

# Per-watch lifecycle states.
WATCH_OK = "ok"
WATCH_QUARANTINED = "quarantined"
WATCH_REBASELINED = "rebaselined"


@dataclass(frozen=True)
class Finding:
    """One concrete structural defect (or an inability to judge)."""

    check: str
    code: str
    severity: str  # PASS/FAIL/UNKNOWN — UNKNOWN means "could not judge"
    message: str
    watch_name: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    findings: tuple[Finding, ...] = ()

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.findings if item.severity == FAIL)

    @property
    def unknown_count(self) -> int:
        return sum(1 for item in self.findings if item.severity == UNKNOWN)

    def as_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "status": self.status,
            "finding_count": len(self.findings),
            "failure_count": self.failure_count,
            "unknown_count": self.unknown_count,
            "findings": [item.as_dict() for item in self.findings],
        }


@dataclass(frozen=True)
class QualityReport:
    round_id: str
    checked_at: float
    status: str
    watch_names: tuple[str, ...]
    checks: tuple[CheckResult, ...]
    findings: tuple[Finding, ...]
    quarantined: tuple[str, ...] = ()
    content_hash: str = ""

    def canonical_payload(self) -> dict[str, Any]:
        """Deterministic content used for the reproducibility hash.

        The round id and timestamp intentionally stay out: the hash covers
        the *conclusion*, so re-verifying unchanged data in a later round
        reproduces the same hash and drift is immediately visible.
        """
        return {
            "watch_names": sorted(self.watch_names),
            "checks": [
                {
                    "check": item.check,
                    "status": item.status,
                    "findings": [finding.as_dict() for finding in item.findings],
                }
                for item in sorted(self.checks, key=lambda item: item.check)
            ],
        }

    @staticmethod
    def hash_payload(payload: dict[str, Any]) -> str:
        from ..storage.db import json_dump

        return hashlib.sha256(
            json_dump(payload).encode("utf-8")
        ).hexdigest()

    def with_hash(self) -> "QualityReport":
        digest = self.hash_payload(self.canonical_payload())
        return QualityReport(
            round_id=self.round_id, checked_at=self.checked_at,
            status=self.status, watch_names=self.watch_names, checks=self.checks,
            findings=self.findings, quarantined=self.quarantined,
            content_hash=digest,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "checked_at": self.checked_at,
            "status": self.status,
            "watch_names": list(self.watch_names),
            "quarantined": list(self.quarantined),
            "content_hash": self.content_hash,
            "checks": [item.as_dict() for item in self.checks],
            "findings": [item.as_dict() for item in self.findings],
        }


@dataclass(frozen=True)
class WatchQualityState:
    watch_name: str
    status: str
    consecutive_failures: int
    rebaseline_version: int | None
    first_failed_round_id: str | None
    last_report_round_id: str | None
    updated_at: float

    @property
    def quarantined(self) -> bool:
        return self.status == WATCH_QUARANTINED
