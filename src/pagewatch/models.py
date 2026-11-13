"""Typed data structures shared across pagewatch."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

SEVERITIES = ("noise", "minor", "major")
RULE_KINDS = (
    "focus",              # a changed block inside a focus region
    "keyword",            # a user keyword appears in changed text
    "threshold",          # change rate crosses a configured threshold
    "noise_region",       # every changed block sits in a noise region
    "block_structure",    # blocks were inserted or deleted, not just rewritten
    "severity_mapping",   # map already-matched conditions to a severity
    "default",            # unconditional fallback
)
RULE_DIRECTIONS = (">=", "<")
MAX_RULE_PRIORITY = 10_000


@dataclass(frozen=True)
class RulePatch:
    """A per-rule override from rules.yaml or a watch's rule_overrides.

    ``None`` means "inherit the value from the rule being patched"; an omitted
    ``rule_id`` in YAML is filled in by the loader.
    """

    rule_id: str
    source: str
    kind: str | None = None
    severity: str | None = None
    priority: int | None = None
    enabled: bool | None = None
    threshold: float | None = None
    direction: str | None = None
    when: tuple[str, ...] | None = None
    label: str | None = None

    def __post_init__(self) -> None:
        # Structural sanity only: enum membership is checked by the engine
        # (``RuleConfigError``) so both config loading and programmatic use
        # report the same error type.
        if not isinstance(self.rule_id, str) or not self.rule_id.strip():
            raise ValueError("rule id must be a non-empty string")
        if (self.priority is not None
                and not 0 <= self.priority <= MAX_RULE_PRIORITY):
            raise ValueError(f"rule priority must be 0..{MAX_RULE_PRIORITY}")
        if self.threshold is not None and not 0.0 <= self.threshold <= 1.0:
            raise ValueError("rule threshold must be between 0 and 1")
        if self.direction is not None and self.direction not in RULE_DIRECTIONS:
            raise ValueError("rule direction must be '>=' or '<'")


@dataclass(frozen=True)
class WatchSpec:
    name: str
    url: str
    interval_seconds: int
    tags: tuple[str, ...] = ()
    include_selectors: tuple[str, ...] = ()
    exclude_selectors: tuple[str, ...] = ()
    noise_patterns: tuple[str, ...] = ()
    keywords: tuple[str, ...] = ()
    enabled: bool = True
    rules_overrides: dict[str, Any] = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class Rules:
    noise_max_change_rate: float = 0.02
    major_change_rate: float = 0.35
    focus_selectors: tuple[str, ...] = ()
    alert_severities: tuple[str, ...] = ("major",)
    alert_cooldown_seconds: int = 3600
    diff_max_chars: int = 12_000
    keep_last: int = 5
    major_keep_forever: bool = True
    archive_max_age_days: int = 30
    max_workers: int = 4
    jitter_seconds: int = 10
    timeout_seconds: float = 5.0
    max_bytes: int = 2_097_152
    # Rule-engine definitions layered on top of the legacy scalar fields.
    # These stay out of ``RULE_FIELDS``/flat overrides on purpose.
    rules: tuple[RulePatch, ...] = field(default=(), compare=False)


@dataclass(frozen=True)
class DataPaths:
    root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))

    @property
    def db_path(self) -> Path:
        return self.root / "pagewatch.sqlite"

    @property
    def archive_dir(self) -> Path:
        return self.root / "archive"

    @property
    def outbox_path(self) -> Path:
        return self.root / "outbox" / "alerts.jsonl"

    @property
    def out_dir(self) -> Path:
        return self.root / "out"


@dataclass(frozen=True)
class NormalizedBlock:
    index: int
    text: str
    tag: str
    region: str
    is_focus: bool = False
    is_noise: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "NormalizedBlock":
        return cls(
            index=int(value["index"]),
            text=str(value["text"]),
            tag=str(value["tag"]),
            region=str(value["region"]),
            is_focus=bool(value.get("is_focus", False)),
            is_noise=bool(value.get("is_noise", False)),
        )


@dataclass(frozen=True)
class NormalizedPage:
    text: str
    blocks: tuple[NormalizedBlock, ...]
    content_hash: str
    normalizer_version: str

    @property
    def line_count(self) -> int:
        return len(self.text.splitlines())


@dataclass(frozen=True)
class ChangeBlock:
    op: str
    old_range: tuple[int, int]
    new_range: tuple[int, int]
    old_texts: tuple[str, ...]
    new_texts: tuple[str, ...]
    regions: tuple[str, ...]
    focus_hit: bool = False
    noise_only: bool = False

    @property
    def structural(self) -> bool:
        """True when a block appeared or disappeared entirely."""
        return self.op in {"insert", "delete"}


@dataclass(frozen=True)
class RuleVerdict:
    """One rule's opinion, annotated for replay and auditing."""

    rule_id: str
    kind: str
    source: str
    severity: str
    priority: int
    matched: bool
    reason: str
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("reason", "detail"):
            if not value[key]:
                value.pop(key)
        return value


@dataclass(frozen=True)
class Classification:
    """Final severity plus the full, ordered rule trace that produced it."""

    severity: str
    trace: tuple[RuleVerdict, ...] = ()
    engine_version: str = ""

    @property
    def matched(self) -> tuple[RuleVerdict, ...]:
        return tuple(verdict for verdict in self.trace if verdict.matched)

    @property
    def reasons(self) -> list[str]:
        return [verdict.reason for verdict in self.matched]

    @property
    def winning_reasons(self) -> list[str]:
        """Reasons of matching rules at the winning level (legacy summary)."""
        return [verdict.reason for verdict in self.matched
                if verdict.severity == self.severity and verdict.reason]

    def explain(self) -> dict[str, Any]:
        return {
            "engine_version": self.engine_version,
            "final": self.severity,
            "verdicts": [verdict.as_dict() for verdict in self.trace],
        }


@dataclass(frozen=True)
class DiffMetrics:
    base_lines: int
    new_lines: int
    added_lines: int
    removed_lines: int
    changed_lines: int
    change_rate: float
    regions: tuple[str, ...]
    focus_hit: bool
    noise_only: bool
    keyword_hits: tuple[str, ...]
    block_ops: tuple[str, ...] = ()
    block_count: int = 0
    added_blocks: int = 0
    removed_blocks: int = 0
    focus_regions: tuple[str, ...] = ()
    noise_regions: tuple[str, ...] = ()


@dataclass(frozen=True)
class Snapshot:
    id: int
    watch_name: str
    version: int
    fetched_at: float
    status_code: int
    url: str
    etag: str | None
    last_modified: str | None
    content_sha256: str
    raw_sha256: str | None
    normalized_text: str
    blocks_json: str
    normalizer_version: str
    is_major: bool


@dataclass(frozen=True)
class ChangeEvent:
    id: int
    watch_name: str
    from_snapshot_id: int
    to_snapshot_id: int
    created_at: float
    severity: str
    change_rate: float
    added_lines: int
    removed_lines: int
    regions: tuple[str, ...]
    keyword_hits: tuple[str, ...]
    focus_hit: bool
    summary: str
    diff_text: str
    rule_trace: str = ""

    def as_json(self) -> dict[str, Any]:
        value = asdict(self)
        value["regions"] = list(self.regions)
        value["keyword_hits"] = list(self.keyword_hits)
        value["focus_hit"] = self.focus_hit
        if self.rule_trace:
            try:
                value["rule_trace"] = json.loads(self.rule_trace)
            except (TypeError, ValueError):
                value["rule_trace"] = self.rule_trace
        else:
            value.pop("rule_trace", None)
        return value


@dataclass(frozen=True)
class AlertRecord:
    id: int
    watch_name: str
    severity: str
    event_id: int
    created_at: float
    dedup_key: str
    message: str

    def as_jsonl(self) -> dict[str, Any]:
        return asdict(self)
