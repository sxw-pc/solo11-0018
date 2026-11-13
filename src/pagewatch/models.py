"""Typed data structures shared across pagewatch.

Configuration models (``WatchSpec``, ``Rules``, ``RuleOverrides``) are
Pydantic v2 models defined in :mod:`pagewatch.config`; they are re-exported
here so existing ``from pagewatch.models import ...`` call sites keep working.
Everything else remains a frozen dataclass.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import RuleOverrides, Rules, WatchSpec

__all__ = [
    "WatchSpec", "Rules", "RuleOverrides", "DataPaths", "NormalizedBlock",
    "NormalizedPage", "ChangeBlock", "DiffMetrics", "Snapshot", "ChangeEvent",
    "AlertRecord",
]


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

    def as_json(self) -> dict[str, Any]:
        value = asdict(self)
        value["regions"] = list(self.regions)
        value["keyword_hits"] = list(self.keyword_hits)
        value["focus_hit"] = self.focus_hit
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
