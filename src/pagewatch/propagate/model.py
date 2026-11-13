"""Attribution data model and the tunable attribution defaults.

The defaults below encode the attribution policy:

* ``DEFAULT_WINDOW_SECONDS`` — two change events are only ever compared when
  their own ``created_at`` timestamps are at most this far apart.  The default
  of 180 s covers one monitoring round (concurrent fetches of a round spread
  over a few seconds) plus the immediately adjacent round for watch intervals
  up to ~2 minutes.  Events further apart are never linked, no matter how
  similar the content is.
* ``DEFAULT_MAX_SPAN_SECONDS`` — a hard cap on the total time span of one
  propagation cluster.  Pairwise windows alone would let clusters chain
  without bound (A~B, B~C, C~D, ...), so any connected component whose span
  exceeds this cap is deterministically split at its largest time gap.  The
  default of 600 s bounds a cluster to roughly "same round or adjacent
  rounds" for typical intervals.
* ``DEFAULT_MIN_BLOCK_CHARS`` — blocks shorter than this never participate in
  fingerprints: short labels ("仅本机", "下一页") occur on many unrelated
  pages and would fabricate links.
* ``TIME_DECAY_FLOOR`` — link confidence decays linearly with the event time
  gap, from 1.0 (same timestamp) down to this floor at the window edge.
  Content overlap stays the dominant signal; temporal distance can at most
  halve it.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_WINDOW_SECONDS = 180.0
DEFAULT_MAX_SPAN_SECONDS = 600.0
DEFAULT_MIN_BLOCK_CHARS = 8

TIME_DECAY_FLOOR = 0.5
TIMESTAMP_WEIGHT = 0.6
TIMESTAMP_WEIGHT_SPAN = 0.4
PATH_PREFIX_WEIGHT = 0.25
NAME_FALLBACK_WEIGHT = 0.1

MAX_SHARED_SAMPLES = 5
SAMPLE_MAX_CHARS = 80

LINK_KINDS = ("added", "removed", "moved")


@dataclass(frozen=True)
class EventFingerprint:
    """Content hashes one event added to / removed from its page."""

    event_id: int
    watch_name: str
    created_at: float
    url: str | None
    added: dict[str, str]
    removed: dict[str, str]

    @property
    def has_content(self) -> bool:
        return bool(self.added or self.removed)


@dataclass(frozen=True)
class PropagationLink:
    """One directed, evidence-carrying edge between two change events."""

    id: str
    kind: str
    source_event_id: int
    source_watch: str
    downstream_event_id: int
    downstream_watch: str
    shared_count: int
    shared_samples: tuple[str, ...]
    time_gap_seconds: float
    confidence: float
    direction_confidence: float
    direction_basis: tuple[str, ...]


@dataclass(frozen=True)
class PropagationCluster:
    """A connected group of links: one root source and its downstreams."""

    id: str
    root_watch: str
    watches: tuple[str, ...]
    event_ids: tuple[int, ...]
    link_ids: tuple[str, ...]
    started_at: float
    ended_at: float
    confidence: float


@dataclass(frozen=True)
class UnattributedEvent:
    """An event that could not be placed into any propagation cluster."""

    event_id: int
    watch_name: str
    reason: str


@dataclass(frozen=True)
class AttributionResult:
    """Full, deterministic outcome of one attribution run."""

    generated_at: str
    window_seconds: float
    max_span_seconds: float
    min_block_chars: int
    event_count: int
    watch_count: int
    degraded: bool
    degrade_reasons: tuple[str, ...]
    links: tuple[PropagationLink, ...]
    clusters: tuple[PropagationCluster, ...]
    unattributed: tuple[UnattributedEvent, ...]

    @property
    def confidence(self) -> float | None:
        """Strongest link confidence; ``None`` when nothing was attributed."""
        if not self.links:
            return None
        return max(link.confidence for link in self.links)
