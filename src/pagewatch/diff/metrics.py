"""Change-rate and diff metrics.

The primary path counts changes at block granularity on anchor-aligned
:class:`ChangeBlock` sequences, so insertion, deletion and translation of
unchanged blocks no longer inflate the rate.  The legacy line-level
computation is kept verbatim for the degraded fallback path, guaranteeing a
fallback round produces exactly the numbers the old pipeline would have.
"""

from __future__ import annotations

from collections.abc import Sequence

from ..models import ChangeBlock, DiffMetrics, NormalizedBlock
from .textdiff import changed_line_counts, split_lines


def keyword_hits(changes: Sequence[ChangeBlock],
                 keywords: Sequence[str]) -> tuple[str, ...]:
    haystack = "\n".join(text for change in changes for text in change.new_texts)
    folded = haystack.casefold()
    return tuple(keyword for keyword in keywords if keyword.casefold() in folded)


def _regions(changes: Sequence[ChangeBlock]) -> tuple[str, ...]:
    return tuple(sorted({region for change in changes for region in change.regions}))


def _flags(changes: Sequence[ChangeBlock]) -> tuple[bool, bool]:
    focus = any(change.focus_hit for change in changes)
    noise_only = bool(changes) and all(change.noise_only for change in changes)
    return focus, noise_only


def compute_metrics(old_blocks: Sequence[NormalizedBlock],
                    new_blocks: Sequence[NormalizedBlock],
                    changes: Sequence[ChangeBlock],
                    keywords: Sequence[str] = ()) -> DiffMetrics:
    """Block-level metrics derived from the anchor-aligned change clusters."""
    added = sum(len(change.new_texts) for change in changes)
    removed = sum(len(change.old_texts) for change in changes)
    base_count = len(old_blocks)
    new_count = len(new_blocks)
    changed = added + removed
    denominator = max(base_count, new_count, 1)
    change_rate = min(1.0, changed / denominator)
    focus, noise_only = _flags(changes)
    return DiffMetrics(
        base_lines=base_count,
        new_lines=new_count,
        added_lines=added,
        removed_lines=removed,
        changed_lines=changed,
        change_rate=change_rate,
        regions=_regions(changes),
        focus_hit=focus,
        noise_only=noise_only,
        keyword_hits=keyword_hits(changes, keywords),
    )


def compute_legacy_metrics(old_blocks: Sequence[NormalizedBlock],
                           new_blocks: Sequence[NormalizedBlock],
                           changes: Sequence[ChangeBlock],
                           keywords: Sequence[str] = ()) -> DiffMetrics:
    """Original line-window metrics; used only when anchor alignment degrades."""
    old_text = "\n".join(block.text for block in old_blocks)
    new_text = "\n".join(block.text for block in new_blocks)
    added, removed = changed_line_counts(old_text, new_text)
    base_lines = len(split_lines(old_text))
    new_lines = len(split_lines(new_text))
    changed = added + removed
    denominator = max(base_lines, new_lines, 1)
    change_rate = min(1.0, changed / denominator)
    focus, noise_only = _flags(changes)
    return DiffMetrics(
        base_lines=base_lines,
        new_lines=new_lines,
        added_lines=added,
        removed_lines=removed,
        changed_lines=changed,
        change_rate=change_rate,
        regions=_regions(changes),
        focus_hit=focus,
        noise_only=noise_only,
        keyword_hits=keyword_hits(changes, keywords),
    )
