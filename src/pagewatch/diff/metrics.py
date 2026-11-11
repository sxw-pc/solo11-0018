"""Change-rate and diff metrics."""

from __future__ import annotations

from collections.abc import Sequence

from ..models import ChangeBlock, DiffMetrics, NormalizedBlock
from .textdiff import changed_line_counts, split_lines


def keyword_hits(changes: Sequence[ChangeBlock],
                 keywords: Sequence[str]) -> tuple[str, ...]:
    haystack = "\n".join(text for change in changes for text in change.new_texts)
    folded = haystack.casefold()
    return tuple(keyword for keyword in keywords if keyword.casefold() in folded)


def compute_metrics(old_blocks: Sequence[NormalizedBlock],
                    new_blocks: Sequence[NormalizedBlock],
                    changes: Sequence[ChangeBlock],
                    keywords: Sequence[str] = ()) -> DiffMetrics:
    old_text = "\n".join(block.text for block in old_blocks)
    new_text = "\n".join(block.text for block in new_blocks)
    added, removed = changed_line_counts(old_text, new_text)
    base_lines = len(split_lines(old_text))
    new_lines = len(split_lines(new_text))
    changed = added + removed
    denominator = max(base_lines, new_lines, 1)
    change_rate = min(1.0, changed / denominator)
    regions = tuple(sorted({region for change in changes
                            for region in change.regions}))
    return DiffMetrics(
        base_lines=base_lines,
        new_lines=new_lines,
        added_lines=added,
        removed_lines=removed,
        changed_lines=changed,
        change_rate=change_rate,
        regions=regions,
        focus_hit=any(change.focus_hit for change in changes),
        noise_only=bool(changes) and all(change.noise_only for change in changes),
        keyword_hits=keyword_hits(changes, keywords),
    )
