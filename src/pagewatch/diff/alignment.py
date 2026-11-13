"""Anchor-based block alignment with a line-level fallback for unmatched blocks.

The alignment never trusts absolute position as part of a block's identity
(see :mod:`pagewatch.normalize.anchors`).  Blocks are paired by their
position-free content anchors in three stages:

1. *strong pairs* -- identical ``(self, previous, next)`` context keys, unique
   on both sides.  These survive pure insertion, deletion and whole-section
   translation, and may legitimately cross ordering;
2. *unique weak pairs* -- identical text fingerprint with exactly one
   remaining candidate on each side (context lost at section edges);
3. *order-preserving weighted matching* for the residual duplicate-text
   blocks; position only enters as a small bounded tie-break there.

Blocks left unpaired are dropped from both sides and compared once with the
legacy line-level :class:`difflib.SequenceMatcher`, so the anchor layer can
only refine the old behaviour, never replace it wholesale.  Any failure
raises :class:`AnchorAlignmentError`; the runner catches it and degrades the
whole watch round to the legacy line-level diff.
"""

from __future__ import annotations

import difflib
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from ..models import ChangeBlock, NormalizedBlock
from ..normalize.anchors import BlockAnchor, build_anchors
from .blockdiff import _make_change

# Integer weights keep the dynamic program free of floating-point ties.
_W_BASE = 100           # same normalized-text fingerprint
_W_NEIGHBOUR = 50       # one neighbouring fingerprint matches
_W_PROXIMITY_NUM = 25   # position tie-break: _W_PROXIMITY_NUM // (1 + distance)


class AnchorAlignmentError(RuntimeError):
    """Raised when anchor alignment cannot produce a trustworthy result."""


@dataclass(frozen=True)
class AlignmentResult:
    pairs: tuple[tuple[int, int], ...]
    unmatched_old: tuple[int, ...]
    unmatched_new: tuple[int, ...]
    strong_pairs: int


def _pair_weight(old_anchor: BlockAnchor, new_anchor: BlockAnchor) -> int:
    weight = _W_BASE
    if old_anchor.prev_fp == new_anchor.prev_fp:
        weight += _W_NEIGHBOUR
    if old_anchor.next_fp == new_anchor.next_fp:
        weight += _W_NEIGHBOUR
    # Bounded tie-break only: the smallest content signal outweighs distance.
    weight += _W_PROXIMITY_NUM // (1 + abs(old_anchor.index - new_anchor.index))
    return weight


def _weighted_lcs(old_anchors: Sequence[BlockAnchor],
                  new_anchors: Sequence[BlockAnchor],
                  old_pool: Sequence[int], new_pool: Sequence[int]
                  ) -> tuple[tuple[int, int], ...]:
    """Maximum-weight order-preserving matching within the residual pools."""
    n, m = len(old_pool), len(new_pool)
    dp = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        old_anchor = old_anchors[old_pool[i - 1]]
        row = dp[i]
        previous = dp[i - 1]
        for j in range(1, m + 1):
            best = max(previous[j], row[j - 1])
            new_anchor = new_anchors[new_pool[j - 1]]
            if old_anchor.fp == new_anchor.fp:
                candidate = previous[j - 1] + _pair_weight(old_anchor, new_anchor)
                if candidate > best:
                    best = candidate
            row[j] = best
    pairs: list[tuple[int, int]] = []
    i, j = n, m
    while i > 0 and j > 0:
        old_anchor = old_anchors[old_pool[i - 1]]
        new_anchor = new_anchors[new_pool[j - 1]]
        if (old_anchor.fp == new_anchor.fp
                and dp[i][j] == dp[i - 1][j - 1]
                + _pair_weight(old_anchor, new_anchor)):
            pairs.append((old_pool[i - 1], new_pool[j - 1]))
            i -= 1
            j -= 1
        elif dp[i][j] == dp[i - 1][j]:
            i -= 1
        else:
            j -= 1
    pairs.reverse()
    return tuple(pairs)


def _residual_pairs(old_anchors: Sequence[BlockAnchor],
                    new_anchors: Sequence[BlockAnchor],
                    old_pool: Sequence[int], new_pool: Sequence[int]
                    ) -> tuple[tuple[int, int], ...]:
    """Stage-3 matching with linear fast paths before the weighted LCS."""
    if not old_pool or not new_pool:
        return ()
    old_fps = {old_anchors[i].fp for i in old_pool}
    new_fps = {new_anchors[j].fp for j in new_pool}
    if old_fps.isdisjoint(new_fps):
        return ()
    # Every residual block carries the same fingerprint (e.g. a long run of
    # identical boilerplate): an order-preserving k-to-k match is optimal and
    # linear, avoiding the O(n*m) dynamic program entirely.
    if len(old_fps) == 1 and len(new_fps) == 1 and old_fps == new_fps:
        common = min(len(old_pool), len(new_pool))
        return tuple((old_pool[k], new_pool[k]) for k in range(common))
    return _weighted_lcs(old_anchors, new_anchors, old_pool, new_pool)


def align_blocks(old: Sequence[NormalizedBlock],
                 new: Sequence[NormalizedBlock]) -> AlignmentResult:
    """Compute validated anchor pairs and the unmatched block indices."""
    old_anchors = build_anchors(old)
    new_anchors = build_anchors(new)
    if len(old_anchors) != len(old) or len(new_anchors) != len(new):
        raise AnchorAlignmentError("anchor count does not match block count")

    pairs: set[tuple[int, int]] = set()
    paired_old: set[int] = set()
    paired_new: set[int] = set()

    # Stage 1: strong context pairs, unique on both sides (order may cross).
    old_context: dict[tuple, list[int]] = {}
    new_context: dict[tuple, list[int]] = {}
    for index, anchor in enumerate(old_anchors):
        old_context.setdefault(anchor.context_key, []).append(index)
    for index, anchor in enumerate(new_anchors):
        new_context.setdefault(anchor.context_key, []).append(index)
    strong = 0
    for key, old_ids in old_context.items():
        new_ids = new_context.get(key, ())
        if len(old_ids) == 1 and len(new_ids) == 1:
            pairs.add((old_ids[0], new_ids[0]))
            paired_old.add(old_ids[0])
            paired_new.add(new_ids[0])
            strong += 1

    # Stage 2: unique weak fingerprint pairs across the residual blocks.
    old_pool = [i for i in range(len(old_anchors)) if i not in paired_old]
    new_pool = [j for j in range(len(new_anchors)) if j not in paired_new]
    old_fp = Counter(old_anchors[i].fp for i in old_pool)
    new_fp = Counter(new_anchors[j].fp for j in new_pool)
    for i in list(old_pool):
        fp = old_anchors[i].fp
        if old_fp[fp] == 1 and new_fp[fp] == 1:
            j = next(j for j in new_pool if new_anchors[j].fp == fp)
            pairs.add((i, j))
            paired_old.add(i)
            paired_new.add(j)

    # Stage 3: weighted order-preserving matching of the duplicate remainder.
    old_pool = [i for i in range(len(old_anchors)) if i not in paired_old]
    new_pool = [j for j in range(len(new_anchors)) if j not in paired_new]
    stage3 = _residual_pairs(old_anchors, new_anchors, old_pool, new_pool)
    for i, j in stage3:
        pairs.add((i, j))
        paired_old.add(i)
        paired_new.add(j)

    ordered = tuple(sorted(pairs))
    _validate(ordered, len(old), len(new), old_anchors, new_anchors)
    return AlignmentResult(
        pairs=ordered,
        unmatched_old=tuple(i for i in range(len(old)) if i not in paired_old),
        unmatched_new=tuple(j for j in range(len(new)) if j not in paired_new),
        strong_pairs=strong,
    )


def _validate(pairs: Sequence[tuple[int, int]], old_size: int,
              new_size: int, old_anchors: Sequence[BlockAnchor],
              new_anchors: Sequence[BlockAnchor]) -> None:
    seen_old: set[int] = set()
    seen_new: set[int] = set()
    for old_index, new_index in pairs:
        if not (0 <= old_index < old_size and 0 <= new_index < new_size):
            raise AnchorAlignmentError("pair index out of bounds")
        if old_index in seen_old or new_index in seen_new:
            raise AnchorAlignmentError("anchor pairing is not one-to-one")
        if old_anchors[old_index].fp != new_anchors[new_index].fp:
            raise AnchorAlignmentError("pair fingerprints differ")
        seen_old.add(old_index)
        seen_new.add(new_index)


def _empty_range(size: int, anchor: int) -> tuple[int, int]:
    anchor = max(0, min(anchor, size))
    return (anchor, anchor)


def _cluster_segments(old: Sequence[NormalizedBlock],
                      new: Sequence[NormalizedBlock],
                      segments: Sequence[tuple[str, tuple[int, ...],
                                                tuple[int, ...]]]
                      ) -> tuple[ChangeBlock, ...]:
    """Cluster adjacent non-equal opcodes, mirroring blockdiff's semantics."""
    changes: list[ChangeBlock] = []
    pending: ChangeBlock | None = None
    for tag, old_ids, new_ids in segments:
        if not old_ids and not new_ids:
            continue
        cluster = _make_change(tag,
                               [old[k] for k in old_ids],
                               [new[k] for k in new_ids],
                               (old_ids[0], old_ids[-1] + 1) if old_ids
                               else _empty_range(len(old), new_ids[0]),
                               (new_ids[0], new_ids[-1] + 1) if new_ids
                               else _empty_range(len(new), old_ids[0]))
        if (pending is not None and pending.old_range[1] == cluster.old_range[0]
                and pending.new_range[1] == cluster.new_range[0]):
            merged_old = tuple(range(pending.old_range[0], cluster.old_range[1]))
            merged_new = tuple(range(pending.new_range[0], cluster.new_range[1]))
            pending = _make_change(
                "replace",
                [old[k] for k in merged_old if 0 <= k < len(old)],
                [new[k] for k in merged_new if 0 <= k < len(new)],
                (pending.old_range[0], cluster.old_range[1]),
                (pending.new_range[0], cluster.new_range[1]),
            )
        else:
            if pending is not None:
                changes.append(pending)
            pending = cluster
    if pending is not None:
        changes.append(pending)
    return tuple(changes)


def _fallback_changes(old: Sequence[NormalizedBlock],
                      new: Sequence[NormalizedBlock],
                      alignment: AlignmentResult) -> tuple[ChangeBlock, ...]:
    """Line-level diff over the unmatched block subsequences.

    Paired blocks are dropped from both sides first, so the fallback can never
    re-touch an anchor match -- including when strong pairs cross ordering, as
    happens for pure section translation.
    """
    if not alignment.unmatched_old and not alignment.unmatched_new:
        return ()
    matcher = difflib.SequenceMatcher(
        None, [old[k].text for k in alignment.unmatched_old],
        [new[k].text for k in alignment.unmatched_new], autojunk=False)
    segments = []
    for tag, a1, a2, b1, b2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        segments.append((
            tag,
            tuple(alignment.unmatched_old[a1:a2]),
            tuple(alignment.unmatched_new[b1:b2]),
        ))
    return _cluster_segments(old, new, segments)


def anchor_diff_blocks(old: Sequence[NormalizedBlock],
                       new: Sequence[NormalizedBlock]) -> tuple[ChangeBlock, ...]:
    """Anchor-paired diff; only unmatched blocks use the line-level fallback."""
    try:
        alignment = align_blocks(old, new)
        return _fallback_changes(old, new, alignment)
    except AnchorAlignmentError:
        raise
    except Exception as exc:  # defensive: any defect must degrade, not crash
        raise AnchorAlignmentError(
            f"anchor alignment failed: {type(exc).__name__}: {exc}") from exc
