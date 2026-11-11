"""Block alignment and clustering of adjacent changed blocks."""

from __future__ import annotations

import difflib
from collections.abc import Sequence

from ..models import ChangeBlock, NormalizedBlock


def _region_values(blocks: Sequence[NormalizedBlock]) -> tuple[str, ...]:
    return tuple(sorted({block.region for block in blocks}))


def _make_change(opcode: str, old: Sequence[NormalizedBlock],
                 new: Sequence[NormalizedBlock],
                 old_range: tuple[int, int],
                 new_range: tuple[int, int]) -> ChangeBlock:
    combined = list(old) + list(new)
    focus = any(block.is_focus for block in combined)
    noise_only = bool(combined) and all(block.is_noise for block in combined)
    return ChangeBlock(
        op=opcode,
        old_range=old_range,
        new_range=new_range,
        old_texts=tuple(block.text for block in old),
        new_texts=tuple(block.text for block in new),
        regions=_region_values(combined),
        focus_hit=focus,
        noise_only=noise_only,
    )


def diff_blocks(old: Sequence[NormalizedBlock],
                new: Sequence[NormalizedBlock]) -> tuple[ChangeBlock, ...]:
    """Align blocks and merge consecutive non-equal opcodes into clusters."""
    matcher = difflib.SequenceMatcher(
        None,
        [block.text for block in old],
        [block.text for block in new],
        autojunk=False,
    )
    changes: list[ChangeBlock] = []
    pending: ChangeBlock | None = None
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            if pending is not None:
                changes.append(pending)
                pending = None
            continue
        cluster = _make_change(tag, old[i1:i2], new[j1:j2], (i1, i2), (j1, j2))
        if (pending is not None and pending.old_range[1] == cluster.old_range[0]
                and pending.new_range[1] == cluster.new_range[0]):
            combined_old = list(range(pending.old_range[0], cluster.old_range[1]))
            combined_new = list(range(pending.new_range[0], cluster.new_range[1]))
            old_blocks = [old[i] for i in combined_old if 0 <= i < len(old)]
            new_blocks = [new[j] for j in combined_new if 0 <= j < len(new)]
            pending = _make_change(
                "replace", old_blocks, new_blocks,
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
