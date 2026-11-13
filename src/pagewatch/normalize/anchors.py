"""Stable, position-free anchors for normalized blocks.

Anchor identity is derived solely from content: a block's normalized-text
fingerprint plus the fingerprints of its immediate predecessor and successor.
No ordinal/line position participates in the identity, so pure insertion,
deletion and whole-section translation keep an anchor unchanged.  Position is
used later (in :mod:`pagewatch.diff.alignment`) only as a tie-breaker when
several blocks share an anchor on the same page.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

from ..models import NormalizedBlock

_FINGERPRINT_BYTES = 16


def fingerprint(text: str) -> str:
    """Return a stable short fingerprint of a block's normalized text."""
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return digest[:_FINGERPRINT_BYTES].hex()


@dataclass(frozen=True)
class BlockAnchor:
    """Position-free anchor of one block within a snapshot.

    ``prev_fp``/``next_fp`` are the fingerprints of the neighbouring blocks,
    or ``None`` at the document edges.  ``context_key`` adds both neighbours
    and is the strong matching key; ``fp`` alone is the weak key used for
    duplicate-text blocks whose context changed.
    """

    index: int
    fp: str
    prev_fp: str | None
    next_fp: str | None

    @property
    def context_key(self) -> tuple[str, str | None, str | None]:
        return (self.fp, self.prev_fp, self.next_fp)


def build_anchors(blocks: Sequence[NormalizedBlock]) -> tuple[BlockAnchor, ...]:
    """Compute deterministic position-free anchors for an ordered block list."""
    fingerprints = tuple(fingerprint(block.text) for block in blocks)
    anchors: list[BlockAnchor] = []
    total = len(fingerprints)
    for index, value in enumerate(fingerprints):
        anchors.append(BlockAnchor(
            index=index,
            fp=value,
            prev_fp=fingerprints[index - 1] if index > 0 else None,
            next_fp=fingerprints[index + 1] if index + 1 < total else None,
        ))
    return tuple(anchors)
