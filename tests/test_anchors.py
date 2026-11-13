from __future__ import annotations

from pagewatch.models import NormalizedBlock
from pagewatch.normalize.anchors import build_anchors, fingerprint


def blocks(*texts):
    return tuple(NormalizedBlock(i, text, "p", "#main")
                 for i, text in enumerate(texts))


def test_fingerprint_is_stable_and_text_sensitive():
    assert fingerprint("same") == fingerprint("same")
    assert fingerprint("same") != fingerprint("different")


def test_anchors_are_position_free_and_context_aware():
    anchors = build_anchors(blocks("a", "b", "c"))
    fps = tuple(fingerprint(text) for text in ("a", "b", "c"))
    assert anchors[0].context_key == (fps[0], None, fps[1])
    assert anchors[1].context_key == (fps[1], fps[0], fps[2])
    assert anchors[2].context_key == (fps[2], fps[1], None)


def test_interior_blocks_keep_context_anchors_through_top_insertion():
    before = build_anchors(blocks("a", "b", "c"))
    after = build_anchors(blocks("banner", "a", "b", "c"))
    # Only the boundary block ("a") loses its predecessor; interior anchors
    # survive the whole insertion unchanged.
    assert before[1].context_key == after[2].context_key
    assert before[2].context_key == after[3].context_key
    # The boundary block still matches on its position-free weak fingerprint.
    assert before[0].fp == after[1].fp


def test_position_free_fingerprints_survive_section_translation():
    before = build_anchors(blocks("a", "b", "c", "d", "e"))
    after = build_anchors(blocks("d", "e", "a", "b", "c"))
    # A block translated together with both neighbours keeps the full anchor.
    assert before[1].context_key == after[3].context_key
    # Identity is position-free: every block's fingerprint reappears verbatim.
    assert sorted(anchor.fp for anchor in before) == sorted(
        anchor.fp for anchor in after)


def test_duplicate_blocks_get_distinct_anchors_from_context():
    anchors = build_anchors(blocks("dup", "x", "dup", "y", "dup"))
    keys = [anchor.context_key for anchor in anchors]
    assert len(set(keys)) == len(keys)
    # Interior duplicates with identical neighbours share only their weak fp.
    assert anchors[0].fp == anchors[2].fp == anchors[4].fp
    assert anchors[0].prev_fp is None and anchors[4].next_fp is None
