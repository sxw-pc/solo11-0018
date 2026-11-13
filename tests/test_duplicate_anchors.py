"""Duplicate-block self-consistency checks for the anchor criterion.

The chosen criterion is position-free: duplicate text blocks must never be
mismatched merely because another identical block exists nearby.  These
cases pin down the disambiguation rules (unique weak pairs, ordered k-to-k
matching for residual duplicates, context isolation of edits).
"""

from __future__ import annotations

from pagewatch.diff.alignment import align_blocks, anchor_diff_blocks
from pagewatch.diff.metrics import compute_metrics
from pagewatch.models import NormalizedBlock


def blocks(*texts):
    return tuple(NormalizedBlock(i, text, "p", "#main")
                 for i, text in enumerate(texts))


def test_repeated_navigation_blocks_stay_matched_under_top_insertion():
    old = blocks("nav", "nav", "nav", "article", "footer")
    new = blocks("promo", "nav", "nav", "nav", "article", "footer")
    alignment = align_blocks(old, new)
    # Every original block survives; only the promo is unmatched.
    assert sorted(i for i, _ in alignment.pairs) == list(range(5))
    assert alignment.unmatched_new == (0,)
    changes = anchor_diff_blocks(old, new)
    assert [change.op for change in changes] == ["insert"]
    assert changes[0].new_texts == ("promo",)


def test_identical_duplicates_on_both_sides_pair_in_order():
    old = blocks("dup", "dup", "dup")
    new = blocks("dup", "dup", "dup")
    alignment = align_blocks(old, new)
    assert alignment.pairs == ((0, 0), (1, 1), (2, 2))
    assert anchor_diff_blocks(old, new) == ()


def test_duplicate_count_change_pairs_the_longest_common_run():
    old = blocks("dup", "dup", "dup", "end")
    new = blocks("dup", "dup", "end")
    alignment = align_blocks(old, new)
    # Two of the three duplicates survive; the deleted one is reported once.
    assert len(alignment.pairs) == 3
    changes = anchor_diff_blocks(old, new)
    assert len(changes) == 1 and changes[0].op == "delete"
    assert changes[0].old_texts == ("dup",)
    metrics = compute_metrics(old, new, changes)
    assert metrics.change_rate == 0.25


def test_edit_inside_a_run_of_duplicates_does_not_rewrite_the_run():
    old = blocks("dup", "dup", "dup", "dup")
    new = blocks("dup", "dup", "CHANGED", "dup")
    changes = anchor_diff_blocks(old, new)
    assert len(changes) == 1
    assert changes[0].op == "replace"
    assert changes[0].old_texts == ("dup",)
    assert changes[0].new_texts == ("CHANGED",)


def test_two_distinct_edits_among_duplicates_are_both_captured():
    old = blocks("dup", "a", "dup", "b", "dup")
    new = blocks("dup", "A", "dup", "B", "dup")
    alignment = align_blocks(old, new)
    # All three duplicate anchors survive; only the edited letters are open.
    assert len(alignment.pairs) == 3
    changes = anchor_diff_blocks(old, new)
    assert all(change.op == "replace" for change in changes)
    metrics = compute_metrics(old, new, changes)
    assert metrics.added_lines == 2 and metrics.removed_lines == 2


def test_duplicate_then_translation_keeps_one_to_one_invariant():
    old = blocks("dup", "x", "dup", "y", "dup", "z")
    new = blocks("y", "dup", "z", "dup", "x", "dup")
    alignment = align_blocks(old, new)
    paired_old = [i for i, _ in alignment.pairs]
    paired_new = [j for _, j in alignment.pairs]
    assert len(paired_old) == len(set(paired_old))
    assert len(paired_new) == len(set(paired_new))
    # No pair ever joins blocks with different normalized text.
    for old_index, new_index in alignment.pairs:
        assert old[old_index].text == new[new_index].text
