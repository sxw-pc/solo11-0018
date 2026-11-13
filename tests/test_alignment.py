from __future__ import annotations

import pytest

from pagewatch.diff.alignment import (
    AnchorAlignmentError, align_blocks, anchor_diff_blocks,
)
from pagewatch.diff.metrics import compute_metrics
from pagewatch.models import NormalizedBlock


def blocks(*texts, noise=()):
    return tuple(NormalizedBlock(i, text, "p", "#main", False, i in noise)
                 for i, text in enumerate(texts))


def rate(changes, old, new):
    return compute_metrics(old, new, changes).change_rate


def test_pure_top_insertion_matches_every_shifted_block():
    old = blocks(*[f"line {n}" for n in range(10)])
    new = blocks("banner", *[f"line {n}" for n in range(10)])
    alignment = align_blocks(old, new)
    assert alignment.pairs == tuple((n, n + 1) for n in range(10))
    assert alignment.unmatched_old == ()
    assert alignment.unmatched_new == (0,)
    changes = anchor_diff_blocks(old, new)
    assert [change.op for change in changes] == ["insert"]
    assert rate(changes, old, new) == pytest.approx(1 / 11)


def test_pure_section_translation_is_a_zero_change():
    old = blocks("a", "b", "c", "d", "e")
    new = blocks("d", "e", "a", "b", "c")
    alignment = align_blocks(old, new)
    assert sorted(old_index for old_index, _ in alignment.pairs) == list(range(5))
    assert sorted(new_index for _, new_index in alignment.pairs) == list(range(5))
    assert anchor_diff_blocks(old, new) == ()
    assert rate(anchor_diff_blocks(old, new), old, new) == 0.0


def test_pure_deletion_matches_all_survivors():
    old = blocks("a", "b", "c", "d")
    new = blocks("a", "c", "d")
    changes = anchor_diff_blocks(old, new)
    assert [change.op for change in changes] == ["delete"]
    assert changes[0].old_texts == ("b",)
    assert rate(changes, old, new) == pytest.approx(0.25)


def test_full_rewrite_leaves_no_anchor_pairs_and_rates_at_one():
    old = blocks(*[f"old {n}" for n in range(10)])
    new = blocks(*[f"brand new {n}" for n in range(10)])
    alignment = align_blocks(old, new)
    assert alignment.pairs == ()
    assert len(alignment.unmatched_old) == len(old)
    changes = anchor_diff_blocks(old, new)
    metrics = compute_metrics(old, new, changes)
    assert metrics.change_rate == 1.0
    assert all(change.op == "replace" for change in changes)


def test_duplicate_text_blocks_pair_one_to_one_and_keep_order():
    old = blocks("dup", "dup", "dup", "tail")
    new = blocks("head", "dup", "dup", "dup", "tail")
    alignment = align_blocks(old, new)
    assert alignment.pairs == ((0, 1), (1, 2), (2, 3), (3, 4))
    changes = anchor_diff_blocks(old, new)
    assert [change.op for change in changes] == ["insert"]


def test_edit_between_duplicate_blocks_is_isolated():
    old = blocks("dup", "x", "dup", "y", "dup")
    new = blocks("dup", "x", "dup", "EDITED", "dup")
    changes = anchor_diff_blocks(old, new)
    assert len(changes) == 1
    assert changes[0].op == "replace"
    assert changes[0].old_texts == ("y",)
    assert changes[0].new_texts == ("EDITED",)


def test_insertion_inside_repeated_boilerplate_does_not_cascade():
    old = blocks("nav", "nav", "nav", "intro", "one", "two", "footer")
    new = blocks("nav", "nav", "nav", "intro", "one", "two", "extra", "footer")
    changes = anchor_diff_blocks(old, new)
    assert [change.op for change in changes] == ["insert"]
    assert changes[0].new_texts == ("extra",)


def test_noise_only_insertion_is_flagged_noise_only():
    old = blocks(*[f"line {n}" for n in range(20)])
    new = blocks("volatile banner", *[f"line {n}" for n in range(20)],
                 noise=(0,))
    changes = anchor_diff_blocks(old, new)
    assert changes[0].noise_only is True
    assert compute_metrics(old, new, changes).noise_only is True


def test_alignment_is_deterministic():
    old = blocks("dup", "x", "dup", "y", "dup", "z")
    new = blocks("head", "dup", "x", "dup", "y", "dup", "z")
    assert align_blocks(old, new) == align_blocks(old, new)


def test_empty_sides():
    assert anchor_diff_blocks((), blocks("first"))[0].op == "insert"
    assert anchor_diff_blocks(blocks("only"), ())[0].op == "delete"
    assert anchor_diff_blocks((), ()) == ()


def test_alignment_error_on_broken_anchor_invariant(monkeypatch):
    import pagewatch.diff.alignment as alignment_module

    def broken_validate(pairs, old_size, new_size, old_anchors, new_anchors):
        raise AnchorAlignmentError("forced invariant failure")

    monkeypatch.setattr(alignment_module, "_validate", broken_validate)
    with pytest.raises(AnchorAlignmentError, match="forced invariant"):
        align_blocks(blocks("a"), blocks("a"))


def test_unexpected_anchor_failure_is_wrapped(monkeypatch):
    import pagewatch.diff.alignment as alignment_module

    def boom(blocks):
        raise ValueError("anchor boom")

    monkeypatch.setattr(alignment_module, "build_anchors", boom)
    with pytest.raises(AnchorAlignmentError, match="anchor boom"):
        anchor_diff_blocks(blocks("a"), blocks("b"))


def test_alignment_invariants_hold_under_randomized_edits():
    import random

    random.seed(20260101)
    vocabulary = [f"word-{n}" for n in range(8)]  # small vocab -> many duplicates

    def mutate(texts):
        choice = random.choice(
            ["insert", "delete", "replace", "permute", "dup-insert"])
        result = list(texts)
        if choice == "insert":
            for _ in range(random.randint(1, 3)):
                result.insert(random.randrange(len(result) + 1),
                              f"new-{random.randrange(1000)}")
        elif choice == "delete" and result:
            result.pop(random.randrange(len(result)))
        elif choice == "replace" and result:
            result[random.randrange(len(result))] = (
                f"edited-{random.randrange(1000)}")
        elif choice == "permute" and len(result) > 1:
            cut = random.randrange(1, len(result))
            result = result[cut:] + result[:cut]
        elif choice == "dup-insert":
            for _ in range(random.randint(1, 2)):
                result.insert(random.randrange(len(result) + 1),
                              random.choice(texts or ["word-0"]))
        return result

    for _ in range(300):
        base = [random.choice(vocabulary)
                for _ in range(random.randint(0, 20))]
        old, new = blocks(*base), blocks(*mutate(base))
        alignment = align_blocks(old, new)
        paired_old = [i for i, _ in alignment.pairs]
        paired_new = [j for _, j in alignment.pairs]
        # One-to-one, text-identical pairs that partition every block index.
        assert len(paired_old) == len(set(paired_old))
        assert len(paired_new) == len(set(paired_new))
        assert all(old[i].text == new[j].text for i, j in alignment.pairs)
        assert sorted(paired_old + list(alignment.unmatched_old)) == list(
            range(len(old)))
        assert sorted(paired_new + list(alignment.unmatched_new)) == list(
            range(len(new)))
        # Deterministic and within the valid metric range.
        assert align_blocks(old, new) == alignment
        metrics = compute_metrics(old, new, anchor_diff_blocks(old, new))
        assert 0.0 <= metrics.change_rate <= 1.0
        # Identical inputs never produce changes.
        assert anchor_diff_blocks(old, old) == ()
