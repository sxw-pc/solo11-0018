from __future__ import annotations

import difflib

from pagewatch.diff.blockdiff import diff_blocks
from pagewatch.diff.metrics import compute_metrics
from pagewatch.diff.textdiff import (
    changed_line_counts, truncate_text, unified_diff,
)
from pagewatch.models import NormalizedBlock


def blocks(*texts, focus=False, noise=False, region="#main"):
    return tuple(
        NormalizedBlock(i, text, "p", region, focus, noise)
        for i, text in enumerate(texts)
    )


def test_line_diff_counts_and_truncation():
    old = "a\nb\nc"
    new = "a\nB\nc\nd"
    added, removed = changed_line_counts(old, new)
    assert (added, removed) == (2, 1)
    diff = unified_diff(old, new)
    assert "--- previous" in diff and "+++ current" in diff
    assert "-b" in diff and "+B" in diff and "+d" in diff
    truncated = unified_diff(old * 30, new * 30, max_chars=80)
    assert len(truncated) <= 80
    assert truncated.endswith("[diff truncated]")
    assert truncate_text("short", 100) == "short"


def test_block_alignment_groups_adjacent_replacements():
    old = blocks("alpha", "beta", "gamma", "delta")
    new = blocks("alpha", "BETA", "GAMMA", "delta")
    changes = diff_blocks(old, new)
    assert len(changes) == 1
    change = changes[0]
    assert change.op == "replace"
    assert change.old_range == (1, 3)
    assert change.new_range == (1, 3)
    assert change.old_texts == ("beta", "gamma")
    assert change.new_texts == ("BETA", "GAMMA")
    assert change.regions == ("#main",)


def test_insert_delete_and_focus_flags():
    old = blocks("one", "two", focus=True)
    new = blocks("one", "new", "two", focus=True)
    changes = diff_blocks(old, new)
    assert [change.op for change in changes] == ["insert"]
    assert changes[0].focus_hit is True
    assert changes[0].new_texts == ("new",)
    deleted = diff_blocks(new, old)
    assert [change.op for change in deleted] == ["delete"]


def test_adjacent_and_gapped_synthetic_opcodes_are_clustered(monkeypatch):
    old = blocks("a", "b", "c")
    new = blocks("A", "B", "C")
    monkeypatch.setattr(difflib.SequenceMatcher, "get_opcodes", lambda self: [
        ("replace", 0, 1, 0, 1), ("replace", 1, 2, 1, 2)])
    merged = diff_blocks(old, new)
    assert len(merged) == 1
    assert merged[0].old_range == (0, 2)
    assert merged[0].new_texts == ("A", "B")
    monkeypatch.setattr(difflib.SequenceMatcher, "get_opcodes", lambda self: [
        ("replace", 0, 1, 0, 1), ("insert", 2, 2, 1, 1)])
    gapped = diff_blocks(old, new)
    assert len(gapped) == 2
    assert gapped[1].op == "insert"


def test_metrics_regions_keywords_and_rate():
    old = blocks("alpha", "beta", "gamma", "delta")
    new = blocks("alpha", "critical update", "gamma", "delta", focus=True)
    changes = diff_blocks(old, new)
    metrics = compute_metrics(old, new, changes, ["critical", "absent"])
    assert metrics.base_lines == 4
    assert metrics.added_lines == 1
    assert metrics.removed_lines == 1
    assert metrics.changed_lines == 2
    assert metrics.change_rate == 0.5
    assert metrics.focus_hit is True
    assert metrics.keyword_hits == ("critical",)
    assert metrics.regions == ("#main",)
    assert metrics.noise_only is False


def test_metrics_noise_only_and_insert_only():
    old = blocks("stable", noise=True)
    new = blocks("changed", noise=True)
    changes = diff_blocks(old, new)
    metrics = compute_metrics(old, new, changes)
    assert metrics.noise_only is True
    empty = blocks()
    inserted = blocks("first line")
    changes = diff_blocks(empty, inserted)
    metrics = compute_metrics(empty, inserted, changes)
    assert metrics.added_lines == 1
    assert metrics.removed_lines == 0
    assert metrics.change_rate == 1.0
