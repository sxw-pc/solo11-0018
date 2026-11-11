"""Line-level unified diff helpers."""

from __future__ import annotations

import difflib


def split_lines(text: str) -> list[str]:
    return text.splitlines()


def changed_line_counts(old_text: str, new_text: str) -> tuple[int, int]:
    old_lines = split_lines(old_text)
    new_lines = split_lines(new_text)
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False)
    added = removed = 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag in {"replace", "delete"}:
            removed += i2 - i1
        if tag in {"replace", "insert"}:
            added += j2 - j1
    return added, removed


def truncate_text(text: str, max_chars: int | None) -> str:
    if max_chars is None or len(text) <= max_chars:
        return text
    marker = "\n... [diff truncated]"
    if max_chars <= len(marker):
        return marker[:max_chars]
    return text[: max_chars - len(marker)] + marker


def unified_diff(old_text: str, new_text: str,
                 max_chars: int | None = None) -> str:
    """Return a deterministic, optionally truncated unified diff."""
    lines = difflib.unified_diff(
        split_lines(old_text),
        split_lines(new_text),
        fromfile="previous",
        tofile="current",
        n=3,
        lineterm="",
    )
    return truncate_text("\n".join(lines), max_chars)
