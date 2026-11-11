"""Regex-based masking of volatile strings."""

from __future__ import annotations

import re
from collections.abc import Iterable

NOISE_PLACEHOLDER = "[NOISE]"


def compile_patterns(patterns: Iterable[str]) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(pattern) for pattern in patterns)


def mask_noise(text: str, patterns: Iterable[str | re.Pattern[str]]) -> str:
    """Apply user patterns in order and replace matches with a stable token.

    The function is idempotent: the placeholder contains no typical volatile
    digits and does not re-match the supplied patterns.
    """
    masked = text
    for pattern in patterns:
        regex = pattern if hasattr(pattern, "sub") else re.compile(pattern)
        masked = regex.sub(NOISE_PLACEHOLDER, masked)
    return masked


def has_noise_pattern(text: str, patterns: Iterable[str]) -> bool:
    return any(re.search(pattern, text) for pattern in patterns)
