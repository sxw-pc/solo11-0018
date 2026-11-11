from __future__ import annotations

import re

from pagewatch.normalize.noise import (
    NOISE_PLACEHOLDER, compile_patterns, has_noise_pattern, mask_noise,
)


def test_masks_each_pattern_and_is_idempotent():
    text = "updated 2026-01-01, counter 42, session abc123"
    patterns = [r"\d{4}-\d{2}-\d{2}", r"counter \d+", r"session [a-z0-9]+"]
    once = mask_noise(text, patterns)
    twice = mask_noise(once, patterns)
    assert once == twice
    assert once.count(NOISE_PLACEHOLDER) == 3
    assert "42" not in once
    assert "abc123" not in once


def test_compiled_patterns_and_non_matching_text():
    compiled = compile_patterns([r"\b\d+\b"])
    assert mask_noise("value 123", compiled) == "value [NOISE]"
    assert mask_noise("nothing volatile", compiled) == "nothing volatile"
    assert has_noise_pattern("value 123", [r"\d+"]) is True
    assert has_noise_pattern("value", [r"\d+"]) is False


def test_placeholder_does_not_get_remasked_by_typical_patterns():
    pattern = re.compile(r"\d+")
    first = mask_noise("id=777", [pattern])
    assert mask_noise(first, [pattern]) == first
