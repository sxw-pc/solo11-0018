from __future__ import annotations

from pagewatch import NORMALIZER_VERSION
from pagewatch.normalize.pipeline import (
    blocks_from_json, blocks_to_json, normalize_html,
)
from pagewatch.normalize.selectors import normalize_whitespace

NOISE = [r"Visits:\s*\d+"]


def read(fixtures_dir, name):
    return (fixtures_dir / name).read_text(encoding="utf-8")


def normalized(html):
    return normalize_html(
        html, include_selectors=["#content"], exclude_selectors=[".ad"],
        noise_patterns=NOISE, focus_selectors=["#notice"],
    )


def test_scripts_comments_and_excluded_regions_are_removed(fixtures_dir):
    page = normalized(read(fixtures_dir, "baseline.html"))
    assert "should not appear" not in page.text
    assert "template comment" not in page.text
    assert "Buy something" not in page.text
    assert "Release notes" in page.text
    assert page.normalizer_version == NORMALIZER_VERSION


def test_blocks_are_ordered_with_regions_focus_and_noise(fixtures_dir):
    page = normalized(read(fixtures_dir, "baseline.html"))
    texts = [block.text for block in page.blocks]
    assert texts[:3] == ["Release notes", "Notice", "Service is stable."]
    notice_blocks = [block for block in page.blocks if block.region == "#notice"]
    assert notice_blocks and all(block.is_focus for block in notice_blocks)
    counter = next(block for block in page.blocks if block.tag == "p"
                   and block.text == "[NOISE]")
    assert counter.is_noise is True
    assert [block.index for block in page.blocks] == list(range(len(page.blocks)))


def test_noise_masking_makes_counter_only_change_invisible(fixtures_dir):
    changed = normalized(read(fixtures_dir, "changed.html"))
    noisy = normalized(read(fixtures_dir, "noisy.html"))
    assert changed.content_hash == noisy.content_hash
    assert changed.text == noisy.text


def test_real_change_changes_hash_and_text(fixtures_dir):
    baseline = normalized(read(fixtures_dir, "baseline.html"))
    changed = normalized(read(fixtures_dir, "changed.html"))
    assert baseline.content_hash != changed.content_hash
    assert "Documentation paragraph two." in baseline.text
    assert "Documentation paragraph two updated." in changed.text


def test_include_and_exclude_selectors_limit_scope(fixtures_dir):
    page = normalize_html(read(fixtures_dir, "baseline.html"),
                          include_selectors=["#notice"])
    assert [block.text for block in page.blocks] == ["Notice", "Service is stable."]
    page = normalize_html(read(fixtures_dir, "baseline.html"),
                          include_selectors=["#content"],
                          exclude_selectors=["#notice", ".ad"])
    assert "Service is stable." not in page.text
    assert "Release notes" in page.text


def test_whitespace_normalization_and_fallback_body():
    page = normalize_html("<html><body><main> a\n\t b   c </main></body></html>")
    assert page.text == "a b c"
    assert page.blocks[0].tag == "body"
    assert normalize_whitespace("  a \n b ") == "a b"


def test_same_input_is_deterministic_and_blocks_roundtrip(fixtures_dir):
    html = read(fixtures_dir, "baseline.html")
    first, second = normalized(html), normalized(html)
    assert first == second
    restored = blocks_from_json(blocks_to_json(first.blocks))
    assert restored == first.blocks
    assert blocks_from_json("not-json") == ()
