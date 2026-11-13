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


def test_include_scope_never_widens_to_the_whole_document():
    """A selected region may be a bare element; extraction must stay there.

    Regression: when the selected roots produced no block-level descendants,
    extraction used to fall back to ``soup.body`` and swallow every other
    region, so an excluded page footer leaked back into the content hash.
    """
    html = """<html><head><title>Example</title></head><body>
<div id="title">Example</div>
<main><p>stable body</p></main>
<footer class="muted">Copyright 2025 Example</footer></body></html>"""
    config = dict(include_selectors=["#title"],
                  exclude_selectors=[".ad"],
                  noise_patterns=[r"更新时间[：:]\s*[0-9:\- ]+"])
    baseline = normalize_html(html, **config)
    after_year_change = normalize_html(html.replace("2025", "2026"), **config)
    assert [block.text for block in baseline.blocks] == ["Example"]
    assert baseline.content_hash == after_year_change.content_hash

    # Removing the exclude selector must not change which region is compared;
    # it only removes markup that never belonged to the scope.
    without_exclude = dict(config)
    without_exclude.pop("exclude_selectors")
    plain = normalize_html(html, **without_exclude)
    changed = normalize_html(html.replace("2025", "2026"), **without_exclude)
    assert [block.text for block in plain.blocks] == ["Example"]
    assert plain.content_hash == changed.content_hash
    assert baseline.content_hash == plain.content_hash


def test_excluding_the_only_selected_root_shrinks_scope_instead_of_widening():
    """If excludes remove every selected root, the scope is empty.

    This is the shape that made a footer year appear only when an exclude
    selector was configured: the removed root left an empty selection, which
    must not fall back to whole-body extraction.
    """
    html = """<html><body>
<div class="analytics"><header id="title"><h1>Example</h1></header></div>
<main><p>stable body</p></main>
<footer>Copyright 2025 Example</footer></body></html>"""
    scoped = normalize_html(html, include_selectors=["#title"],
                            exclude_selectors=[".analytics"])
    assert scoped.blocks == ()
    assert scoped.text == ""
    assert scoped.content_hash == normalize_html(
        html.replace("2025", "2026"), include_selectors=["#title"],
        exclude_selectors=[".analytics"]).content_hash


def test_include_scope_that_matches_nothing_yields_no_text():
    page = normalize_html("<html><body><footer>Copyright 2025</footer></body></html>",
                          include_selectors=["#missing"])
    assert page.blocks == ()
    assert page.text == ""


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
