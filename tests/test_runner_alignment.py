from __future__ import annotations

import gzip
from dataclasses import replace

from pagewatch.clock import FrozenClock
from pagewatch.diff.alignment import AnchorAlignmentError
from pagewatch.fetch.client import FetchResult
from pagewatch.models import DataPaths, Rules, WatchSpec
from pagewatch.runner import Runner
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import list_audit, list_events
from pagewatch.storage.snapshots import latest_snapshot

HTML_TEMPLATE = """<!doctype html><html><body><main id="content">{items}</main>
</body></html>"""


def html_page(*paragraphs: str) -> str:
    items = "".join(f"<p>{text}</p>" for text in paragraphs)
    return HTML_TEMPLATE.format(items=items)


class FakeClient:
    def __init__(self, pages: list[str]):
        self.pages = list(pages)
        self.calls = 0

    def fetch(self, url, *, etag=None, last_modified=None):
        html = self.pages[min(self.calls, len(self.pages) - 1)]
        self.calls += 1
        return FetchResult(
            status_code=200, url=url, body=html.encode("utf-8"), text=html,
            etag=None, last_modified=None, elapsed_ms=1, not_modified=False,
        )


def make_runner(tmp_path, pages, rules=None):
    data = DataPaths(tmp_path / "data")
    init_paths(data)
    watch = WatchSpec(name="w", url="http://127.0.0.1/page",
                      interval_seconds=60)
    # One client instance across runs: the runner calls the factory every
    # round, but page progression must persist between rounds.
    client = FakeClient(pages)
    runner = Runner(
        db_path=data.db_path, archive_dir=data.archive_dir,
        outbox_path=data.outbox_path, rules=rules or Rules(),
        clock=FrozenClock(1_767_225_600.0),
        client_factory=lambda effective_rules: client,
    )
    return data, watch, runner


def test_anchor_path_rates_translation_below_noise_floor(tmp_path):
    old = html_page("intro", "alpha", "bravo", "charlie", "delta", "echo",
               "footer")
    translated = html_page("delta", "echo", "intro", "alpha", "bravo",
                      "charlie", "footer")
    data, watch, runner = make_runner(tmp_path, [old, translated])
    runner.run(watch)
    result = runner.run(watch)
    assert result.outcome == "changed"
    assert result.severity == "noise"
    with connect(data.db_path) as connection:
        event = list_events(connection)[0]
        assert event.change_rate == 0.0
        assert event.added_lines == 0 and event.removed_lines == 0
        # Unified diff field semantics are unchanged: still a text diff.
        assert event.diff_text.startswith("--- previous\n+++ current\n")


def test_full_rewrite_stays_major_on_change_rate(tmp_path):
    old = html_page(*[f"old paragraph number {n}" for n in range(10)])
    rewritten = html_page(*[f"completely rewritten paragraph {n}" for n in range(10)])
    data, watch, runner = make_runner(tmp_path, [old, rewritten])
    runner.run(watch)
    result = runner.run(watch)
    assert result.severity == "major"
    with connect(data.db_path) as connection:
        event = list_events(connection)[0]
        assert event.change_rate == 1.0


def test_focus_selector_still_forces_major_under_block_metrics(tmp_path):
    rules = Rules(focus_selectors=("#notice",))
    html_old = ('<html><body><div id="notice"><p>notice text</p></div>'
                + "".join(f"<p>stable line {n}</p>" for n in range(20))
                + "</body></html>")
    html_new = ('<html><body><div id="notice"><p>notice text changed</p></div>'
                + "".join(f"<p>stable line {n}</p>" for n in range(20))
                + "</body></html>")
    data, watch, runner = make_runner(tmp_path, [html_old, html_new], rules)
    runner.run(watch)
    result = runner.run(watch)
    assert result.severity == "major"
    with connect(data.db_path) as connection:
        event = list_events(connection)[0]
        assert event.focus_hit is True
        # The rate alone would be minor; focus must override it.
        assert event.change_rate < rules.major_change_rate


def test_alignment_failure_degrades_to_line_diff_with_audit(tmp_path, monkeypatch):
    import pagewatch.runner as runner_module

    def boom(old, new):
        raise AnchorAlignmentError("forced alignment failure")

    monkeypatch.setattr(runner_module, "anchor_diff_blocks", boom)
    old = html_page("alpha", "beta", "gamma", "delta")
    new = html_page("alpha", "BETA", "gamma", "delta")
    data, watch, runner = make_runner(tmp_path, [old, new])
    runner.run(watch)
    result = runner.run(watch)
    # The round still completes and produces a normal change event.
    assert result.outcome == "changed"
    with connect(data.db_path) as connection:
        event = list_events(connection)[0]
        assert event.change_rate == 0.5  # legacy line-window rate: 2 of 4
        audit = [row for row in list_audit(connection)
                 if row["kind"] == "anchor.alignment_fallback"]
        assert len(audit) == 1
        assert "forced alignment failure" in audit[0]["message"]
        assert audit[0]["watch_name"] == "w"


def test_unexpected_anchor_exception_is_wrapped_and_degrades(tmp_path,
                                                             monkeypatch):
    import pagewatch.diff.alignment as alignment_module

    def boom(blocks):
        raise ValueError("unexpected internal state")

    monkeypatch.setattr(alignment_module, "build_anchors", boom)
    old = html_page("a", "b", "c")
    new = html_page("a", "B", "c")
    data, watch, runner = make_runner(tmp_path, [old, new])
    runner.run(watch)
    result = runner.run(watch)
    assert result.outcome == "changed"
    with connect(data.db_path) as connection:
        audit = [row for row in list_audit(connection)
                 if row["kind"] == "anchor.alignment_fallback"]
        assert audit and "unexpected internal state" in audit[0]["message"]


def test_diff_truncation_and_gzip_archive_contracts_are_kept(tmp_path):
    rules = replace(Rules(), diff_max_chars=120)
    old = html_page(*[f"stable paragraph number {n} with some padding text"
                      for n in range(8)])
    rewritten = html_page(*[f"rewritten paragraph number {n} different wording"
                            for n in range(8)])
    data, watch, runner = make_runner(tmp_path, [old, rewritten], rules)
    runner.run(watch)
    result = runner.run(watch)
    assert result.outcome == "changed"
    with connect(data.db_path) as connection:
        event = list_events(connection)[0]
        # diff.max_chars still caps the stored unified diff, same marker.
        assert len(event.diff_text) <= rules.diff_max_chars
        assert event.diff_text.endswith("[diff truncated]")
        snapshot = latest_snapshot(connection, watch.name)
        assert snapshot.raw_sha256 is not None
    # The raw response is still kept as a deterministic gzip archive.
    archive = data.archive_dir / f"{snapshot.raw_sha256}.gz"
    assert archive.exists()
    with gzip.open(archive, "rb") as handle:
        assert handle.read() == rewritten.encode("utf-8")
