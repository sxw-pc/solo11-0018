from __future__ import annotations

from pagewatch.models import DataPaths, Rules, WatchSpec
from pagewatch.runner import Runner
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import list_events


def listing_watch(base_url: str) -> WatchSpec:
    return WatchSpec(
        name="listings", url=f"{base_url}/listings", interval_seconds=60,
        include_selectors=("#listing-body",), noise_patterns=(), keywords=(),
    )


def run_round(runner, watch, scenario, step, clock):
    scenario.set_step(step)
    if step > 0:
        clock.advance(60)
    return runner.run(watch)


def test_listing_scenario_insert_translate_delete_rewrite(tmp_path, mock_server,
                                                          frozen_clock):
    base_url, scenario = mock_server
    data = DataPaths(tmp_path / "data")
    init_paths(data)
    runner = Runner(
        db_path=data.db_path, archive_dir=data.archive_dir,
        outbox_path=data.outbox_path, rules=Rules(), clock=frozen_clock,
    )
    watch = listing_watch(base_url)

    run_round(runner, watch, scenario, 0, frozen_clock)
    run_round(runner, watch, scenario, 1, frozen_clock)
    run_round(runner, watch, scenario, 2, frozen_clock)
    run_round(runner, watch, scenario, 3, frozen_clock)
    run_round(runner, watch, scenario, 4, frozen_clock)

    with connect(data.db_path) as connection:
        events = list_events(connection)
    severities = [event.severity for event in events]
    # 0->1 top insertion: below the major threshold, not a block replacement.
    assert events[0].change_rate < Rules().major_change_rate
    assert severities[0] in {"noise", "minor"}
    # 1->2 pure section translation: the anchor layer reports zero changes.
    assert events[1].change_rate == 0.0
    assert severities[1] == "noise"
    assert events[1].added_lines == 0 and events[1].removed_lines == 0
    # 2->3 single-block deletion: stays well below the major threshold.
    assert events[2].change_rate < Rules().major_change_rate
    assert severities[2] in {"noise", "minor"}
    # 3->4 whole-page rewrite: every block replaced, rate hits the top.
    assert events[3].change_rate == 1.0
    assert severities[3] == "major"


def test_listing_scenario_legacy_line_diff_would_overrate_translation(
        mock_server):
    """The failure mode being fixed: line windows overrate pure translation."""
    from flask import render_template

    from mocksite.app import create_app
    from mocksite.scenarios import context_for
    from pagewatch.diff.alignment import anchor_diff_blocks
    from pagewatch.diff.blockdiff import diff_blocks
    from pagewatch.diff.metrics import compute_legacy_metrics, compute_metrics
    from pagewatch.normalize.pipeline import normalize_html

    _, scenario = mock_server
    flask_app = create_app(scenario)
    normalized = {}
    for step in (1, 2):
        with flask_app.test_request_context():
            html = render_template(
                "listings.html", step=step,
                **context_for("listings", step))
        normalized[step] = normalize_html(
            html, include_selectors=("#listing-body",))
    old, new = normalized[1].blocks, normalized[2].blocks
    legacy = compute_legacy_metrics(old, new, diff_blocks(old, new))
    anchored = compute_metrics(old, new, anchor_diff_blocks(old, new))
    assert legacy.change_rate >= Rules().major_change_rate
    assert anchored.change_rate == 0.0
