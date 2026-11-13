"""Acceptance tests for cross-watch change-propagation attribution."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
import yaml
from werkzeug.serving import make_server

import pagewatch.cli as cli
from pagewatch.models import DataPaths, NormalizedBlock
from pagewatch.normalize.pipeline import blocks_to_json, content_hash
from pagewatch.propagate import run_attribution
from pagewatch.propagate.engine import attribute
from pagewatch.propagate.fingerprint import build_fingerprint
from pagewatch.propagate.model import EventFingerprint
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import insert_event
from pagewatch.storage.snapshots import append_snapshot, snapshot_by_id

ROOT = Path(__file__).parents[1]
SHARED_NOTICE = ("【重要】计费规则将于本月底调整，请各业务线管理员提前核对配额、"
                 "账单与发票信息。")


# ---------------------------------------------------------------------------
# local two-page site whose pages change in sync (used via the real CLI)
# ---------------------------------------------------------------------------

def create_sync_app(state):
    from flask import Flask

    app = Flask("propagate-sync")

    @app.get("/portal")
    def portal():
        extra = f"<p>{SHARED_NOTICE}</p>" if state["step"] >= 1 else ""
        return ("<html><body><main><h1>平台公告</h1>"
                "<p>本周系统运行正常，没有计划内的维护窗口。</p>"
                f"{extra}</main></body></html>")

    @app.get("/portal/terms")
    def terms():
        extra = f"<p>{SHARED_NOTICE}</p>" if state["step"] >= 1 else ""
        return ("<html><body><main><h1>服务条款</h1>"
                "<p>本条款仅用于本地演示环境，不构成真实合同。</p>"
                f"{extra}</main></body></html>")

    return app


@pytest.fixture
def sync_server():
    state = {"step": 0}
    server = make_server("127.0.0.1", 0, create_sync_app(state), threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        thread.join(timeout=5)


def write_sync_watchlist(path: Path, base_url: str, *,
                         watches: tuple[str, ...] = ("z-portal", "a-terms")
                         ) -> Path:
    specs = {
        "z-portal": {"name": "z-portal", "url": f"{base_url}/portal",
                     "interval_seconds": 60, "tags": ["notice"]},
        "a-terms": {"name": "a-terms", "url": f"{base_url}/portal/terms",
                    "interval_seconds": 60, "tags": ["terms"]},
    }
    payload = {"version": 1,
               "watches": [specs[name] for name in watches]}
    path.write_text(yaml.safe_dump(payload, allow_unicode=True),
                    encoding="utf-8")
    return path


def run_once(config: Path, rules: Path, data_dir: Path) -> int:
    return cli.main(["run-once", "--config", str(config), "--rules",
                     str(rules), "--data-dir", str(data_dir)])


# ---------------------------------------------------------------------------
# direct-ledger helpers (no fetching involved)
# ---------------------------------------------------------------------------

def make_paths(tmp_path: Path) -> DataPaths:
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    return paths


def add_snapshot(connection, watch: str, url: str, fetched_at: float,
                 texts: tuple[str, ...], *, noise_indexes: tuple[int, ...] = ()):
    blocks = tuple(
        NormalizedBlock(index=index, text=text, tag="p", region="#main",
                        is_noise=index in noise_indexes)
        for index, text in enumerate(texts))
    return append_snapshot(
        connection, watch_name=watch, fetched_at=fetched_at, status_code=200,
        url=url, etag=None, last_modified=None,
        content_sha256=content_hash("\n".join(texts)), raw_sha256=None,
        normalized_text="\n".join(texts), blocks_json=blocks_to_json(blocks),
        normalizer_version="test")


def add_event(connection, watch: str, from_snap, to_snap, created_at: float):
    return insert_event(
        connection, watch_name=watch, from_snapshot_id=from_snap.id,
        to_snapshot_id=to_snap.id, created_at=created_at, severity="minor",
        change_rate=0.5, added_lines=1, removed_lines=0, regions=("#main",),
        keyword_hits=(), focus_hit=False, summary="test event", diff_text="")


def fp(event_id: int, watch: str, at: float, *, added=(), removed=(),
       url=None) -> EventFingerprint:
    return EventFingerprint(
        event_id=event_id, watch_name=watch, created_at=at, url=url,
        added={content_hash(text): text for text in added},
        removed={content_hash(text): text for text in removed})


# ---------------------------------------------------------------------------
# 1. real offline batch: two watches change in sync during one round
# ---------------------------------------------------------------------------

def test_synchronized_round_is_attributed_with_direction_and_confidence(
        tmp_path, sync_server, frozen_clock, monkeypatch):
    base_url, state = sync_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_sync_watchlist(tmp_path / "watchlist.yaml", base_url)
    rules_path = ROOT / "configs/rules.example.yaml"
    data_dir = tmp_path / "data"

    assert run_once(config, rules_path, data_dir) == 0
    state["step"] = 1
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0

    with connect(data_dir / "pagewatch.sqlite") as connection:
        result = run_attribution(connection)
    assert result.degraded is False
    assert result.event_count == 2
    assert result.watch_count == 2
    assert len(result.clusters) == 1
    cluster = result.clusters[0]
    assert cluster.root_watch == "z-portal"
    assert cluster.watches == ("a-terms", "z-portal")
    assert cluster.confidence == 1.0
    (link,) = result.links
    assert link.kind == "added"
    # /portal is a proper path prefix of /portal/terms, so the shallower
    # page is the source even though both events share one timestamp.
    assert link.source_watch == "z-portal"
    assert link.downstream_watch == "a-terms"
    assert link.direction_basis == ("path_prefix",)
    assert link.direction_confidence == 0.25
    assert link.confidence == 1.0
    assert link.time_gap_seconds == 0.0
    assert link.shared_count == 1
    assert SHARED_NOTICE[:20] in link.shared_samples[0]
    assert result.confidence == 1.0
    assert result.unattributed == ()


def test_cli_attribute_and_digest_export(tmp_path, sync_server, frozen_clock,
                                         monkeypatch):
    base_url, state = sync_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_sync_watchlist(tmp_path / "watchlist.yaml", base_url)
    rules_path = ROOT / "configs/rules.example.yaml"
    data_dir = tmp_path / "data"
    assert run_once(config, rules_path, data_dir) == 0
    state["step"] = 1
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0

    first = tmp_path / "attr1.json"
    second = tmp_path / "attr2.json"
    html_out = tmp_path / "attr.html"
    assert cli.main(["attribute", "--data-dir", str(data_dir),
                     "--output", str(first), "--html", str(html_out)]) == 0
    assert cli.main(["attribute", "--data-dir", str(data_dir),
                     "--output", str(second)]) == 0
    # identical data -> byte-identical standalone export
    assert first.read_bytes() == second.read_bytes()
    payload = json.loads(first.read_text(encoding="utf-8"))
    assert payload["degraded"] is False
    assert payload["clusters"][0]["root_watch"] == "z-portal"
    assert payload["links"][0]["kind"] == "added"
    assert payload["confidence"] == 1.0
    assert "Change propagation attribution" in html_out.read_text(
        encoding="utf-8")

    digest1 = tmp_path / "digest1.html"
    digest2 = tmp_path / "digest2.html"
    digest_attr = tmp_path / "digest-attr.json"
    args = ["digest", "--data-dir", str(data_dir), "--with-attribution",
            "--attribution-out", str(digest_attr),
            "--generated-at", "2026-01-01T00:05:00Z"]
    assert cli.main(args + ["--output", str(digest1)]) == 0
    assert cli.main(args + ["--output", str(digest2)]) == 0
    assert digest1.read_bytes() == digest2.read_bytes()
    html = digest1.read_text(encoding="utf-8")
    assert "Change propagation attribution" in html
    assert "cluster-1" in html
    assert "root: z-portal" in html
    embedded = json.loads(digest_attr.read_text(encoding="utf-8"))
    assert embedded["clusters"][0]["root_watch"] == "z-portal"
    assert embedded["generated_at"] == "2026-01-01T00:05:00Z"


def test_cli_attribute_single_watch_degrades_without_error(
        tmp_path, sync_server, frozen_clock, monkeypatch):
    base_url, state = sync_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_sync_watchlist(tmp_path / "watchlist.yaml", base_url,
                                  watches=("z-portal",))
    rules_path = ROOT / "configs/rules.example.yaml"
    data_dir = tmp_path / "data"
    assert run_once(config, rules_path, data_dir) == 0
    state["step"] = 1
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0

    out = tmp_path / "attr.json"
    assert cli.main(["attribute", "--data-dir", str(data_dir),
                     "--output", str(out)]) == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["degraded"] is True
    assert payload["degrade_reasons"] == ["single_watch_scope"]
    assert payload["links"] == [] and payload["clusters"] == []
    assert payload["confidence"] is None
    assert [item["reason"] for item in payload["unattributed"]
            ] == ["single_watch_scope"]


def test_cli_attribute_rejects_invalid_window(tmp_path):
    paths = make_paths(tmp_path)
    assert cli.main(["attribute", "--data-dir", str(paths.root),
                     "--window-seconds", "0"]) == 2
    assert cli.main(["attribute", "--data-dir", str(paths.root),
                     "--window-seconds", "180",
                     "--max-span-seconds", "60"]) == 2


# ---------------------------------------------------------------------------
# 2. direction comes from event timestamps, never from insertion order
# ---------------------------------------------------------------------------

def test_direction_follows_event_time_not_insertion_order(tmp_path):
    paths = make_paths(tmp_path)
    shared = "同一段需要跨页面同步的说明文字。"
    with connect(paths.db_path) as connection:
        a1 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 40.0,
                          ("阿尔法页面基线内容保持不变。",))
        a2 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 200.0,
                          ("阿尔法页面基线内容保持不变。", shared))
        b1 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 30.0,
                          ("贝塔页面基线内容保持不变。",))
        b2 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 100.0,
                          ("贝塔页面基线内容保持不变。", shared))
        # the LATER event is written first: ledger order now disagrees
        # with the order the changes actually happened in
        event_alpha = add_event(connection, "alpha", a1, a2, 200.0)
        event_beta = add_event(connection, "beta", b1, b2, 100.0)
        connection.commit()
        assert event_alpha.id < event_beta.id
        result = run_attribution(connection)
    assert result.degraded is False
    (link,) = result.links
    assert link.source_watch == "beta"
    assert link.downstream_watch == "alpha"
    assert link.source_event_id == event_beta.id
    assert link.direction_basis == ("timestamp",)
    assert link.time_gap_seconds == 100.0
    assert link.direction_confidence == pytest.approx(
        0.6 + 0.4 * (100.0 / 180.0))
    assert link.confidence == pytest.approx(1.0 - 0.5 * (100.0 / 180.0))
    assert result.clusters[0].root_watch == "beta"


def test_engine_is_deterministic_regardless_of_input_order():
    shared1 = "第一段跨页面同步的说明文字。"
    shared2 = "第二段跨页面同步的说明文字。"
    fps = [
        fp(1, "wa", 100.0, added=[shared1], url="http://127.0.0.1/a"),
        fp(2, "wb", 130.0, added=[shared1], url="http://127.0.0.1/b"),
        fp(3, "wc", 160.0, added=[shared1, shared2],
           url="http://127.0.0.1/c"),
        fp(4, "wd", 5000.0, added=[shared2], url="http://127.0.0.1/d"),
    ]
    first = attribute(fps, window_seconds=180.0, max_span_seconds=600.0)
    second = attribute(list(reversed(fps)), window_seconds=180.0,
                       max_span_seconds=600.0)
    assert first == second


def test_repeat_runs_on_same_data_are_byte_identical(tmp_path):
    paths = make_paths(tmp_path)
    shared = "重复归因时保持稳定的同一段说明文字。"
    with connect(paths.db_path) as connection:
        a1 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 10.0,
                          ("阿尔法页面基线内容保持不变。",))
        a2 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 50.0,
                          ("阿尔法页面基线内容保持不变。", shared))
        b1 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 20.0,
                          ("贝塔页面基线内容保持不变。",))
        b2 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 60.0,
                          ("贝塔页面基线内容保持不变。", shared))
        add_event(connection, "alpha", a1, a2, 50.0)
        add_event(connection, "beta", b1, b2, 60.0)
        connection.commit()
        from pagewatch.propagate.report import render_attribution_json
        first = render_attribution_json(run_attribution(connection))
        second = render_attribution_json(run_attribution(connection))
    assert first == second


# ---------------------------------------------------------------------------
# 3. degradation: thin or damaged history never raises
# ---------------------------------------------------------------------------

def test_single_watch_history_degrades_cleanly(tmp_path):
    paths = make_paths(tmp_path)
    with connect(paths.db_path) as connection:
        s1 = add_snapshot(connection, "solo", "http://127.0.0.1/solo", 10.0,
                          ("单页基线内容保持不变。",))
        s2 = add_snapshot(connection, "solo", "http://127.0.0.1/solo", 20.0,
                          ("单页基线内容保持不变。", "单页新增的一段说明文字。"))
        event = add_event(connection, "solo", s1, s2, 20.0)
        connection.commit()
        result = run_attribution(connection)
    assert result.degraded is True
    assert result.degrade_reasons == ("single_watch_scope",)
    assert result.links == () and result.clusters == ()
    assert result.confidence is None
    assert [(item.event_id, item.reason) for item in result.unattributed
            ] == [(event.id, "single_watch_scope")]


def test_empty_history_degrades_cleanly(tmp_path):
    paths = make_paths(tmp_path)
    with connect(paths.db_path) as connection:
        result = run_attribution(connection)
    assert result.degraded is True
    assert result.degrade_reasons == ("no_events",)
    assert result.confidence is None
    assert result.unattributed == ()


def test_missing_snapshots_degrade_the_event_not_the_run(tmp_path):
    paths = make_paths(tmp_path)
    shared = "跨页面同步出现的同一段说明文字。"
    with connect(paths.db_path) as connection:
        a1 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 40.0,
                          ("阿尔法页面基线内容保持不变。",))
        a2 = add_snapshot(connection, "alpha", "http://127.0.0.1/alpha", 90.0,
                          ("阿尔法页面基线内容保持不变。", shared))
        b1 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 30.0,
                          ("贝塔页面基线内容保持不变。",))
        b2 = add_snapshot(connection, "beta", "http://127.0.0.1/beta", 80.0,
                          ("贝塔页面基线内容保持不变。", shared))
        add_event(connection, "alpha", a1, a2, 90.0)
        add_event(connection, "beta", b1, b2, 80.0)
        # retention pruned this event's snapshots: ids dangle
        insert_event(connection, watch_name="gamma", from_snapshot_id=9001,
                     to_snapshot_id=9002, created_at=85.0, severity="minor",
                     change_rate=0.5, added_lines=1, removed_lines=0,
                     regions=("#main",), keyword_hits=(), focus_hit=False,
                     summary="pruned", diff_text="")
        connection.commit()
        result = run_attribution(connection)
    assert result.degraded is True
    assert result.degrade_reasons == ("missing_snapshots",)
    assert len(result.links) == 1  # the healthy pair still correlates
    assert result.clusters[0].root_watch == "beta"
    gamma = [item for item in result.unattributed
             if item.watch_name == "gamma"]
    assert [item.reason for item in gamma] == ["missing_snapshot"]


# ---------------------------------------------------------------------------
# 4. window boundary, span cap, link kinds, fingerprint eligibility
# ---------------------------------------------------------------------------

def test_window_boundary_is_inclusive():
    shared = "窗口边界上同步出现的同一段说明文字。"
    earlier = fp(1, "wa", 1000.0, added=[shared])
    at_edge = fp(2, "wb", 1180.0, added=[shared])
    past_edge = fp(3, "wb", 1181.0, added=[shared])
    links, clusters, _ = attribute([earlier, at_edge], window_seconds=180.0,
                                   max_span_seconds=600.0)
    assert len(links) == 1 and len(clusters) == 1
    links, clusters, _ = attribute([earlier, past_edge],
                                   window_seconds=180.0,
                                   max_span_seconds=600.0)
    assert links == () and clusters == ()


def test_cluster_span_cap_splits_chains(tmp_path):
    paths = make_paths(tmp_path)
    shared = "链式传播中反复出现的同一段说明文字。"
    with connect(paths.db_path) as connection:
        for watch, base_at, change_at in (("wa", 10.0, 100.0),
                                          ("wb", 20.0, 250.0),
                                          ("wc", 30.0, 400.0)):
            url = f"http://127.0.0.1/{watch}"
            s1 = add_snapshot(connection, watch, url, base_at,
                              (f"{watch} 页面基线内容保持不变。",))
            s2 = add_snapshot(connection, watch, url, change_at,
                              (f"{watch} 页面基线内容保持不变。", shared))
            add_event(connection, watch, s1, s2, change_at)
        connection.commit()
        result = run_attribution(connection, window_seconds=180.0,
                                 max_span_seconds=180.0)
    # wa-wb and wb-wc are pairwise in-window, but the chain spans 300s;
    # the cap cuts it at the earliest largest gap, isolating wa.
    assert len(result.clusters) == 1
    assert result.clusters[0].watches == ("wb", "wc")
    reasons = {item.watch_name: item.reason for item in result.unattributed}
    assert reasons == {"wa": "beyond_max_span"}


def test_removed_and_moved_link_kinds():
    shared = "将被统一下线或迁移的一段说明文字。"
    first = fp(1, "wa", 100.0, removed=[shared], url="http://127.0.0.1/a")
    second = fp(2, "wb", 120.0, removed=[shared], url="http://127.0.0.1/b")
    links, _, _ = attribute([first, second], window_seconds=180.0,
                            max_span_seconds=600.0)
    assert [link.kind for link in links] == ["removed"]

    first = fp(1, "wa", 100.0, removed=[shared], url="http://127.0.0.1/a")
    second = fp(2, "wb", 120.0, added=[shared], url="http://127.0.0.1/b")
    links, _, _ = attribute([first, second], window_seconds=180.0,
                            max_span_seconds=600.0)
    assert [link.kind for link in links] == ["moved"]
    assert links[0].source_watch == "wa"
    assert links[0].direction_basis == ("timestamp",)


def test_noise_and_short_blocks_are_not_fingerprinted(tmp_path):
    paths = make_paths(tmp_path)
    noise_text = "当前访问计数：[NOISE]"
    short_text = "下一页"
    base_a = "阿尔法页面自己的基线说明文字。"
    base_b = "贝塔页面自己的基线说明文字。"
    new_a = "阿尔法页面新增的独立说明文字。"
    new_b = "贝塔页面新增的独立说明文字。"
    with connect(paths.db_path) as connection:
        a1 = add_snapshot(connection, "wa", "http://127.0.0.1/a", 10.0,
                          (base_a,))
        a2 = add_snapshot(connection, "wa", "http://127.0.0.1/a", 20.0,
                          (base_a, noise_text, short_text, new_a),
                          noise_indexes=(1,))
        b1 = add_snapshot(connection, "wb", "http://127.0.0.1/b", 10.0,
                          (base_b,))
        b2 = add_snapshot(connection, "wb", "http://127.0.0.1/b", 20.0,
                          (base_b, noise_text, short_text, new_b),
                          noise_indexes=(1,))
        event_a = add_event(connection, "wa", a1, a2, 20.0)
        event_b = add_event(connection, "wb", b1, b2, 20.0)
        connection.commit()
        fp_a = build_fingerprint(event_a, snapshot_by_id(connection, a1.id),
                                 snapshot_by_id(connection, a2.id),
                                 min_block_chars=8)
        assert fp_a is not None
        # the identical noise block and the identical short block must not
        # become fingerprints; only the eligible normal block remains
        assert list(fp_a.added.values()) == [new_a]
        result = run_attribution(connection)
    assert result.links == () and result.clusters == ()
    assert {item.reason for item in result.unattributed
            } == {"no_shared_content"}
