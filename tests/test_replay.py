"""Acceptance tests for the replay command.

Covers: chain order vs. actual execution order, breakpoints for deleted
archives, byte-for-byte reproducibility, legacy-data degradation,
concurrent-replay temp isolation, interrupt cleanup and read-only access.
"""

from __future__ import annotations

import hashlib
import json
import signal
import threading
from contextlib import closing
from pathlib import Path

import pytest
import yaml

import pagewatch.cli as cli
from pagewatch.fetch.archive import write_archive
from pagewatch.models import DataPaths
from pagewatch.replay import (
    STAGE_ORDER,
    ReplayWorkspace,
    build_replay,
    connect_readonly,
    discover_rounds,
    select_rounds,
)
from pagewatch.replay.report import render_report_json
from pagewatch.storage.db import connect, init_paths

ROOT = Path(__file__).parents[1]


def write_watchlist(path: Path, base_url: str) -> Path:
    watches = [
        {"name": "announcements", "url": f"{base_url}/announcements",
         "interval_seconds": 60, "tags": ["notice"],
         "include_selectors": ["#notice-area"],
         "exclude_selectors": [".analytics"],
         "noise_patterns": [], "keywords": ["重要", "计费"]},
        {"name": "docs", "url": f"{base_url}/docs", "interval_seconds": 60,
         "tags": ["docs"], "include_selectors": ["#doc-body"],
         "exclude_selectors": [], "noise_patterns": [], "keywords": []},
        {"name": "pricing", "url": f"{base_url}/pricing", "interval_seconds": 60,
         "tags": ["pricing"], "include_selectors": ["#price-table"],
         "exclude_selectors": [".ad"], "noise_patterns": [], "keywords": []},
        {"name": "noisy-status", "url": f"{base_url}/noisy",
         "interval_seconds": 60, "tags": ["status"],
         "include_selectors": ["main"], "exclude_selectors": [],
         "noise_patterns": [r"当前访问计数[：:]\s*\d+",
                            r"更新时间[：:]\s*[0-9:\- ]+",
                            r"随机会话[：:]\s*[A-Za-z0-9_-]+"],
         "keywords": []},
    ]
    path.write_text(yaml.safe_dump({"version": 1, "watches": watches},
                                   allow_unicode=True), encoding="utf-8")
    return path


def run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch,
                 steps=(0, 1, 3)) -> Path:
    """Run one monitoring round per scenario step; returns the data dir."""
    base_url, scenario = mock_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_watchlist(tmp_path / "watchlist.yaml", base_url)
    rules_path = ROOT / "configs" / "rules.example.yaml"
    data_dir = tmp_path / "data"
    first = True
    for step in steps:
        scenario.set_step(step)
        if not first:
            frozen_clock.advance(60)
        first = False
        assert cli.main(["run-once", "--config", str(config),
                         "--rules", str(rules_path),
                         "--data-dir", str(data_dir)]) == 0
    return data_dir


def build_report(data_dir: Path, temp_base: Path, *, selectors=(), last=None,
                 watch_names=()):
    paths = DataPaths(data_dir)
    with closing(connect_readonly(paths.db_path)) as connection:
        discovered = discover_rounds(connection)
    rounds = select_rounds(discovered, selectors=list(selectors), last=last)
    with ReplayWorkspace(temp_base) as workspace:
        return build_replay(paths.db_path, paths.archive_dir, rounds=rounds,
                            watch_names=watch_names, workspace=workspace)


def chain_by_name(rnd, watch_name):
    return next(chain for chain in rnd.chains
                if chain.watch_name == watch_name)


def stages_by_name(chain):
    return {stage.stage: stage for stage in chain.stages}


def tree_hashes(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(
            path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*")) if path.is_file()
    }


# -- 1. chain order matches actual monitoring execution ---------------------


def test_replay_chain_order_matches_monitoring_execution(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    report = build_report(data_dir, tmp_path / "ws")

    assert report.status == "ok"
    assert [rnd.index for rnd in report.rounds] == [1, 2, 3]
    starts = [rnd.start for rnd in report.rounds]
    assert starts == sorted(starts)
    # every chain follows the pipeline's actual execution order
    for rnd in report.rounds:
        for chain in rnd.chains:
            assert [stage.stage for stage in chain.stages] == list(STAGE_ORDER)

    # round 1 (baseline): first snapshots, no events
    round1 = report.rounds[0]
    assert {chain.watch_name for chain in round1.chains} == {
        "announcements", "docs", "pricing", "noisy-status"}
    for chain in round1.chains:
        stages = stages_by_name(chain)
        assert stages["probe"].status == "ok"
        assert stages["archive"].status == "ok"
        assert stages["archive"].details["verified"] is True
        assert stages["snapshot"].status == "ok"
        assert stages["normalize"].status == "ok"
        assert stages["normalize"].details["source"] == "archived"
        assert stages["normalize"].details["block_hashes"]
        assert stages["diff"].status == "skipped"
        assert stages["classify"].status == "skipped"
        assert stages["alert"].status == "skipped"

    # round 2 (docs paragraph changed): docs -> minor, others 304
    round2 = report.rounds[1]
    docs = stages_by_name(chain_by_name(round2, "docs"))
    assert docs["diff"].status == "ok"
    assert docs["diff"].details["added_lines"] == 1
    assert docs["classify"].details["severity"] == "minor"
    assert docs["alert"].status == "skipped"
    pricing = stages_by_name(chain_by_name(round2, "pricing"))
    assert pricing["probe"].details["not_modified"] is True
    assert pricing["archive"].status == "skipped"
    assert pricing["snapshot"].status == "skipped"
    assert pricing["snapshot"].details["previous_snapshot"]["version"] == 1

    # round 3 (focus notice + keyword): announcements -> major + alert
    round3 = report.rounds[2]
    ann = stages_by_name(chain_by_name(round3, "announcements"))
    assert ann["classify"].details["severity"] == "major"
    assert ann["alert"].status == "ok"
    assert ann["alert"].details["dedup_key"] == "announcements:major"
    noisy = stages_by_name(chain_by_name(round3, "noisy-status"))
    assert noisy["probe"].details["status_code"] == 200
    assert noisy["archive"].status == "ok"  # body archived even when masked
    assert noisy["snapshot"].status == "skipped"  # masked counter: unchanged

    # chain records carry the timestamps of the actual execution pass
    with connect(data_dir / "pagewatch.sqlite") as connection:
        event_row = connection.execute(
            "SELECT * FROM events WHERE watch_name='announcements'").fetchone()
        alert_row = connection.execute(
            "SELECT * FROM alerts WHERE event_id=?",
            (event_row["id"],)).fetchone()
    assert ann["diff"].details["event_id"] == event_row["id"]
    assert ann["diff"].details["change_rate"] == event_row["change_rate"]
    assert ann["alert"].details["alert_id"] == alert_row["id"]
    assert ann["snapshot"].details["fetched_at"] == \
        ann["probe"].details["checked_at"]
    assert ann["alert"].details["created_at"] == \
        ann["probe"].details["checked_at"]


def test_replay_watch_filter_and_round_selection(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    report = build_report(data_dir, tmp_path / "ws", last=1,
                          watch_names=["docs"])
    assert len(report.rounds) == 1
    assert report.rounds[0].index == 3
    assert [chain.watch_name for chain in report.rounds[0].chains] == ["docs"]


# -- 2. deleted archive -> breakpoint, report still generated ----------------


def test_deleted_archive_marks_breakpoint_without_failing(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    with connect(data_dir / "pagewatch.sqlite") as connection:
        row = connection.execute(
            "SELECT raw_sha256 FROM snapshots WHERE raw_sha256 IS NOT NULL "
            "ORDER BY id LIMIT 1").fetchone()
    digest = row["raw_sha256"]
    (data_dir / "archive" / f"{digest}.gz").unlink()

    report = build_report(data_dir, tmp_path / "ws")
    assert report.status == "broken"
    broken = [chain for rnd in report.rounds for chain in rnd.chains
              if chain.status == "broken"]
    assert len(broken) == 1
    stages = stages_by_name(broken[0])
    assert stages["archive"].status == "missing"
    assert f"archive/{digest}.gz" in stages["archive"].reason
    assert broken[0].breakpoints == (
        {"stage": "archive", "reason": stages["archive"].reason},)
    # the rest of the chain is still reconstructed from the ledger
    assert stages["snapshot"].status == "ok"
    assert stages["normalize"].status == "ok"
    assert any(chain.status == "ok" for rnd in report.rounds
               for chain in rnd.chains)

    # the command itself does not fail and writes both formats
    html_out = tmp_path / "replay.html"
    json_out = tmp_path / "replay.json"
    assert cli.main(["replay", "--data-dir", str(data_dir),
                     "--output", str(html_out),
                     "--json-out", str(json_out)]) == 0
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["status"] == "broken"
    assert "archive file missing" in html_out.read_text(encoding="utf-8")


def test_missing_snapshot_reference_is_a_breakpoint(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    with connect(paths.db_path) as connection:
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified) VALUES('ghost', 100.0, 200, 0)")
        connection.execute(
            "INSERT INTO events(watch_name, from_snapshot_id, to_snapshot_id, "
            "created_at, severity, change_rate, added_lines, removed_lines, "
            "regions_json, keyword_hits_json, focus_hit, summary, diff_text) "
            "VALUES('ghost', 41, 42, 100.0, 'minor', 0.1, 1, 0, '[]', '[]', "
            "0, 'minor: +1/-0', '')")
        connection.commit()
    report = build_report(tmp_path / "data", tmp_path / "ws")
    chain = report.rounds[0].chains[0]
    stages = stages_by_name(chain)
    assert stages["snapshot"].status == "missing"
    assert "no snapshot exists" in stages["snapshot"].reason
    assert stages["diff"].status == "invalid"
    assert "missing to-snapshot #42" in stages["diff"].reason
    assert chain.status == "broken"
    assert {entry["stage"] for entry in chain.breakpoints} == {
        "snapshot", "diff"}


# -- 3. byte-for-byte reproducibility ----------------------------------------


def test_replay_is_byte_reproducible(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    base_args = ["replay", "--data-dir", str(data_dir)]
    first_html, first_json = tmp_path / "a.html", tmp_path / "a.json"
    second_html, second_json = tmp_path / "b.html", tmp_path / "b.json"
    assert cli.main(base_args + ["--output", str(first_html),
                                 "--json-out", str(first_json)]) == 0
    assert cli.main(base_args + ["--output", str(second_html),
                                 "--json-out", str(second_json)]) == 0
    assert first_html.read_bytes() == second_html.read_bytes()
    assert first_json.read_bytes() == second_json.read_bytes()
    # generated_at is derived from the data, not the wall clock
    payload = json.loads(first_json.read_text(encoding="utf-8"))
    assert payload["generated_at"] == "2026-01-01T00:02:00Z"


# -- 4. legacy data degrades with annotations --------------------------------


def _seed_legacy_round(paths: DataPaths, *, blocks_json: str,
                       watch_name: str = "legacy") -> None:
    text = "old line one\nold line two"
    content_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    _, raw_digest, _ = write_archive(b"<html>legacy body</html>",
                                     paths.archive_dir)
    with connect(paths.db_path) as connection:
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified, elapsed_ms, raw_sha256) "
            "VALUES(?, 1000.0, 200, 0, 12, ?)", (watch_name, raw_digest))
        connection.execute(
            "INSERT INTO snapshots(watch_name, version, fetched_at, "
            "status_code, url, content_sha256, raw_sha256, normalized_text, "
            "blocks_json, normalizer_version) "
            "VALUES(?, 1, 1000.0, 200, 'http://127.0.0.1/legacy', ?, ?, ?, "
            "?, 'normalizer-v1')",
            (watch_name, content_digest, raw_digest, text, blocks_json))
        connection.commit()


def test_legacy_blocks_without_hashes_degrade_with_annotation(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    legacy_blocks = json.dumps([
        {"index": 0, "text": "old line one", "tag": "p", "region": "main",
         "is_focus": False, "is_noise": False},
        {"index": 1, "text": "old line two", "tag": "p", "region": "main",
         "is_focus": False, "is_noise": False},
    ], ensure_ascii=False)
    _seed_legacy_round(paths, blocks_json=legacy_blocks)

    report = build_report(tmp_path / "data", tmp_path / "ws")
    assert report.status == "degraded"
    chain = report.rounds[0].chains[0]
    assert chain.status == "degraded"
    stages = stages_by_name(chain)
    normalize = stages["normalize"]
    assert normalize.status == "degraded"
    assert "inferred" in normalize.reason
    assert "legacy" in normalize.reason
    assert normalize.details["source"] == "inferred"
    # inferred hashes still identify the blocks
    assert [entry["index"] for entry in normalize.details["block_hashes"]] == [0, 1]
    assert all(entry["hash"] for entry in normalize.details["block_hashes"])
    assert chain.degradations == (
        {"stage": "normalize", "reason": normalize.reason},)
    # everything else still reconstructs normally
    assert stages["probe"].status == "ok"
    assert stages["archive"].status == "ok"
    assert stages["snapshot"].status == "ok"

    payload = json.loads(render_report_json(report))
    degradations = payload["rounds"][0]["watches"][0]["degradations"]
    assert degradations and "inferred" in degradations[0]["reason"]


def test_malformed_legacy_blocks_become_breakpoint_not_error(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    _seed_legacy_round(paths, blocks_json="this-is-not-json",
                       watch_name="corrupt")
    report = build_report(tmp_path / "data", tmp_path / "ws")
    chain = report.rounds[0].chains[0]
    stages = stages_by_name(chain)
    assert stages["normalize"].status == "invalid"
    assert "not valid JSON" in stages["normalize"].reason
    assert chain.status == "broken"
    # the report is still produced for the remaining stages
    assert stages["probe"].status == "ok"
    assert stages["archive"].status == "ok"


# -- 5. concurrent replays share no temp files -------------------------------


def test_concurrent_replays_do_not_pollute_each_other(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    temp_base = tmp_path / "tmp"
    barrier = threading.Barrier(2)
    original_extract = ReplayWorkspace.extract_archive

    def synced_extract(self, archive_path, digest):
        # force both replays to be inside extraction at the same time
        barrier.wait(timeout=10)
        return original_extract(self, archive_path, digest)

    monkeypatch.setattr(ReplayWorkspace, "extract_archive", synced_extract)
    results: dict[str, str] = {}
    errors: list[BaseException] = []

    def run(tag: str) -> None:
        try:
            results[tag] = render_report_json(
                build_report(data_dir, temp_base))
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(tag,))
               for tag in ("first", "second")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not errors
    assert results["first"] == results["second"]
    # every workspace cleaned up its own private directory
    assert list(temp_base.iterdir()) == []


# -- 6. interruption leaves no temp artifacts ---------------------------------


def test_interrupt_leaves_no_temp_artifacts(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    temp_base = tmp_path / "tmp"
    calls = {"count": 0}
    original_extract = ReplayWorkspace.extract_archive

    def interrupting_extract(self, archive_path, digest):
        calls["count"] += 1
        if calls["count"] == 2:
            raise KeyboardInterrupt
        return original_extract(self, archive_path, digest)

    monkeypatch.setattr(ReplayWorkspace, "extract_archive",
                        interrupting_extract)
    with pytest.raises(KeyboardInterrupt):
        build_report(data_dir, temp_base)
    assert not temp_base.exists() or list(temp_base.iterdir()) == []


def test_workspace_cleans_up_on_signals(tmp_path):
    workspace = ReplayWorkspace(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        with workspace:
            (workspace.path / "artifact.tmp").write_text("x", encoding="utf-8")
            workspace._handle_signal(signal.SIGINT, None)
    assert not workspace.path.exists()
    assert signal.getsignal(signal.SIGINT) is not workspace._handle_signal

    second = ReplayWorkspace(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        with second:
            (second.path / "artifact.tmp").write_text("x", encoding="utf-8")
            second._handle_signal(signal.SIGTERM, None)
    assert excinfo.value.code == 128 + signal.SIGTERM
    assert not second.path.exists()
    assert list(tmp_path.iterdir()) == []


def test_workspace_isolates_concurrent_runs(tmp_path):
    first = ReplayWorkspace(tmp_path)
    second = ReplayWorkspace(tmp_path)
    with first as one, second as two:
        assert one.path != two.path
        assert one.path.parent == two.path.parent
        (one.path / "x.tmp").write_text("1", encoding="utf-8")
        assert not (two.path / "x.tmp").exists()
    assert list(tmp_path.iterdir()) == []


# -- 7. replay is strictly read-only ------------------------------------------


def test_replay_does_not_touch_the_ledger(
        tmp_path, mock_server, frozen_clock, monkeypatch):
    data_dir = run_scenario(tmp_path, mock_server, frozen_clock, monkeypatch)
    before = tree_hashes(data_dir)
    assert cli.main(["replay", "--data-dir", str(data_dir),
                     "--output", str(tmp_path / "replay.html"),
                     "--json-out", str(tmp_path / "replay.json"),
                     "--temp-dir", str(tmp_path / "tmp")]) == 0
    assert tree_hashes(data_dir) == before
    assert not (tmp_path / "tmp").exists() or \
        list((tmp_path / "tmp").iterdir()) == []


# -- round discovery and selection -------------------------------------------


def test_round_discovery_clusters_and_selects(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    with connect(paths.db_path) as connection:
        for moment in (100.0, 100.5, 200.0, 200.4, 500.0):
            connection.execute(
                "INSERT INTO probes(watch_name, checked_at) VALUES('w', ?)",
                (moment,))
        connection.commit()
    with closing(connect_readonly(paths.db_path)) as connection:
        rounds = discover_rounds(connection, gap_seconds=2.0)
    assert [(rnd.index, rnd.start, rnd.end) for rnd in rounds] == [
        (1, 100.0, 100.5), (2, 200.0, 200.4), (3, 500.0, 500.0)]

    assert [rnd.index for rnd in select_rounds(rounds)] == [1, 2, 3]
    assert [rnd.index for rnd in select_rounds(rounds, selectors=["2"])] == [2]
    assert [rnd.index for rnd in select_rounds(rounds, selectors=["1", "3"])] == [1, 3]
    assert [rnd.index for rnd in select_rounds(rounds, last=2)] == [2, 3]
    assert [rnd.index for rnd in select_rounds(rounds, selectors=["200.2"])] == [2]
    iso = "1970-01-01T00:03:20Z"  # 200.0
    assert [rnd.index for rnd in select_rounds(rounds, selectors=[iso])] == [2]
    with pytest.raises(ValueError, match="out of range"):
        select_rounds(rounds, selectors=["4"])
    with pytest.raises(ValueError, match="no monitoring round"):
        select_rounds(rounds, selectors=["300.0"])
    with pytest.raises(ValueError, match="--last"):
        select_rounds(rounds, last=0)


def test_replay_requires_an_existing_database(tmp_path):
    assert cli.main(["replay", "--data-dir", str(tmp_path / "missing")]) == 2


# -- edge-path breakpoints ----------------------------------------------------


def test_fetch_error_round_is_recorded_not_broken(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    with connect(paths.db_path) as connection:
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified, error) VALUES('flaky', 100.0, NULL, 0, "
            "'FetchError: timeout')")
        connection.commit()
    report = build_report(tmp_path / "data", tmp_path / "ws")
    chain = report.rounds[0].chains[0]
    stages = stages_by_name(chain)
    assert stages["probe"].status == "ok"
    assert stages["probe"].details["error"].startswith("FetchError")
    assert stages["archive"].status == "skipped"
    assert stages["snapshot"].status == "skipped"
    assert stages["alert"].status == "skipped"
    # a recorded fetch failure is a fact of the round, not a breakpoint
    assert chain.status == "ok"


def test_corrupt_and_mismatched_archives_are_breakpoints(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    corrupt_digest = hashlib.sha256(b"corrupt").hexdigest()
    (paths.archive_dir / f"{corrupt_digest}.gz").write_bytes(b"not a gzip")
    _, real_digest, _ = write_archive(b"real content", paths.archive_dir)
    mismatch_digest = hashlib.sha256(b"other content").hexdigest()
    (paths.archive_dir / f"{real_digest}.gz").rename(
        paths.archive_dir / f"{mismatch_digest}.gz")
    with connect(paths.db_path) as connection:
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified, raw_sha256) VALUES('corrupt', 100.0, 200, 0, ?)",
            (corrupt_digest,))
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified, raw_sha256) VALUES('mismatch', 100.0, 200, 0, ?)",
            (mismatch_digest,))
        connection.commit()
    report = build_report(tmp_path / "data", tmp_path / "ws")
    corrupt = stages_by_name(chain_by_name(report.rounds[0], "corrupt"))
    assert corrupt["archive"].status == "invalid"
    assert "cannot be decompressed" in corrupt["archive"].reason
    mismatch = stages_by_name(chain_by_name(report.rounds[0], "mismatch"))
    assert mismatch["archive"].status == "invalid"
    assert "hash mismatch" in mismatch["archive"].reason
    assert report.status == "broken"


def test_orphan_snapshot_marks_missing_probe(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    _, raw_digest, _ = write_archive(b"<html>orphan</html>", paths.archive_dir)
    text = "orphan text"
    content_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    with connect(paths.db_path) as connection:
        # the round is defined by another watch's probe; this watch lost its
        # probe record but kept its snapshot
        connection.execute(
            "INSERT INTO probes(watch_name, checked_at, status_code, "
            "not_modified) VALUES('other', 100.0, 304, 1)")
        connection.execute(
            "INSERT INTO snapshots(watch_name, version, fetched_at, "
            "status_code, url, content_sha256, raw_sha256, normalized_text, "
            "blocks_json, normalizer_version) VALUES('orphan', 1, 100.0, "
            "200, 'http://127.0.0.1/x', ?, ?, ?, '[]', 'normalizer-v1')",
            (content_digest, raw_digest, text))
        connection.commit()
    report = build_report(tmp_path / "data", tmp_path / "ws")
    chain = chain_by_name(report.rounds[0], "orphan")
    stages = stages_by_name(chain)
    assert stages["probe"].status == "missing"
    assert "snapshot #1" in stages["probe"].reason
    # the archive link is still verified from the snapshot's raw hash
    assert stages["archive"].status == "ok"
    assert stages["snapshot"].status == "ok"
    assert chain.status == "broken"


def test_wal_guard_refuses_uncheckpointed_ledger(tmp_path):
    import sqlite3

    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    writer = sqlite3.connect(str(paths.db_path))
    try:
        writer.execute(
            "INSERT INTO probes(watch_name, checked_at) VALUES('w', 1.0)")
        writer.commit()  # frames sit in the -wal until the writer closes
        with pytest.raises(RuntimeError, match="un-checkpointed WAL"):
            connect_readonly(paths.db_path)
    finally:
        writer.close()
    # after a clean close the ledger is checkpointed and readable
    with closing(connect_readonly(paths.db_path)) as connection:
        assert discover_rounds(connection)


def test_empty_database_produces_empty_report(tmp_path):
    paths = DataPaths(tmp_path / "data")
    init_paths(paths)
    report = build_report(tmp_path / "data", tmp_path / "ws")
    assert report.rounds == ()
    assert report.status == "ok"
    payload = json.loads(render_report_json(report))
    assert payload["round_count"] == 0
