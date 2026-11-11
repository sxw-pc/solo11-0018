from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import pagewatch.cli as cli
from pagewatch.models import Rules, WatchSpec
from pagewatch.report.status import create_status_app, serve_status
from pagewatch.runner import Runner
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import list_audit, list_events
from pagewatch.storage.snapshots import list_schedules

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


def run_once(config: Path, rules: Path, data_dir: Path) -> int:
    return cli.main(["run-once", "--config", str(config), "--rules", str(rules),
                     "--data-dir", str(data_dir)])


def test_full_scenario_cli_digest_status_and_restart(tmp_path, mock_server,
                                                     frozen_clock, monkeypatch):
    base_url, scenario = mock_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_watchlist(tmp_path / "watchlist.yaml", base_url)
    rules_path = ROOT / "configs/rules.example.yaml"
    data_dir = tmp_path / "data"

    scenario.set_step(0)
    assert run_once(config, rules_path, data_dir) == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        assert len(list_events(connection)) == 0
        assert connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 4

    scenario.set_step(1)
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        events = list_events(connection)
        assert len(events) == 1
        assert (events[0].watch_name, events[0].severity) == ("docs", "minor")

    scenario.set_step(2)
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        assert len(list_events(connection)) == 1
        assert connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 5

    scenario.set_step(3)
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        events = list_events(connection)
        assert len(events) == 2
        major = [event for event in events if event.severity == "major"]
        assert len(major) == 1
        assert major[0].watch_name == "announcements"
        assert "重要" in major[0].keyword_hits
        assert connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM probes WHERE not_modified=1").fetchone()[0] > 0
    outbox = data_dir / "outbox" / "alerts.jsonl"
    assert len(outbox.read_text(encoding="utf-8").splitlines()) == 1

    # Re-running the same step is idempotent: conditional requests return 304.
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0
    scenario.set_step(4)
    frozen_clock.advance(60)
    assert run_once(config, rules_path, data_dir) == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        assert len(list_events(connection)) == 2
        assert connection.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 1
        assert len(list_schedules(connection)) == 4

    # Static digest and JSON export are deterministic for identical inputs.
    first = tmp_path / "digest1.html"
    second = tmp_path / "digest2.html"
    json_out = tmp_path / "events.json"
    args = ["digest", "--data-dir", str(data_dir), "--generated-at",
            "2026-01-01T00:05:00Z"]
    assert cli.main(args + ["--output", str(first), "--json-out", str(json_out)]) == 0
    assert cli.main(args + ["--output", str(second)]) == 0
    assert first.read_bytes() == second.read_bytes()
    html = first.read_text(encoding="utf-8")
    assert "announcements" in html and "docs" in html
    assert "major" in html and "minor" in html
    assert "计费规则" in html and "归一化说明" in html
    assert "Noise events folded" not in html  # no noise event in this scenario
    exported = json.loads(json_out.read_text(encoding="utf-8"))
    assert len(exported) == 2
    assert {item["severity"] for item in exported} == {"major", "minor"}

    app = create_status_app(data_dir / "pagewatch.sqlite")
    with app.test_client() as client:
        response = client.get("/status")
        assert response.status_code == 200
        assert b"announcements" in response.data
        assert client.get("/healthz").status_code == 200
    with pytest.raises(ValueError, match="127.0.0.1"):
        serve_status(data_dir / "pagewatch.sqlite", host="0.0.0.0")


def test_allowlist_rejection_is_audited(tmp_path, frozen_clock):
    from pagewatch.models import DataPaths
    data = DataPaths(tmp_path / "data")
    init_paths(data)
    watch = WatchSpec(name="bad", url="https://outside.invalid/", interval_seconds=60)
    runner = Runner(db_path=data.db_path, archive_dir=data.archive_dir,
                    outbox_path=data.outbox_path, rules=Rules(), clock=frozen_clock)
    result = runner.run(watch)
    assert result.outcome == "error"
    with connect(data.db_path) as connection:
        audit = list_audit(connection, "allowlist.rejected")
        assert audit and "blocked by the local allowlist" in audit[0]["message"]
        assert connection.execute("SELECT COUNT(*) FROM probes").fetchone()[0] == 1
