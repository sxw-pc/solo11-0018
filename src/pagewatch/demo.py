"""One-command offline demo: local site -> scenario steps -> digest."""

from __future__ import annotations

import logging
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

import yaml
from werkzeug.serving import make_server

from mocksite.app import create_app
from mocksite.scenarios import ScenarioState

from .alert.outbox import read_outbox
from .clock import FrozenClock, utc_iso
from .config import load_all
from .models import DataPaths
from .report.digest import write_digest
from .runner import Runner
from .schedule.scheduler import RoundFailure, execute_round
from .storage.db import connect, init_paths
from .storage.events import count_events, list_events

STEP_LABELS = {
    0: "baseline",
    1: "documentation paragraph changed",
    2: "noisy counter changed only",
    3: "focus notice changed + keyword",
    4: "no change (304 expected)",
}
NOISE_PATTERNS = [
    r"当前访问计数[：:]\s*\d+",
    r"更新时间[：:]\s*[0-9:\- ]+",
    r"随机会话[：:]\s*[A-Za-z0-9_-]+",
]


def _write_configs(root: Path, base_url: str) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    watches = {
        "version": 1,
        "watches": [
            {
                "name": "announcements", "url": f"{base_url}/announcements",
                "interval_seconds": 60, "tags": ["notice", "priority"],
                "include_selectors": ["#notice-area"],
                "exclude_selectors": [".analytics"],
                "noise_patterns": [], "keywords": ["重要", "计费"],
            },
            {
                "name": "docs", "url": f"{base_url}/docs",
                "interval_seconds": 60, "tags": ["docs"],
                "include_selectors": ["#doc-body"],
                "exclude_selectors": [], "noise_patterns": [], "keywords": [],
            },
            {
                "name": "pricing", "url": f"{base_url}/pricing",
                "interval_seconds": 60, "tags": ["pricing"],
                "include_selectors": ["#price-table"],
                "exclude_selectors": [".ad"],
                "noise_patterns": [], "keywords": ["价格"],
            },
            {
                "name": "noisy-status", "url": f"{base_url}/noisy",
                "interval_seconds": 60, "tags": ["status", "noisy"],
                "include_selectors": ["main"], "exclude_selectors": [],
                "noise_patterns": NOISE_PATTERNS, "keywords": [],
            },
        ],
    }
    rules = {
        "version": 1,
        "classification": {
            "noise_max_change_rate": 0.02,
            "major_change_rate": 0.35,
            "focus_selectors": ["#main-notice", ".critical"],
        },
        "alerts": {"severities": ["major"], "cooldown_seconds": 3600},
        "diff": {"max_chars": 12000},
        "retention": {"keep_last": 3, "major_keep_forever": True,
                      "archive_max_age_days": 30},
        "schedule": {"max_workers": 1, "jitter_seconds": 0},
        "fetch": {"timeout_seconds": 5, "max_bytes": 2097152},
    }
    watch_path = root / "watchlist.yaml"
    rules_path = root / "rules.yaml"
    watch_path.write_text(yaml.safe_dump(watches, allow_unicode=True,
                                         sort_keys=False), encoding="utf-8")
    rules_path.write_text(yaml.safe_dump(rules, allow_unicode=True,
                                         sort_keys=False), encoding="utf-8")
    return watch_path, rules_path


def _server_log_level(level: int) -> int:
    logger = logging.getLogger("werkzeug")
    previous = logger.level
    logger.setLevel(level)
    return previous


def run_demo(root: str | Path = "data") -> int:
    data_root = Path(root)
    demo_root = data_root / "demo"
    out_dir = data_root / "out"
    if demo_root.exists():
        shutil.rmtree(demo_root)
    demo_root.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    scenario = ScenarioState()
    app = create_app(scenario)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, name="mocksite",
                              daemon=True)
    previous_log_level = _server_log_level(logging.WARNING)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{port}"
        watch_path, rules_path = _write_configs(demo_root, base_url)
        watches, rules = load_all(watch_path, rules_path)
        paths = DataPaths(demo_root)
        init_paths(paths)
        clock = FrozenClock(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
        start = clock.now()
        runner = Runner(
            db_path=paths.db_path, archive_dir=paths.archive_dir,
            outbox_path=paths.outbox_path, rules=rules, clock=clock,
        )
        print(f"mock site started at {base_url} (scenario seed {scenario.seed})")
        for step in range(5):
            scenario.set_step(step)
            if step > 0:
                clock.advance(60)
            results = execute_round(watches, runner.run, max_workers=1,
                                    stagger_seconds=rules.jitter_seconds)
            rendered = ", ".join(
                f"{item.watch_name}="
                + (f"error:{type(item.error).__name__}"
                   if isinstance(item, RoundFailure) else item.outcome)
                + ("" if isinstance(item, RoundFailure) or not item.severity
                   else f"/{item.severity}")
                for item in results
            )
            print(f"step {step} [{STEP_LABELS[step]}]: {rendered}")

        with connect(paths.db_path) as connection:
            events = list_events(connection, since=start, until=clock.now())
            counts = count_events(connection)
        digest_path = out_dir / "digest.html"
        json_path = out_dir / "events.json"
        result = write_digest(
            events, digest_path, json_path=json_path, since=start,
            until=clock.now(), generated_at=utc_iso(clock.now()),
        )
        alerts = read_outbox(paths.outbox_path)
        print("")
        print(f"events: {counts or {}}")
        print(f"alerts (local JSONL only): {len(alerts)}")
        print("")
        print(f"watchlist : {watch_path}")
        print(f"rules     : {rules_path}")
        print(f"database  : {paths.db_path}")
        print(f"archive   : {paths.archive_dir}")
        print(f"outbox    : {paths.outbox_path}")
        print(f"digest    : {result.html_path}")
        print(f"json      : {result.json_path}")
        if not any(event.severity == "major" for event in events):
            print("demo assertion failed: expected a major event", flush=True)
            return 1
        return 0
    finally:
        server.shutdown()
        thread.join(timeout=5)
        logging.getLogger("werkzeug").setLevel(previous_log_level)


def main() -> int:
    return run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
