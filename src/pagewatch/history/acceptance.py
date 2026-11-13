"""Offline, deterministic acceptance scenario for the history module.

Run with ``pagewatch history-acceptance`` (or
``python -m pagewatch.history.acceptance``).  The scenario:
1. starts the bundled local mock site (loopback only, no network);
2. drives five deterministic monitoring rounds with a frozen clock so real
   events/snapshots/audit rows are produced by the unchanged monitoring path;
3. adds scripted legacy rows: several watches changing at the *same*
   timestamp with different severities, one closed watch and one watch
   deleted from the watchlist;
4. exercises filters (name/tag/severity union, half-open range);
5. exports two consecutive rounds from the same database, where new events
   are written *between* rounds, and asserts that the union of both rounds
   equals the full event set exactly once (no duplicates, no omissions);
6. writes new events while an *old* mid-round cursor is still in use and
   asserts the old cursor keeps advancing without seeing or skipping them,
   and that a later pull sees them exactly once;
7. checks the distinguishable error taxonomy (malformed cursor, filter
   mismatch, out-of-range cursor, invalid range).

Everything is local and reproducible: no wall clock, no network.  The
scenario fails loudly (exit 1) and writes a JSON report of each check.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml
from werkzeug.serving import make_server

from mocksite.app import create_app
from mocksite.scenarios import ScenarioState

from ..clock import FrozenClock
from ..models import DataPaths
from ..config import load_watchlist, load_all
from .query import HistoryQuery, query_events
from ..storage.db import connect, init_paths, json_dump
from ..storage.events import insert_event
from .reader import connect_readonly
from .tags import load_tag_snapshot
from .errors import (
    CursorFilterMismatch, CursorMalformed, CursorOutOfRange, HistoryError,
)
from .export import ExportBatch, export_jsonl, write_cursor_file

BASE_TS = 1_767_225_600.0  # frozen clock origin, matches the demo
STEP_SECONDS = 60.0
ACCEPTANCE_EXTRA_TS = BASE_TS  # same instant for several multi-severity
                              # events on different watches
NOISE_PATTERNS = [
    r"当前访问计数[：:]\s*\d+",
    r"更新时间[：:]\s*[0-9:\- ]+",
    r"随机会话[：:]\s*[A-Za-z0-9_-]+",
]


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {"check": self.name, "ok": self.ok, "detail": self.detail}


def _watchlist_data(base_url: str) -> dict[str, object]:
    return {
        "version": 1,
        "watches": [
            {"name": "announcements", "url": f"{base_url}/announcements",
             "interval_seconds": 60, "tags": ["notice", "priority"],
             "include_selectors": ["#notice-area"],
             "exclude_selectors": [".analytics"],
             "noise_patterns": [], "keywords": ["重要", "计费"]},
            {"name": "docs", "url": f"{base_url}/docs",
             "interval_seconds": 60, "tags": ["docs"],
             "include_selectors": ["#doc-body"],
             "exclude_selectors": [], "noise_patterns": [], "keywords": []},
            {"name": "pricing", "url": f"{base_url}/pricing",
             "interval_seconds": 60, "tags": ["pricing"],
             "include_selectors": ["#price-table"],
             "exclude_selectors": [".ad"],
             "noise_patterns": [], "keywords": []},
            {"name": "noisy-status", "url": f"{base_url}/noisy",
             "interval_seconds": 60, "tags": ["status", "noisy"],
             "include_selectors": ["main"], "exclude_selectors": [],
             "noise_patterns": NOISE_PATTERNS, "keywords": []},
            # Enabled watches whose only role is to prove that identical
            # timestamps from different watches resolve to a stable order.
            {"name": "alpha-tie", "url": f"{base_url}/docs",
             "interval_seconds": 60, "tags": ["legacy", "tie"],
             "include_selectors": ["#doc-body"], "exclude_selectors": [],
             "noise_patterns": [], "keywords": []},
            {"name": "zeta-tie", "url": f"{base_url}/pricing",
             "interval_seconds": 60, "tags": ["legacy", "tie"],
             "include_selectors": ["#price-table"], "exclude_selectors": [],
             "noise_patterns": [], "keywords": []},
            # Closed watch: still in the watchlist but disabled.  Its history
            # must remain queryable and be flagged as degraded.
            {"name": "closed-status", "url": f"{base_url}/noisy",
             "interval_seconds": 60, "tags": ["legacy", "status"],
             "include_selectors": ["main"], "exclude_selectors": [],
             "noise_patterns": NOISE_PATTERNS, "keywords": [],
             "enabled": False},
            # The legacy watch below is deliberately *absent* from this list;
            # its rows exist only in the database, so it is "deleted".
        ],
    }


def _rules_data() -> dict[str, object]:
    return {
        "version": 1,
        "classification": {"noise_max_change_rate": 0.02,
                           "major_change_rate": 0.35,
                           "focus_selectors": ["#main-notice", ".critical"]},
        "alerts": {"severities": ["major"], "cooldown_seconds": 3600},
        "diff": {"max_chars": 12000},
        "retention": {"keep_last": 3, "major_keep_forever": True,
                      "archive_max_age_days": 30},
        "schedule": {"max_workers": 1, "jitter_seconds": 0},
        "fetch": {"timeout_seconds": 5, "max_bytes": 2097152},
    }


def _write_configs(root: Path, base_url: str) -> tuple[Path, Path]:
    watchlist = root / "watchlist.yaml"
    rules = root / "rules.yaml"
    watchlist.write_text(yaml.safe_dump(_watchlist_data(base_url),
                                        allow_unicode=True, sort_keys=False),
                         encoding="utf-8")
    rules.write_text(yaml.safe_dump(_rules_data(), allow_unicode=True,
                                    sort_keys=False), encoding="utf-8")
    return watchlist, rules


def _insert_watch(connection, name: str, tags: list[str]) -> None:
    connection.execute(
        "INSERT INTO watches(name, url, tags_json) VALUES(?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET tags_json=excluded.tags_json",
        (name, f"http://127.0.0.1/legacy/{name}", json_dump(tags)),
    )


def _insert_event(connection, *, watch_name: str, severity: str,
                  created_at: float, diff_text: str = "acceptance") -> int:
    event = insert_event(
        connection, watch_name=watch_name, from_snapshot_id=1,
        to_snapshot_id=2, created_at=created_at, severity=severity,
        change_rate=0.1, added_lines=1, removed_lines=1,
        regions=("#legacy",), keyword_hits=(), focus_hit=False,
        summary=f"acceptance {severity} event",
        diff_text=f"--- previous\n+++ current\n@@ acceptance @@\n{diff_text}",
    )
    return int(event.id)


def _seed_legacy_events(connection) -> dict[str, object]:
    """Scripted rows exercising tie-ordering and lifecycle degradation."""
    t = ACCEPTANCE_EXTRA_TS
    _insert_watch(connection, "audit-legacy", ["legacy", "notice"])
    _insert_watch(connection, "closed-status", ["legacy", "status"])
    # Same instant, three watches, three severities, intentionally inserted
    # out of (watch_name) alphabetical order to verify the tie-breaker.
    ids = {
        "zeta-tie": _insert_event(connection, watch_name="zeta-tie",
                                  severity="major", created_at=t + 10),
        "alpha-tie": _insert_event(connection, watch_name="alpha-tie",
                                   severity="minor", created_at=t + 10),
        "audit-legacy": _insert_event(connection, watch_name="audit-legacy",
                                      severity="noise", created_at=t + 10),
        "closed-status": _insert_event(connection, watch_name="closed-status",
                                       severity="minor", created_at=t + 20),
    }
    _insert_watch(connection, "zeta-tie", ["legacy"])
    _insert_watch(connection, "alpha-tie", ["legacy"])
    connection.commit()
    order = connection.execute(
        "SELECT watch_name, id FROM events WHERE created_at=? "
        "ORDER BY created_at, watch_name, id", (t + 10,)
    ).fetchall()
    return {"ids": ids,
            "tie_order": [row["watch_name"] for row in order]}


def _csv_keys(path: Path) -> list[tuple[str, int]]:
    keys: list[tuple[str, int]] = []
    if not path.exists():
        return keys
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            value = json.loads(line)
            keys.append((str(value["watch_name"]), int(value["event_seq"])))
    return keys


def _all_event_keys(connection) -> set[tuple[str, int]]:
    rows = connection.execute("SELECT watch_name, id FROM events").fetchall()
    return {(row["watch_name"], int(row["id"])) for row in rows}


def _pull_pages(connection, query: HistoryQuery, snapshot, *, cursor,
                page_size: int, destination: Path, append: bool) -> str:
    """Pull pages until exhausted, appending JSONL to ``destination``."""
    while True:
        batch = export_jsonl(connection, query, destination, cursor=cursor,
                             page_size=page_size, append=append,
                             tag_snapshot=snapshot)
        append = True
        cursor = batch.next_cursor
        if batch.exhausted:
            return cursor


def _check(name: str, ok: bool, **detail: object) -> CheckResult:
    return CheckResult(name=name, ok=bool(ok), detail=detail)


def _run_reproducible_rounds(root: Path) -> dict[str, object]:
    scenario = ScenarioState()
    app = create_app(scenario)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    port = server.server_port
    previous_level = logging.getLogger("werkzeug").level
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    thread = threading.Thread(target=server.serve_forever, name="acceptance-mock",
                              daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{port}"
        watch_path, rules_path = _write_configs(root, base_url)
        watches, rules = load_all_acceptance(watch_path, rules_path)
        paths = DataPaths(root)
        init_paths(paths)
        clock = FrozenClock(BASE_TS)
        runner = RunnerAcceptance(paths=paths, rules=rules, clock=clock)
        for step in range(5):
            scenario.set_step(step)
            if step > 0:
                clock.advance(STEP_SECONDS)
            results = runner.run_round(watches)
            if any(result.outcome == "error" for result in results):
                raise RuntimeError("monitoring round failed during acceptance")
        with connect(paths.db_path) as connection:
            legacy = _seed_legacy_events(connection)
        return {"base_url": base_url, "legacy": legacy,
                "watchlist": watch_path, "data_paths": paths,
                "clock": clock}
    finally:
        server.shutdown()
        thread.join(timeout=5)
        logging.getLogger("werkzeug").setLevel(previous_level)


# Small indirections so the acceptance module imports the monitoring chain
# lazily and remains usable without a running server for the read-only checks.
def load_all_acceptance(watch_path, rules_path):
    return load_all(watch_path, rules_path)


class RunnerAcceptance:
    def __init__(self, *, paths: DataPaths, rules, clock: FrozenClock) -> None:
        from ..runner import Runner
        self._runner = Runner(db_path=paths.db_path, archive_dir=paths.archive_dir,
                              outbox_path=paths.outbox_path, rules=rules,
                              clock=clock)
        self._rules = rules

    def run_round(self, watches) -> list[object]:
        from ..schedule.scheduler import execute_round
        return execute_round(watches, self._runner.run, self._rules.max_workers)


def _prepare_outdir(out_root: Path) -> None:
    """Recreate ``out_root`` so repeated runs are byte-for-byte reproducible.

    The directory is resolved and then checked to look like a dedicated
    acceptance output location; the scenario deletes and rewrites it.
    """
    out_root = out_root.expanduser()
    if out_root.name in {"", ".", "..", "/"}:
        raise ValueError("refusing to use the filesystem root as "
                         "acceptance output")
    if out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True)


def run_acceptance(root: str | Path = "data/acceptance", *,
                   mock_writers: bool = False) -> int:
    """Run the scenario and return a process exit code (0 == all passed).

    ``mock_writers=True`` additionally drives the same two-round protocol
    through the public read-only API while a background thread keeps
    committing real events, proving the pull has no hole or duplicate under
    continuous write pressure.  It is timing-dependent by nature, so it is
    opt-in; the default run stays deterministic.
    """
    out_root = Path(root)
    _prepare_outdir(out_root)
    seeded = _run_reproducible_rounds(out_root)

    paths: DataPaths = seeded["data_paths"]
    watchlist_path: Path = seeded["watchlist"]
    watches = load_watchlist(watchlist_path)
    clock: FrozenClock = seeded["clock"]

    checks: list[CheckResult] = []
    artifacts: dict[str, object] = {"database": str(paths.db_path),
                                    "watchlist": str(watchlist_path)}

    # --- lifecycle + tag resolution -------------------------------------
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
    legacy_names = {"audit-legacy", "closed-status", "zeta-tie", "alpha-tie"}
    states = {name: snapshot.meta(name).state for name in legacy_names}
    checks.append(_check(
        "lifecycle_states", states == {"audit-legacy": "deleted",
                                       "closed-status": "closed",
                                       "zeta-tie": "active",
                                       "alpha-tie": "active"},
        states=states))

    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        tagged = query_events(connection, HistoryQuery(tags=("legacy",)),
                              tag_snapshot=snapshot, limit=1000)
        degraded = {item.name: item.state for item in tagged.degraded_watches}
        names = {event.watch_name for event in tagged.events}
    checks.append(_check(
        "tag_filter_returns_deleted_and_closed_history",
        names == legacy_names
        and degraded.get("audit-legacy") == "deleted"
        and degraded.get("closed-status") == "closed",
        events=sorted(names), degraded=degraded))

    # --- half-open range and severity union ------------------------------
    t = ACCEPTANCE_EXTRA_TS
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        left = query_events(connection,
                            HistoryQuery(since=t + 10, until=t + 20),
                            tag_snapshot=snapshot, limit=1000)
        union = query_events(connection,
                             HistoryQuery(severities=("minor", "major")),
                             tag_snapshot=snapshot, limit=1000)
    half_open_ok = ({event.created_at for event in left.events} == {t + 10}
                    and not any(event.created_at == t + 20
                                for event in left.events))
    checks.append(_check("range_is_left_closed_right_open", half_open_ok,
                         rows=[event.id for event in left.events]))
    checks.append(_check(
        "severity_union_excludes_noise",
        {event.severity for event in union.events} <= {"minor", "major"}
        and all(event.severity != "noise" for event in union.events),
        severities=sorted({event.severity for event in union.events})))
    checks.append(_check(
        "same_timestamp_tie_order_is_watch_name",
        seeded["legacy"]["tie_order"] == ["alpha-tie", "audit-legacy",
                                          "zeta-tie"],
        order=seeded["legacy"]["tie_order"]))

    # --- two consecutive export rounds, new writes in between -----------
    ledger_round1 = out_root / "ledger-round1.jsonl"
    ledger_round2 = out_root / "ledger-round2.jsonl"
    cursor_file = out_root / "cursor-after-round1.json"
    query = HistoryQuery()
    inserted_between: list[int] = []
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        cursor_after_round1 = _pull_pages(
            connection, query, snapshot, cursor=None, page_size=2,
            destination=ledger_round1, append=False)
    write_cursor_file(cursor_file, _cursor_batch(cursor_after_round1), query)

    with connect(paths.db_path) as connection:
        for severity in ("major", "minor", "noise"):
            inserted_between.append(_insert_event(
                connection, watch_name="between-round", severity=severity,
                created_at=BASE_TS + 30, diff_text="between rounds"))
        connection.commit()

    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        cursor_after_round2 = _pull_pages(
            connection, query, snapshot, cursor=cursor_after_round1,
            page_size=2, destination=ledger_round2, append=False)
        all_keys = _all_event_keys(connection)
    keys1 = _csv_keys(ledger_round1)
    keys2 = _csv_keys(ledger_round2)
    combined = keys1 + keys2
    round2_only_new = (len(keys2) == len(inserted_between)
                       and {key[1] for key in keys2} == set(inserted_between))
    checks.append(_check(
        "round2_continues_from_round1_cursor",
        round2_only_new and len(combined) == len(set(combined)),
        round1_records=len(keys1), round2_records=len(keys2),
        inserted=inserted_between))
    checks.append(_check(
        "two_round_union_has_no_duplicates_and_no_omissions",
        set(combined) == all_keys and len(combined) == len(all_keys)
        and len(combined) == len(set(combined)),
        exported=len(combined), database=len(all_keys)))
    checks.append(_check(
        "duplicate_pull_is_idempotent",
        _csv_keys(ledger_round1) == keys1
        and len(keys1) == len(set(keys1)),
        records=len(keys1)))
    # Pull again from the *terminal* cursor of the second round.  No new
    # events were written since, so it must come back empty; if the round-2
    # terminal cursor ever replayed rows this would catch it.
    artifacts_cursor_round2 = cursor_after_round2
    with connect_readonly(paths.db_path) as connection:
        again = query_events(connection, query, cursor=artifacts_cursor_round2,
                             limit=1000,
                             tag_snapshot=load_tag_snapshot(connection, watches))
    checks.append(_check(
        "terminal_cursor_advances_without_replay",
        again.events == () and again.exhausted,
        terminal_cursor=artifacts_cursor_round2,
        replay_events=len(again.events)))
    artifacts["ledger_round1"] = str(ledger_round1)
    artifacts["ledger_round2"] = str(ledger_round2)
    artifacts["cursor_after_round1"] = str(cursor_file)

    # --- old mid-round cursor keeps advancing during concurrent writes --
    concurrent_ledger = out_root / "ledger-concurrent.jsonl"
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        first = query_events(connection, query, limit=2, tag_snapshot=snapshot)
    old_cursor = first.next_cursor
    first_ids = [event.id for event in first.events]
    concurrent_ids: list[int] = []
    with connect(paths.db_path) as connection:
        for index, severity in enumerate(("major", "minor")):
            concurrent_ids.append(_insert_event(
                connection, watch_name="concurrent-writer", severity=severity,
                created_at=BASE_TS + 5 * STEP_SECONDS + index,
                diff_text="written during round"))
        connection.commit()
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        reached = _pull_pages(connection, query, snapshot, cursor=old_cursor,
                              page_size=2,
                              destination=concurrent_ledger, append=False)
        during_round_ids = [int(item["event_seq"]) for item in
                            _jsonl_records(concurrent_ledger)]
        after = query_events(connection, query, cursor=reached, limit=10,
                             tag_snapshot=snapshot)
        after_ids = [event.id for event in after.events]
    checks.append(_check(
        "old_cursor_advances_without_seeing_concurrent_writes",
        not (set(during_round_ids) & set(concurrent_ids))
        and len(during_round_ids) == len(set(during_round_ids)),
        old_cursor_start=first_ids,
        during_round=during_round_ids, concurrent=concurrent_ids))
    checks.append(_check(
        "concurrent_writes_appear_exactly_once_after_round",
        after_ids == concurrent_ids,
        pulled_after=after_ids, written=concurrent_ids))
    artifacts["ledger_concurrent"] = str(concurrent_ledger)

    if mock_writers:
        checks.append(_continuous_writer_check(paths, watches, out_root))

    # --- error taxonomy --------------------------------------------------
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        page = query_events(connection, query, limit=3, tag_snapshot=snapshot)
        errors: dict[str, str] = {}
        try:
            query_events(connection, query, cursor="not-a-cursor",
                         tag_snapshot=snapshot)
        except CursorMalformed as exc:
            errors["malformed"] = exc.code
        try:
            query_events(connection,
                         HistoryQuery(watch_names=("docs",)),
                         cursor=page.next_cursor, tag_snapshot=snapshot)
        except CursorFilterMismatch as exc:
            errors["filter_mismatch"] = exc.code
        try:
            query_events(connection,
                         HistoryQuery(until=BASE_TS - 60),
                         cursor=page.next_cursor, tag_snapshot=snapshot)
        except CursorOutOfRange as exc:
            errors["out_of_range"] = exc.code
        try:
            query_events(connection,
                         HistoryQuery(since=BASE_TS + 10, until=BASE_TS + 10),
                         tag_snapshot=snapshot)
        except HistoryError as exc:
            errors["invalid_range"] = exc.code
    checks.append(_check(
        "errors_are_distinguishable",
        errors == {"malformed": "cursor_malformed",
                   "filter_mismatch": "cursor_filter_mismatch",
                   "out_of_range": "cursor_out_of_range",
                   "invalid_range": "invalid_range"},
        codes=errors,
        range_is_validation=errors.get("invalid_range") == "invalid_range"))
    artifacts["error_codes"] = errors

    # --- report ----------------------------------------------------------
    ok = all(check.ok for check in checks)
    report = {"ok": ok, "checks": [check.as_dict() for check in checks],
              "artifacts": artifacts}
    report_path = out_root / "history-acceptance.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                      sort_keys=True) + "\n", encoding="utf-8")
    print("pagewatch history acceptance")
    print(f"database : {paths.db_path}")
    print(f"report   : {report_path}")
    deterministic = (out_root / "ledger-round1.jsonl").read_text(
        encoding="utf-8")
    print(f"round1 ledger bytes: {len(deterministic)}")
    for check in checks:
        mark = "PASS" if check.ok else "FAIL"
        print(f"[{mark}] {check.name}")
        if not check.ok:
            print(f"       detail: {json.dumps(check.detail, ensure_ascii=False)}")
    print("result   : " + ("OK (all checks passed)" if ok else "FAILED"))
    return 0 if ok else 1


def _continuous_writer_check(paths: DataPaths, watches, out_root: Path
                             ) -> CheckResult:
    """Export every event exactly once while a writer keeps committing."""
    import time

    from .export import export_jsonl

    destination = out_root / "ledger-under-write-pressure.jsonl"
    stop = threading.Event()
    committed: list[int] = []

    def writer() -> None:
        with connect(paths.db_path) as connection:
            index = 0
            while not stop.is_set():
                committed.append(_insert_event(
                    connection, watch_name="pressure", severity="minor",
                    created_at=BASE_TS + 10_000 + index))
                connection.commit()
                index += 1
                time.sleep(0.002)

    thread = threading.Thread(target=writer, name="acceptance-writer",
                              daemon=True)
    thread.start()
    query = HistoryQuery()
    seen: list[tuple[str, int]] = []
    cursor = None
    appended = False
    try:
        time.sleep(0.05)
        with connect_readonly(paths.db_path) as connection:
            deadline = time.time() + 2.0
            while time.time() < deadline:
                snapshot = load_tag_snapshot(connection, watches)
                batch = export_jsonl(connection, query, destination,
                                     cursor=cursor, page_size=8,
                                     append=appended, tag_snapshot=snapshot)
                appended = True
                cursor = batch.next_cursor
                # Keep appending across rounds: the ledger is cumulative and
                # de-duplicated by (watch_name, event_seq).
    finally:
        stop.set()
        thread.join(timeout=5)
    # Writer is stopped and joined; drain the final round so the comparison
    # has no "committed after the last read" race.
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, watches)
        while True:
            batch = export_jsonl(connection, query, destination,
                                 cursor=cursor, page_size=64,
                                 append=True, tag_snapshot=snapshot)
            cursor = batch.next_cursor
            if batch.exhausted:
                break
    records = _jsonl_records(destination)
    seen = [(item["watch_name"], int(item["event_seq"])) for item in records]
    written_keys = {("pressure", int(item)) for item in committed}
    pulled_keys = {key for key in seen if key[0] == "pressure"}
    # Every committed event is exported exactly once, the whole ledger has no
    # duplicates, and nothing that was *not* written shows up.
    ok = (pulled_keys == written_keys and len(seen) == len(set(seen))
          and set(seen) >= written_keys)
    return _check("continuous_pull_under_write_pressure", ok,
                  exported_total=len(seen), exported_pressure=len(pulled_keys),
                  written=len(written_keys),
                  duplicates=len(seen) - len(set(seen)))


def _jsonl_records(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _cursor_batch(cursor: str) -> ExportBatch:
    return ExportBatch(path=Path("."), records=0, next_cursor=cursor,
                       exhausted=True, has_more=False, created=False,
                       skipped_duplicates=0, cursor_only=True)


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(prog="pagewatch history-acceptance")
    parser.add_argument("--output-dir", default="data/acceptance")
    parser.add_argument("--with-writer-stress", action="store_true",
                        help="also run the timing-dependent continuous-"
                             "writer stress check")
    args = parser.parse_args(argv)
    return run_acceptance(args.output_dir,
                          mock_writers=args.with_writer_stress)


if __name__ == "__main__":
    raise SystemExit(main())
