"""Command-line interface for pagewatch."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from . import __version__
from .clock import Clock, SystemClock, parse_iso, utc_iso
from .config import ConfigError, load_all, load_rules
from .fetch.allowlist import is_host_allowed
from .models import DataPaths
from .retention.compact import compact
from .retention.policy import format_plan
from .runner import Runner
from .schedule.scheduler import execute_round, select_watches
from .storage.db import connect, init_paths
from .storage.events import list_events
from .storage.snapshots import list_schedules

DEFAULT_WATCHLIST = "configs/watchlist.example.yaml"
DEFAULT_RULES = "configs/rules.example.yaml"


def build_clock() -> Clock:
    """Indirection used by tests to inject a frozen clock."""
    return SystemClock()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pagewatch",
        description="Local, auditable page-change monitor. Targets are allowlisted "
                    "to 127.0.0.1, localhost and ::1 only.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="validate watchlist and rules files")
    check.add_argument("--config", default=DEFAULT_WATCHLIST)
    check.add_argument("--rules", default=DEFAULT_RULES)
    check.set_defaults(func=_cmd_check_config)

    run = sub.add_parser("run-once", help="run one monitoring round")
    run.add_argument("--config", default=DEFAULT_WATCHLIST)
    run.add_argument("--rules", default=DEFAULT_RULES)
    run.add_argument("--data-dir", default="data")
    run.add_argument("--name", action="append", default=[])
    run.add_argument("--tag", action="append", default=[])
    run.add_argument("--due-only", action="store_true",
                     help="only watches whose persisted next_due_at has passed")
    run.add_argument("--json", action="store_true", help="print results as JSON")
    run.set_defaults(func=_cmd_run_once)

    digest = sub.add_parser("digest", help="write a static HTML change digest")
    digest.add_argument("--data-dir", default="data")
    digest.add_argument("--since", default=None)
    digest.add_argument("--until", default=None)
    digest.add_argument("--output", default=None)
    digest.add_argument("--json-out", default=None)
    digest.add_argument("--generated-at", default=None)
    digest.add_argument("--title", default="pagewatch change digest")
    digest.set_defaults(func=_cmd_digest)

    compact_cmd = sub.add_parser("compact", help="apply retention/cleanup policy")
    compact_cmd.add_argument("--rules", default=DEFAULT_RULES)
    compact_cmd.add_argument("--data-dir", default="data")
    compact_cmd.add_argument("--dry-run", action="store_true")
    compact_cmd.set_defaults(func=_cmd_compact)

    status = sub.add_parser("serve-status", help="serve read-only status on localhost")
    status.add_argument("--data-dir", default="data")
    status.add_argument("--host", default="127.0.0.1")
    status.add_argument("--port", type=int, default=8766)
    status.set_defaults(func=_cmd_serve_status)

    demo = sub.add_parser("demo", help="run the bundled end-to-end demo")
    demo.set_defaults(func=_cmd_demo)
    return parser


def _cmd_check_config(args: argparse.Namespace) -> int:
    watches, rules = load_all(args.config, args.rules)
    enabled = sum(1 for watch in watches if watch.enabled)
    print(f"config OK: {len(watches)} watch(es), {enabled} enabled; "
          f"max_workers={rules.max_workers}, keep_last={rules.keep_last}")
    for watch in watches:
        print(f"  {watch.name}: {watch.url} every {watch.interval_seconds}s")
    return 0


def _cmd_run_once(args: argparse.Namespace) -> int:
    watches, rules = load_all(args.config, args.rules)
    paths = DataPaths(Path(args.data_dir))
    init_paths(paths)
    clock = build_clock()
    now = clock.now()
    with connect(paths.db_path) as connection:
        states = list_schedules(connection)
    selected = select_watches(watches, names=args.name, tags=args.tag,
                              due_only=args.due_only, states=states, now=now)
    runner = Runner(db_path=paths.db_path, archive_dir=paths.archive_dir,
                    outbox_path=paths.outbox_path, rules=rules, clock=clock)
    results = execute_round(selected, runner.run, rules.max_workers)
    payload = [asdict(result) for result in results]
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for result in results:
            suffix = f" severity={result.severity}" if result.severity else ""
            error = f" error={result.error}" if result.error else ""
            print(f"{result.watch_name}: {result.outcome}{suffix}{error}")
        if not results:
            print("no watches selected")
    return 1 if any(result.outcome == "error" for result in results) else 0


def _cmd_digest(args: argparse.Namespace) -> int:
    from .report.digest import write_digest

    paths = DataPaths(Path(args.data_dir))
    if not paths.db_path.exists():
        raise FileNotFoundError(f"database not found: {paths.db_path}")
    since = parse_iso(args.since) if args.since else None
    until = parse_iso(args.until) if args.until else None
    with connect(paths.db_path) as connection:
        events = list_events(connection, since=since, until=until)
    output = Path(args.output) if args.output else paths.out_dir / "digest.html"
    json_out = Path(args.json_out) if args.json_out else None
    generated_at = args.generated_at
    if generated_at is None:
        generated_at = utc_iso(max((event.created_at for event in events),
                                   default=until if until is not None else 0.0))
    result = write_digest(events, output, json_path=json_out, since=since,
                          until=until, generated_at=generated_at,
                          title=args.title)
    print(f"digest: {result.html_path} ({result.event_count} event(s))")
    if result.json_path:
        print(f"events: {result.json_path}")
    return 0


def _cmd_compact(args: argparse.Namespace) -> int:
    rules = load_rules(args.rules)
    paths = DataPaths(Path(args.data_dir))
    if not paths.db_path.exists():
        raise FileNotFoundError(f"database not found: {paths.db_path}")
    clock = build_clock()
    result = compact(paths.db_path, paths.archive_dir, rules, clock.now(),
                     dry_run=args.dry_run)
    print(("DRY RUN (no changes)\n" if args.dry_run else "COMPACT\n")
          + format_plan(result.plan))
    if result.executed:
        print(f"removed {result.removed_snapshots} snapshot(s), "
              f"{result.removed_archives} archive(s), {result.removed_bytes} bytes")
    return 0


def _cmd_serve_status(args: argparse.Namespace) -> int:
    if args.host != "127.0.0.1" or not is_host_allowed(args.host):
        raise ValueError("serve-status may only bind to 127.0.0.1")
    paths = DataPaths(Path(args.data_dir))
    if not paths.db_path.exists():
        raise FileNotFoundError(f"database not found: {paths.db_path}")
    from .report.status import serve_status
    print(f"serving read-only status at http://{args.host}:{args.port}/status")
    serve_status(paths.db_path, host=args.host, port=args.port)
    return 0


def _cmd_demo(_: argparse.Namespace) -> int:
    from .demo import run_demo
    return run_demo()


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ConfigError, FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
