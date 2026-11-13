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
from .history import (
    DEFAULT_LIMIT, HistoryError, HistoryQuery, MAX_LIMIT, export_jsonl,
    load_tag_snapshot, query_events, read_cursor_file, write_cursor_file,
    write_cursor_state,
)
from .history.reader import connect_readonly
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

    history = sub.add_parser(
        "history",
        help="query change history by watch/tag/severity/time, with cursors",
    )
    history.add_argument("--data-dir", default="data")
    history.add_argument("--config", default=None,
                         help="optional watchlist; enables closed/deleted "
                              "lifecycle detection for --tag filters")
    history.add_argument("--name", action="append", default=[],
                         help="watch name (repeatable; union)")
    history.add_argument("--tag", action="append", default=[],
                         help="watch tag (repeatable; union unless "
                              "--tag-match all)")
    history.add_argument("--tag-match", choices=["any", "all"], default="any")
    history.add_argument("--severity", action="append", default=[],
                         help="noise/minor/major (repeatable; union)")
    history.add_argument("--since", default=None,
                         help="inclusive lower bound (ISO-8601 or epoch)")
    history.add_argument("--until", default=None,
                         help="exclusive upper bound (ISO-8601 or epoch)")
    history.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                         help=f"page size, 1..{MAX_LIMIT}")
    history.add_argument("--cursor", default=None,
                         help="continuation cursor returned by the previous page")
    history.add_argument("--cursor-file", default=None,
                         help="read the cursor from a file written by "
                              "--cursor-out (mutually exclusive with --cursor)")
    history.add_argument("--cursor-out", default=None,
                         help="persist the continuation cursor to this file")
    history.add_argument("--json", action="store_true",
                         help="emit the page envelope as a single JSON document")
    history.add_argument("--out", default=None,
                         help="also write matching events to this JSONL file")
    history.add_argument("--append", action="store_true",
                         help="append to --out instead of refusing to "
                              "overwrite an existing file")
    history.set_defaults(func=_cmd_history)

    export = sub.add_parser(
        "export-history",
        help="export one history page as JSONL and persist the next cursor",
    )
    export.add_argument("--data-dir", default="data")
    export.add_argument("--config", default=None)
    export.add_argument("--name", action="append", default=[])
    export.add_argument("--tag", action="append", default=[])
    export.add_argument("--tag-match", choices=["any", "all"], default="any")
    export.add_argument("--severity", action="append", default=[])
    export.add_argument("--since", default=None)
    export.add_argument("--until", default=None)
    export.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    export.add_argument("--cursor", default=None)
    export.add_argument("--cursor-file", default=None)
    export.add_argument("--out", required=True,
                         help="JSONL destination for this page")
    export.add_argument("--cursor-out", required=True,
                         help="where to persist the cursor for the next pull")
    export.add_argument("--append", action="store_true")
    export.add_argument("--json", action="store_true")
    export.set_defaults(func=_cmd_export_history)

    demo = sub.add_parser("demo", help="run the bundled end-to-end demo")
    demo.set_defaults(func=_cmd_demo)

    acceptance = sub.add_parser(
        "history-acceptance",
        help="run the offline reproducible acceptance scenario for history",
    )
    acceptance.add_argument("--output-dir", default="data/acceptance")
    acceptance.add_argument("--with-writer-stress", action="store_true",
                            help="also run the timing-dependent "
                                 "continuous-writer stress check")
    acceptance.set_defaults(func=_cmd_history_acceptance)
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


def _cmd_history(args: argparse.Namespace) -> int:
    query = _history_query(args)
    cursor = _history_cursor(args)
    paths = _history_paths(args)
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, _optional_watches(args))
        page = query_events(connection, query, cursor=cursor,
                            limit=args.limit, tag_snapshot=snapshot)
        batch = None
        if args.out:
            # The export reuses the exact same query code; keeping it here
            # (instead of re-serialising the page) preserves dedupe-on-append
            # and the "never truncate an existing ledger" guarantee.
            batch = export_jsonl(connection, query, args.out,
                                 cursor=cursor, page_size=args.limit,
                                 append=args.append, tag_snapshot=snapshot)
    next_cursor = batch.next_cursor if batch is not None else page.next_cursor
    if args.cursor_out:
        write_cursor_state(args.cursor_out, next_cursor, query,
                           exhausted=(batch.exhausted if batch is not None
                                      else page.exhausted),
                           has_more=(batch.has_more if batch is not None
                                     else page.has_more),
                           records=(batch.records if batch is not None
                                    else len(page.events)))
    if args.json:
        payload = page.as_dict()
        payload["next_cursor"] = next_cursor
        if batch is not None:
            payload["jsonl_path"] = str(batch.path)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for event in page.events:
            print(f"{utc_iso(event.created_at)} {event.watch_name} "
                  f"{event.severity} seq={event.id} "
                  f"+{event.added_lines}/-{event.removed_lines} "
                  f"{event.summary}")
        for degraded in page.degraded_watches:
            print(f"degraded: watch={degraded.name} state={degraded.state} "
                  f"source={degraded.source}", file=sys.stderr)
        print(f"events: {len(page.events)} watermark={page.watermark} "
              f"has_more={str(page.has_more).lower()} "
              f"exhausted={str(page.exhausted).lower()}")
        print(f"next_cursor: {next_cursor}")
        if args.out:
            print(f"jsonl: {args.out}")
        if args.cursor_out:
            print(f"cursor_file: {args.cursor_out}")
    return 0


def _cmd_export_history(args: argparse.Namespace) -> int:
    query = _history_query(args)
    cursor = _history_cursor(args)
    paths = _history_paths(args)
    with connect_readonly(paths.db_path) as connection:
        snapshot = load_tag_snapshot(connection, _optional_watches(args))
        batch = export_jsonl(connection, query, args.out, cursor=cursor,
                             page_size=args.limit, append=args.append,
                             tag_snapshot=snapshot)
    write_cursor_file(args.cursor_out, batch, query)
    if args.json:
        print(json.dumps(batch.as_dict(), ensure_ascii=False, indent=2,
                         sort_keys=True))
    else:
        if batch.cursor_only:
            print(f"exported 0 record(s); {batch.path} left unchanged")
        else:
            print(f"exported {batch.records} record(s) to {batch.path}")
        if batch.skipped_duplicates:
            print(f"skipped {batch.skipped_duplicates} duplicate(s) "
                  "already present in the file")
        if batch.degraded_watches:
            print("degraded: " + ", ".join(batch.degraded_watches),
                  file=sys.stderr)
        print(f"exhausted={str(batch.exhausted).lower()} "
              f"cursor_file={args.cursor_out}")
    return 0


def _history_paths(args: argparse.Namespace) -> DataPaths:
    paths = DataPaths(Path(args.data_dir))
    if not paths.db_path.exists():
        raise FileNotFoundError(f"database not found: {paths.db_path}")
    return paths


def _history_query(args: argparse.Namespace) -> HistoryQuery:
    since = parse_iso(args.since) if args.since else None
    until = parse_iso(args.until) if args.until else None
    try:
        return HistoryQuery(
            watch_names=tuple(args.name), tags=tuple(args.tag),
            severities=tuple(args.severity), since=since, until=until,
            tag_match=args.tag_match,
        )
    except HistoryError as exc:
        exc.detail.setdefault("filters", {
            "names": args.name, "tags": args.tag, "severities": args.severity,
            "since": args.since, "until": args.until})
        raise


def _history_cursor(args: argparse.Namespace) -> str | None:
    if args.cursor and args.cursor_file:
        raise ValueError("--cursor and --cursor-file are mutually exclusive")
    if args.cursor_file:
        return read_cursor_file(args.cursor_file)
    return args.cursor


def _optional_watches(args: argparse.Namespace) -> list | None:
    if not args.config:
        return None
    from .config import load_watchlist
    return load_watchlist(args.config)


def _cmd_demo(_: argparse.Namespace) -> int:
    from .demo import run_demo
    return run_demo()


def _cmd_history_acceptance(args: argparse.Namespace) -> int:
    from .history.acceptance import run_acceptance
    return run_acceptance(args.output_dir,
                          mock_writers=args.with_writer_stress)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except HistoryError as exc:
        # Machine-distinguishable taxonomy: each code has its own exit status
        # so a downstream puller can branch without parsing text.
        if getattr(args, "json", False):
            print(json.dumps(exc.as_dict(), ensure_ascii=False,
                             sort_keys=True), file=sys.stderr)
        else:
            print(f"error[{exc.code}]: {exc.message}", file=sys.stderr)
        return exc.exit_code
    except (ConfigError, FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
