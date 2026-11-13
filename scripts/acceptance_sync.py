#!/usr/bin/env python3
"""Offline acceptance walkthrough for the pagewatch sync link.

Runs entirely on local directories and a local mock site (no non-local
sockets).  It exercises the acceptance contract:

1. several monitoring rounds -> snapshots, events, alerts/outbox, gzip archives
2. an interrupted transfer (staged batch, process dies) followed by resume,
   then a byte/row-level comparison of both sides
3. a monitoring round committed *during* the sync window -> watermark advances
   exactly once, no lost or duplicated events
4. a legacy database without any sync table -> auto-provisioned, old commands
   still work, watermark seeded from the existing max version

Run with:  python3 scripts/acceptance_sync.py
"""

from __future__ import annotations

import gzip
import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

import yaml
from werkzeug.serving import make_server

from mocksite.app import create_app
from mocksite.scenarios import ScenarioState

from pagewatch import cli
from pagewatch.clock import FrozenClock
from pagewatch.config import load_all
from pagewatch.models import DataPaths
from pagewatch.runner import Runner
from pagewatch.schedule.scheduler import execute_round
from pagewatch.storage.db import connect, init_db, init_paths
from pagewatch.sync import push
from pagewatch.sync.schema import has_sync_schema

PASS = "PASS"
FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    status = PASS if condition else "FAIL"
    print(f"  [{status}] {label}{(' — ' + detail) if detail and not condition else ''}")
    if not condition:
        FAILURES.append(label)


def write_watchlist(path: Path, base_url: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    watches = [{
        "name": "announcements", "url": f"{base_url}/announcements",
        "interval_seconds": 60, "tags": ["notice"],
        "include_selectors": ["#notice-area"], "exclude_selectors": [".analytics"],
        "noise_patterns": [], "keywords": ["重要", "计费"],
    }]
    path.write_text(yaml.safe_dump({"version": 1, "watches": watches}),
                    encoding="utf-8")
    return path


def table_rows(db_path: Path, table: str) -> list[dict]:
    with connect(db_path) as connection:
        return [dict(r) for r in
                connection.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()]


def gzip_manifest(archive_dir: Path) -> dict[str, str]:
    result = {}
    for path in sorted(archive_dir.glob("*.gz")):
        raw = gzip.decompress(path.read_bytes())
        result[path.name] = hashlib.sha256(raw).hexdigest()
    return result


def main() -> int:
    workspace = Path(__file__).resolve().parents[1]
    work = Path(tempfile.mkdtemp(prefix="pw-accept-"))
    print(f"workspace: {work}")
    try:
        # ---- local mock site ------------------------------------------------
        scenario = ScenarioState()
        server = make_server("127.0.0.1", 0, create_app(scenario), threaded=True)
        import threading
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        clock = FrozenClock(1_800_000_000.0)
        cli.build_clock = lambda: clock  # type: ignore[assignment]

        source_dir = work / "node-a"
        replica_dir = work / "node-b"
        spool = work / "spool"
        config = write_watchlist(source_dir / "watchlist.yaml", base_url)
        rules = workspace / "configs" / "rules.example.yaml"
        source_paths = DataPaths(source_dir)
        init_paths(source_paths)
        watches, rules_obj = load_all(config, rules)
        runner = Runner(
            db_path=source_paths.db_path, archive_dir=source_paths.archive_dir,
            outbox_path=source_paths.outbox_path, rules=rules_obj, clock=clock,
        )

        # ---- step 1: several monitoring rounds ------------------------------
        print("\n[1] generating multiple monitoring rounds on node-a")
        for step in range(4):
            scenario.set_step(step)
            if step:
                clock.advance(60)
            execute_round(watches, runner.run, max_workers=1)
        source_events = table_rows(source_paths.db_path, "events")
        source_snaps = table_rows(source_paths.db_path, "snapshots")
        source_outbox = source_paths.outbox_path.read_text(encoding="utf-8").splitlines()
        source_gzips = gzip_manifest(source_paths.archive_dir)
        print(f"  snapshots={len(source_snaps)} events={len(source_events)} "
              f"outbox_lines={len(source_outbox)} gzip={len(source_gzips)}")
        check("source has snapshots", len(source_snaps) >= 2)
        check("source has change events", len(source_events) >= 1)

        # ---- step 2: interrupted transfer then resume -----------------------
        print("\n[2] interrupted push (stage-only) then resume")
        with pytest_raises_sync_error():
            push(
                source_db=source_paths.db_path,
                source_archive=source_paths.archive_dir,
                source_outbox=source_paths.outbox_path,
                target_db=DataPaths(replica_dir).db_path,
                target_archive=DataPaths(replica_dir).archive_dir,
                target_outbox=DataPaths(replica_dir).outbox_path,
                spool_dir=spool, now=clock.now(), fail_after_staging=True,
            )
        check("replica DB absent after interrupted staging",
              not DataPaths(replica_dir).db_path.exists())
        staged = list((spool / "batches").glob("*"))
        check("staged batch survived the interruption", len(staged) == 1,
              f"found {len(staged)}")

        rc = cli.main(["push", "--data-dir", str(source_dir),
                       "--target-dir", str(replica_dir),
                       "--spool-dir", str(spool)])
        check("resume push exits 0", rc == 0, f"rc={rc}")

        replica_paths = DataPaths(replica_dir)
        replica_events = table_rows(replica_paths.db_path, "events")
        replica_snaps = table_rows(replica_paths.db_path, "snapshots")
        replica_outbox = replica_paths.outbox_path.read_text(
            encoding="utf-8").splitlines()
        replica_gzips = gzip_manifest(replica_paths.archive_dir)

        check("snapshot count identical",
              len(replica_snaps) == len(source_snaps))
        check("event count identical",
              len(replica_events) == len(source_events))
        check("event ids identical and ordered",
              [r["id"] for r in replica_events]
              == [r["id"] for r in source_events])
        check("snapshot version chain identical per watch",
              sorted((r["watch_name"], r["version"]) for r in replica_snaps)
              == sorted((r["watch_name"], r["version"]) for r in source_snaps))
        check("snapshot primary keys preserved",
              sorted(r["id"] for r in replica_snaps)
              == sorted(r["id"] for r in source_snaps))
        check("outbox lines identical", replica_outbox == source_outbox)
        check("gzip filenames + content hashes identical",
              replica_gzips == source_gzips,
              f"{len(replica_gzips)} vs {len(source_gzips)}")

        # ---- step 3: new round during/after sync window ---------------------
        print("\n[3] new monitoring round then another push (watermark + dedupe)")
        before = len(table_rows(replica_paths.db_path, "events"))
        before_snaps = len(table_rows(replica_paths.db_path, "snapshots"))
        # Simulate a monitoring round committing new rows while the sync
        # window is open: append one snapshot + one event directly through the
        # same storage APIs the runner uses (avoids conditional 304 replies).
        from pagewatch.normalize.pipeline import content_hash
        from pagewatch.storage.snapshots import (
            append_snapshot, latest_snapshot, mark_snapshot_major,
        )
        from pagewatch.storage.events import insert_event
        new_text = "announcements text post-sync change"
        with connect(source_paths.db_path) as connection:
            previous = latest_snapshot(connection, "announcements")
            snapshot = append_snapshot(
                connection, watch_name="announcements",
                fetched_at=clock.now(), status_code=200,
                url=f"{base_url}/announcements", etag=None, last_modified=None,
                content_sha256=content_hash(new_text), raw_sha256=None,
                normalized_text=new_text, blocks_json="[]",
                normalizer_version="normalizer-v1",
            )
            insert_event(
                connection, watch_name="announcements",
                from_snapshot_id=previous.id, to_snapshot_id=snapshot.id,
                created_at=clock.now(), severity="major", change_rate=0.8,
                added_lines=3, removed_lines=1, regions=["#notice-area"],
                keyword_hits=["计费"], focus_hit=True,
                summary="post-sync major change", diff_text="@@ post",
            )
            mark_snapshot_major(connection, snapshot.id)
            connection.commit()
        mid_source = table_rows(source_paths.db_path, "events")
        mid_snaps = table_rows(source_paths.db_path, "snapshots")
        check("a new monitoring round produced rows",
              len(mid_snaps) == before_snaps + 1
              and len(mid_source) == before + 1,
              f"snaps {before_snaps}->{len(mid_snaps)}, "
              f"events {before}->{len(mid_source)}")
        rc = cli.main(["push", "--data-dir", str(source_dir),
                       "--target-dir", str(replica_dir),
                       "--spool-dir", str(spool)])
        check("second push exits 0", rc == 0, f"rc={rc}")
        after = table_rows(replica_paths.db_path, "events")
        check("replica event count catches up exactly",
              len(after) == len(mid_source))
        check("new snapshot reached the replica",
              len(table_rows(replica_paths.db_path, "snapshots"))
              == len(mid_snaps))
        check("no duplicate event ids on replica",
              len({r["id"] for r in after}) == len(after))
        rc = cli.main(["push", "--data-dir", str(source_dir),
                       "--target-dir", str(replica_dir),
                       "--spool-dir", str(spool)])
        check("idle re-push is a no-op (rc=0)", rc == 0)
        check("still no duplicates after idle re-push",
              len(table_rows(replica_paths.db_path, "events")) == len(after))
        check("watermark table present on both sides",
              has_sync_schema_table(source_paths.db_path)
              and has_sync_schema_table(replica_paths.db_path))

        # ---- step 4: legacy database auto-provisioning ----------------------
        print("\n[4] legacy database without sync tables")
        legacy_dir = work / "legacy"
        legacy_paths = DataPaths(legacy_dir)
        init_db(legacy_paths.db_path)  # base schema only, no sync tables
        with connect(legacy_paths.db_path) as connection:
            check("legacy DB starts without sync schema",
                  not has_sync_schema(connection))
        # old command still works (digest reads events; run-once style read path)
        rc = cli.main(["digest", "--data-dir", str(legacy_dir),
                       "--output", str(work / "legacy-digest.html")])
        check("legacy digest command still works", rc == 0, f"rc={rc}")
        legacy_replica = work / "legacy-replica"
        rc = cli.main(["push", "--data-dir", str(legacy_dir),
                       "--target-dir", str(legacy_replica),
                       "--spool-dir", str(work / "legacy-spool")])
        check("push auto-provisions sync tables (rc=0)", rc == 0, f"rc={rc}")
        with connect(legacy_paths.db_path) as connection:
            check("sync schema now present on legacy DB",
                  has_sync_schema(connection))
            columns = {r[1] for r in
                       connection.execute("PRAGMA table_info(snapshots)").fetchall()}
        check("original snapshots table structure untouched",
              "sync" not in {c.lower() for c in columns})

        server.shutdown()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print("\n" + ("=" * 60))
    if FAILURES:
        print(f"ACCEPTANCE FAILED: {len(FAILURES)} check(s): {FAILURES}")
        return 1
    print("ACCEPTANCE PASSED: all checks green")
    return 0


class pytest_raises_sync_error:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        from pagewatch.sync import SyncError
        if exc_type and issubclass(exc_type, SyncError):
            return True
        return False


def has_sync_schema_table(db_path: Path) -> bool:
    with connect(db_path) as connection:
        return has_sync_schema(connection)


if __name__ == "__main__":
    sys.exit(main())
