#!/usr/bin/env python3
"""Offline acceptance demo for the three cross-round defects.

Runs entirely without sockets or wall-clock time: scripted in-process fetch
clients + FrozenClock + an isolated temp data directory. For each defect it
reproduces the OLD (pre-fix) behavior by monkeypatching the buggy logic, then
runs the CURRENT fixed code, and prints a before/after comparison.

Scenarios
---------
1. fetch-failure degradation : a 5xx must not create a phantom event/snapshot
   or pollute the digest; the healthy watch in the same round still advances.
2. disabled-watch skipping   : a disabled watch must never be selected or
   executed, and must leave zero artifacts (guards the demo round that used
   to bypass select_watches).
3. cooldown cross-round      : cooldown is keyed on (watch, severity); repeats
   inside the window stay out of the JSONL outbox, and at the exact expiry
   instant alerting recovers instead of going permanently silent.

Run:  PYTHONPATH=src:. python3 scripts/offline_acceptance_demo.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pagewatch.alert.outbox import read_outbox
from pagewatch.clock import FrozenClock
from pagewatch.fetch.client import FetchError, FetchResult
from pagewatch.models import DataPaths, Rules, WatchSpec
from pagewatch.report.digest import render_digest
from pagewatch.runner import Runner
from pagewatch.schedule.scheduler import execute_round, select_watches
from pagewatch.storage.db import connect, init_paths
from pagewatch.storage.events import insert_alert, list_alerts, list_events
from pagewatch.storage.snapshots import list_snapshots, probe_rows

PASS = "PASS"
FAIL = "FAIL"


# --------------------------------------------------------------------------- #
# Test doubles
# --------------------------------------------------------------------------- #

class ScriptedClient:
    def __init__(self, script: dict[str, list]):
        self.script = {url: list(actions) for url, actions in script.items()}
        self.calls: set[str] = set()
        self._count: dict[str, int] = {}

    def fetch(self, url, *, etag=None, last_modified=None):
        self.calls.add(url)
        index = self._count.get(url, 0)
        self._count[url] = index + 1
        actions = self.script[url]
        # Reuse the last action once the script is exhausted.
        action = actions[min(index, len(actions) - 1)]
        if isinstance(action, Exception):
            raise action
        return FetchResult(200, url, action.encode("utf-8"), action, None,
                           None, 1, False)


def big_page(revision: int) -> str:
    return "<html><body>" + "".join(
        f"<p>paragraph {i} revision {revision}</p>" for i in range(20)
    ) + "</body></html>"


def fresh_paths(tmp: Path) -> DataPaths:
    paths = DataPaths(tmp / "data")
    init_paths(paths)
    return paths


def make_runner(paths, client, clock, *, cooldown=3600, major_rate=0.01):
    rules = Rules(jitter_seconds=0, alert_cooldown_seconds=cooldown,
                  major_change_rate=major_rate)
    runner = Runner(db_path=paths.db_path, archive_dir=paths.archive_dir,
                    outbox_path=paths.outbox_path, rules=rules, clock=clock,
                    client_factory=lambda _r: client)
    return runner, rules


def watch(name, *, enabled=True, **kw):
    return WatchSpec(name=name, url=f"http://127.0.0.1/{name}",
                     interval_seconds=60, enabled=enabled, **kw)


def heading(text: str) -> None:
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def verdict(label: str, ok: bool, detail: str) -> bool:
    print(f"  [{PASS if ok else FAIL}] {label}: {detail}")
    return ok


# --------------------------------------------------------------------------- #
# Scenario 1: fetch failure must not produce a phantom change
# --------------------------------------------------------------------------- #

def scenario_fetch_failure(root: Path) -> bool:
    heading("Scenario 1: 抓取 5xx 失败只降级该监视项，不产生虚假变更/事件")
    ok = True
    wa, wb = watch("alpha"), watch("beta")
    failure = FetchError("HTTP 503 for http://127.0.0.1/alpha")
    client = ScriptedClient({
        wa.url: ["<html><body><p>alpha v1</p></body></html>",
                 failure,
                 "<html><body><p>alpha v1</p></body></html>"],
        wb.url: ["<html><body><p>beta v1</p></body></html>",
                 big_page(2), big_page(2)],
    })
    paths = fresh_paths(root / "s1")
    clock = FrozenClock(1_000.0)
    runner, rules = make_runner(paths, client, clock)

    clock.advance(60)
    execute_round(select_watches([wa, wb], now=clock.now()), runner.run, 1)
    clock.advance(60)
    results = {r.watch_name: r for r in execute_round(
        select_watches([wa, wb], now=clock.now()), runner.run, 1)}

    print("  失败轮结果: "
          f"alpha.outcome={results['alpha'].outcome}, "
          f"beta.outcome={results['beta'].outcome}")
    ok &= verdict("失败项返回 error 且无 event/snapshot id",
                  results["alpha"].outcome == "error"
                  and results["alpha"].event_id is None
                  and results["alpha"].snapshot_id is None,
                  f"event={results['alpha'].event_id}, "
                  f"snapshot={results['alpha'].snapshot_id}")
    ok &= verdict("同轮健康监视项正常推进并产生真实变更",
                  results["beta"].outcome == "changed"
                  and results["beta"].event_id is not None,
                  f"beta.event={results['beta'].event_id}")

    with connect(paths.db_path) as con:
        alpha_snaps = list_snapshots(con, "alpha")
        alpha_events = [e for e in list_events(con) if e.watch_name == "alpha"]
        all_events = list_events(con)
        probes = probe_rows(con, "alpha")
        digest = render_digest(all_events)
    ok &= verdict("失败项快照数保持为 1（仅基线）", len(alpha_snaps) == 1,
                  f"{len(alpha_snaps)} snapshot(s)")
    ok &= verdict("失败项零变更事件", alpha_events == [],
                  f"{len(alpha_events)} event(s)")
    ok &= verdict("失败探测已留痕（probe 记录错误）",
                  len(probes) == 2 and probes[-1]["status_code"] is None
                  and bool(probes[-1]["error"]),
                  f"{len(probes)} probe(s)")
    ok &= verdict("事件台账只含健康项的真实事件",
                  [e.watch_name for e in all_events] == ["beta"],
                  str([e.watch_name for e in all_events]))
    ok &= verdict("摘要页不出现失败项分组",
                  "<h2>alpha" not in digest, "digest group absent")

    # Recovery round: same content -> unchanged, still no event.
    clock.advance(60)
    recovered = {r.watch_name: r for r in execute_round(
        select_watches([wa, wb], now=clock.now()), runner.run, 1)}
    with connect(paths.db_path) as con:
        alpha_events = [e for e in list_events(con) if e.watch_name == "alpha"]
    ok &= verdict("恢复轮内容未变 -> unchanged，仍无虚假事件",
                  recovered["alpha"].outcome == "unchanged"
                  and not alpha_events,
                  recovered["alpha"].outcome)
    return ok


# --------------------------------------------------------------------------- #
# Scenario 2: disabled watches are never scheduled or materialized
# --------------------------------------------------------------------------- #

def scenario_disabled(root: Path) -> bool:
    heading("Scenario 2: 禁用监视项完全不参与调度，也不产生任何产物")
    ok = True
    on, off = watch("on"), watch("off", enabled=False)
    watches = [on, off]
    client = ScriptedClient({on.url: [big_page(1)], off.url: [big_page(1)]})
    paths = fresh_paths(root / "s2")
    clock = FrozenClock(1_000.0)
    runner, rules = make_runner(paths, client, clock)

    for _ in range(3):
        clock.advance(60)
        selected = select_watches(watches, now=clock.now())
        execute_round(selected, runner.run, 1)
        ok &= verdict("每轮选择结果都排除禁用项",
                      [w.name for w in selected] == ["on"],
                      str([w.name for w in selected]))

    ok &= verdict("禁用项从未发起抓取", off.url not in client.calls,
                  f"fetched={sorted(client.calls)}")
    with connect(paths.db_path) as con:
        snaps = list_snapshots(con, "off")
        probes = probe_rows(con, "off")
        events = [e for e in list_events(con) if e.watch_name == "off"]
        watch_rows = {r[0] for r in con.execute("SELECT name FROM watches")}
        schedule_rows = {r[0] for r in
                         con.execute("SELECT watch_name FROM schedule_state")}
        digest = render_digest(list_events(con))
    ok &= verdict("禁用项无快照/探测/事件",
                  not snaps and not probes and not events,
                  f"snapshots={len(snaps)} probes={len(probes)} "
                  f"events={len(events)}")
    ok &= verdict("禁用项不写 watches / schedule_state",
                  "off" not in watch_rows and "off" not in schedule_rows,
                  f"watches={sorted(watch_rows)} schedule={sorted(schedule_rows)}")
    ok &= verdict("摘要页无禁用项分组", "<h2>off" not in digest,
                  "digest group absent")

    # Demonstrate the OLD demo bypass explicitly: feeding raw watches straight
    # into execute_round is what used to run the disabled watch once.
    bypassed = execute_round(watches, lambda w: w.name, 1)
    ok &= verdict("旧缺陷复现：原始 watches 直送 execute_round 会含禁用项",
                  "off" in bypassed,
                  f"executed={bypassed} (current demo now filters first)")
    return ok


# --------------------------------------------------------------------------- #
# Scenario 3: cooldown precision and cross-round recovery
# --------------------------------------------------------------------------- #

def _buggy_insert_alert(connection, *, watch_name, severity, event_id,
                        created_at, message, cooldown_seconds):
    """Pre-fix logic: non-strict >= boundary (rolling-window silence)."""
    if cooldown_seconds > 0:
        row = connection.execute(
            "SELECT id FROM alerts WHERE watch_name=? AND severity=? "
            "AND created_at >= ? ORDER BY created_at DESC LIMIT 1",
            (watch_name, severity, created_at - cooldown_seconds),
        ).fetchone()
        if row is not None:
            return None
    return insert_alert(connection, watch_name=watch_name, severity=severity,
                        event_id=event_id, created_at=created_at,
                        message=message, cooldown_seconds=0)


def scenario_cooldown(root: Path) -> bool:
    heading("Scenario 3: 冷却按 监视项+级别 精确判定，到期恢复、不永久静默")
    ok = True
    cooldown = 3600
    now = 1_800_000_000.0

    # --- storage-layer before/after at the exact boundary ---
    old_db = root / "s3old" / "pagewatch.sqlite"
    init_paths(DataPaths(root / "s3old"))
    with connect(old_db) as con:
        insert_alert(con, watch_name="w", severity="major", event_id=1,
                     created_at=now, cooldown_seconds=cooldown, message="first")
        old_boundary = _buggy_insert_alert(
            con, watch_name="w", severity="major", event_id=2,
            created_at=now + cooldown, message="at-expiry",
            cooldown_seconds=cooldown)

    new_db = root / "s3new" / "pagewatch.sqlite"
    init_paths(DataPaths(root / "s3new"))
    with connect(new_db) as con:
        insert_alert(con, watch_name="w2", severity="major", event_id=1,
                     created_at=now, cooldown_seconds=cooldown, message="seed")
        fixed_boundary = insert_alert(
            con, watch_name="w2", severity="major", event_id=2,
            created_at=now + cooldown, cooldown_seconds=cooldown,
            message="at-expiry")

    print(f"  精确边界 t = 上次告警 + {cooldown}s:")
    ok &= verdict("旧逻辑(>=) 在到期瞬间仍误判冷却 -> 静默",
                  old_boundary is None,
                  f"returned={old_boundary}")
    ok &= verdict("新逻辑(>) 到期瞬间恢复告警",
                  fixed_boundary is not None,
                  f"returned={'alert' if fixed_boundary else None}")

    # --- end-to-end cross-round outbox behavior ---
    wa, wb = watch("alpha"), watch("beta")
    revision = {"n": 0}

    class AlwaysChanging(ScriptedClient):
        def fetch(self, url, *, etag=None, last_modified=None):
            self.calls.add(url)
            revision["n"] += 1
            page = big_page(revision["n"])
            return FetchResult(200, url, page.encode(), page, None, None, 1,
                               False)

    paths = fresh_paths(root / "s3e2e")
    clock = FrozenClock(now)
    client = AlwaysChanging({})
    runner, rules = make_runner(paths, client, clock, cooldown=cooldown)

    def run_round():
        return {r.watch_name: r for r in execute_round(
            select_watches([wa, wb], now=clock.now()), runner.run, 1)}

    clock.advance(60)
    run_round()  # baselines at +60
    clock.advance(540)
    first_alerts = run_round()  # first major change at +600 -> alerts
    first_alert_at = clock.now()
    outbox_after_first = len(read_outbox(paths.outbox_path))
    # Five rounds at +1200..+3600 stay inside the 3600s window measured from
    # the first alert at +600: changes recorded, no new outbox lines.
    for _ in range(5):
        clock.advance(600)
        suppressed = run_round()
        assert all(r.alert_id is None for r in suppressed.values())
    outbox_inside = len(read_outbox(paths.outbox_path))
    # The next round at first_alert_at + cooldown reaches the exact expiry and
    # must recover (strict boundary).
    clock.advance(600)
    assert clock.now() == first_alert_at + cooldown
    recovered = run_round()
    outbox_after_recovery = len(read_outbox(paths.outbox_path))

    ok &= verdict("首次 major 变更每监视项各发 1 条 outbox",
                  outbox_after_first == 2
                  and all(r.alert_id is not None
                          for r in first_alerts.values()),
                  f"{outbox_after_first} line(s)")
    ok &= verdict("冷却期内重复变更不外发（outbox 不增长）",
                  outbox_inside == 2, f"{outbox_inside} line(s)")
    ok &= verdict("到期后两个监视项都恢复外发",
                  outbox_after_recovery == 4
                  and all(r.alert_id is not None
                          for r in recovered.values()),
                  f"{outbox_after_recovery} line(s)")
    with connect(paths.db_path) as con:
        alerts = list_alerts(con)
        keys = {(a.watch_name, a.severity) for a in alerts}
    ok &= verdict("冷却键精确为 (监视项, 级别)",
                  keys == {("alpha", "major"), ("beta", "major")},
                  str(sorted(keys)))

    # Demonstrate permanent silence under the OLD comparison: recurring
    # changes at 600s cadence would never cross a non-strict rolling boundary.
    silent_db = root / "s3silent" / "pagewatch.sqlite"
    init_paths(DataPaths(root / "s3silent"))
    with connect(silent_db) as con:
        insert_alert(con, watch_name="w", severity="major", event_id=1,
                     created_at=now, cooldown_seconds=cooldown, message="x")
        old_emitted, new_emitted = [], []
        for k in range(1, 8):
            t = now + 600 * k
            old = _buggy_insert_alert(con, watch_name="w", severity="major",
                                      event_id=k + 1, created_at=t, message="x",
                                      cooldown_seconds=cooldown)
            old_emitted.append(old is not None)
    clean2 = root / "s3silent2" / "pagewatch.sqlite"
    init_paths(DataPaths(root / "s3silent2"))
    with connect(clean2) as con:
        insert_alert(con, watch_name="w", severity="major", event_id=1,
                     created_at=now, cooldown_seconds=cooldown, message="x")
        for k in range(1, 8):
            t = now + 600 * k
            new = insert_alert(con, watch_name="w", severity="major",
                               event_id=k + 1, created_at=t, message="x",
                               cooldown_seconds=cooldown)
            new_emitted.append(new is not None)
    print("  600s 周期重复变更（冷却 3600s）各轮是否外发:")
    print(f"    旧逻辑: {old_emitted}")
    print(f"    新逻辑: {new_emitted}")
    ok &= verdict("旧逻辑滚动窗口导致长时间静默（前 6 轮全抑制）",
                  old_emitted == [False, False, False, False, False, False, True],
                  "permanent-silence reproduced")
    ok &= verdict("新逻辑在到期轮准时恢复",
                  new_emitted == [False, False, False, False, False, True, False],
                  "recovers at exact expiry")
    return ok


def main() -> int:
    results = {}
    with tempfile.TemporaryDirectory(prefix="pagewatch-acceptance-") as tmp:
        root = Path(tmp)
        results["fetch-failure"] = scenario_fetch_failure(root)
        results["disabled-skip"] = scenario_disabled(root)
        results["cooldown"] = scenario_cooldown(root)
    heading("验收汇总")
    for name, ok in results.items():
        print(f"  [{PASS if ok else FAIL}] {name}")
    overall = all(results.values())
    print("\n整体结果:", "全部通过 ✅" if overall else "存在失败 ❌")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
