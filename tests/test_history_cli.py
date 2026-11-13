"""CLI-level tests for ``history`` / ``export-history`` / acceptance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import pagewatch.cli as cli
from pagewatch.history.acceptance import run_acceptance
from pagewatch.storage.db import connect, init_db
from pagewatch.storage.events import insert_event


T0 = 1_767_225_600.0


@pytest.fixture
def seeded(tmp_path):
    path = tmp_path / "pagewatch.sqlite"
    init_db(path)
    with connect(path) as connection:
        ids = []
        for index, (name, severity) in enumerate(
                [("announcements", "major"), ("docs", "minor"),
                 ("pricing", "noise"), ("docs", "minor")]):
            event = insert_event(
                connection, watch_name=name, from_snapshot_id=1,
                to_snapshot_id=2, created_at=T0 + index, severity=severity,
                change_rate=0.2, added_lines=1, removed_lines=1,
                regions=(), keyword_hits=(), focus_hit=False,
                summary=f"{severity} change", diff_text="--- a\n+++ b\n")
            ids.append(int(event.id))
        connection.commit()
    return tmp_path, ids


def run(args):
    return cli.main(args)


def test_history_text_json_and_cursor_chain(seeded, capsys):
    data_dir, ids = seeded
    assert run(["history", "--data-dir", str(data_dir), "--limit", "1"]) == 0
    out = capsys.readouterr().out
    assert "events: 1" in out
    cursor = [line.split("next_cursor: ", 1)[1] for line in out.splitlines()
              if line.startswith("next_cursor: ")][0]
    assert run(["history", "--data-dir", str(data_dir), "--limit", "1",
                "--json", "--cursor", cursor]) == 0
    page = json.loads(capsys.readouterr().out)
    assert len(page["events"]) == 1
    assert page["has_more"] is True
    assert page["events"][0]["id"] == ids[1]


def test_history_filters_half_open_and_union(seeded, capsys):
    data_dir, _ = seeded
    until = T0 + 2
    assert run(["history", "--data-dir", str(data_dir), "--json",
                "--severity", "major", "--severity", "minor",
                "--since", str(T0), "--until", str(until),
                "--limit", "100"]) == 0
    page = json.loads(capsys.readouterr().out)
    assert {item["severity"] for item in page["events"]} == {"major", "minor"}
    assert all(item["created_at"] < until for item in page["events"])
    assert len(page["events"]) == 2


def test_history_cursor_errors_use_distinct_exit_codes(seeded, capsys):
    data_dir, _ = seeded
    assert run(["history", "--data-dir", str(data_dir), "--limit", "1"]) == 0
    cursor = [line.split("next_cursor: ", 1)[1] for line in
              capsys.readouterr().out.splitlines()
              if line.startswith("next_cursor: ")][0]
    assert run(["history", "--data-dir", str(data_dir), "--cursor",
                "garbage!!"]) == 3
    assert "cursor_malformed" in capsys.readouterr().err
    assert run(["history", "--data-dir", str(data_dir), "--cursor", cursor,
                "--name", "announcements"]) == 4
    assert "cursor_filter_mismatch" in capsys.readouterr().err
    assert run(["history", "--data-dir", str(data_dir), "--cursor", cursor,
                "--until", str(T0)]) == 5
    assert "cursor_out_of_range" in capsys.readouterr().err
    assert run(["history", "--data-dir", str(data_dir),
                "--since", str(T0), "--until", str(T0)]) == 2
    assert "invalid_range" in capsys.readouterr().err
    assert run(["history", "--data-dir", str(data_dir),
                "--severity", "bogus"]) == 2
    assert "invalid_filter" in capsys.readouterr().err


def test_export_history_two_consecutive_rounds(seeded, capsys):
    data_dir, ids = seeded
    ledger1 = data_dir / "round1.jsonl"
    cursor1 = data_dir / "round1.cursor.json"
    ledger2 = data_dir / "round2.jsonl"
    cursor2 = data_dir / "round2.cursor.json"
    args = ["export-history", "--data-dir", str(data_dir),
            "--limit", "2", "--json"]
    # Round 1: drain every page, appending to one ledger.
    assert run(args + ["--out", str(ledger1),
                       "--cursor-out", str(cursor1)]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["records"] == 2 and first["exhausted"] is False
    assert run(args + ["--out", str(ledger1), "--append",
                       "--cursor-file", str(cursor1),
                       "--cursor-out", str(cursor1)]) == 0
    second_page = json.loads(capsys.readouterr().out)
    assert second_page["records"] == 2 and second_page["exhausted"] is True
    assert json.loads(cursor1.read_text())["cursor"] == \
           second_page["next_cursor"]
    assert len(ledger1.read_text(encoding="utf-8").splitlines()) == 4

    # New events written between the two rounds must be the only round-2 rows.
    with connect(data_dir / "pagewatch.sqlite") as connection:
        later = insert_event(
            connection, watch_name="docs", from_snapshot_id=2,
            to_snapshot_id=3, created_at=T0 + 10, severity="major",
            change_rate=0.5, added_lines=2, removed_lines=0, regions=(),
            keyword_hits=(), focus_hit=True, summary="late", diff_text="")
        connection.commit()
    late_id = int(later.id)

    assert run(args + ["--out", str(ledger2),
                       "--cursor-file", str(cursor1),
                       "--cursor-out", str(cursor2)]) == 0
    third = json.loads(capsys.readouterr().out)
    assert third["records"] == 1
    assert third["exhausted"] is True
    records1 = [json.loads(line) for line in
                ledger1.read_text(encoding="utf-8").splitlines()]
    records2 = [json.loads(line) for line in
                ledger2.read_text(encoding="utf-8").splitlines()]
    keys = [(item["watch_name"], item["event_seq"])
            for item in records1 + records2]
    assert len(keys) == len(set(keys)) == 5
    assert [item["event_seq"] for item in records2] == [late_id]

    # Re-pulling the now-drained cursor is a new empty round: no ledger
    # truncation, no duplicate rows.
    assert run(args + ["--out", str(ledger2),
                       "--cursor-file", str(cursor2),
                       "--cursor-out", str(cursor2)]) == 0
    third = json.loads(capsys.readouterr().out)
    assert third["records"] == 0 and third["cursor_only"] is True
    assert len(ledger2.read_text(encoding="utf-8").splitlines()) == 1


def test_export_history_append_dedupes(seeded, capsys):
    data_dir, _ = seeded
    ledger = data_dir / "ledger.jsonl"
    assert run(["export-history", "--data-dir", str(data_dir), "--limit", "100",
                "--out", str(ledger), "--cursor-out",
                str(data_dir / "c1.json")]) == 0
    assert run(["export-history", "--data-dir", str(data_dir), "--limit", "100",
                "--append", "--out", str(ledger), "--cursor-out",
                str(data_dir / "c2.json")], ) == 0
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 4


def test_history_requires_existing_database(tmp_path):
    assert run(["history", "--data-dir", str(tmp_path / "missing")]) == 2


def test_history_and_export_reject_cursor_and_cursor_file_together(seeded):
    data_dir, _ = seeded
    assert run(["history", "--data-dir", str(data_dir), "--cursor", "x",
                "--cursor-file", str(data_dir / "c.json")]) == 2


def test_acceptance_scenario_passes_and_is_replayable(tmp_path, capsys):
    output = tmp_path / "acceptance"
    assert run_acceptance(output) == 0
    report = json.loads((output / "history-acceptance.json").read_text())
    assert report["ok"] is True
    names = {check["check"] for check in report["checks"]}
    for required in (
            "round2_continues_from_round1_cursor",
            "two_round_union_has_no_duplicates_and_no_omissions",
            "terminal_cursor_advances_without_replay",
            "old_cursor_advances_without_seeing_concurrent_writes",
            "concurrent_writes_appear_exactly_once_after_round",
            "tag_filter_returns_deleted_and_closed_history",
            "range_is_left_closed_right_open",
            "errors_are_distinguishable"):
        assert required in names
    round1 = (output / "ledger-round1.jsonl").read_bytes()
    second = tmp_path / "acceptance2"
    assert run_acceptance(second) == 0
    assert (second / "ledger-round1.jsonl").read_bytes() == round1
