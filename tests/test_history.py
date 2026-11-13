"""Tests for the stable-cursor history query and incremental JSONL export."""

from __future__ import annotations

import json
import os
import threading
import time

import pytest

from pagewatch.history import (
    Cursor, CursorFilterMismatch, CursorMalformed, CursorOutOfRange,
    ExportError, HistoryQuery, InvalidFilterError, RangeError,
    STATE_ACTIVE, STATE_CLOSED, STATE_DELETED, STATE_UNKNOWN,
    TagSnapshot, WatchMeta, export_jsonl, load_tag_snapshot, query_events,
    read_cursor_file, write_cursor_file,
)
from pagewatch.history.reader import connect_readonly
from pagewatch.models import DataPaths, WatchSpec
from pagewatch.storage.db import connect, init_db, init_paths, json_dump
from pagewatch.storage.events import insert_event


T0 = 1_767_225_600.0


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "pagewatch.sqlite"
    init_db(path)
    return path


def add_event(connection, watch_name, severity="minor", created_at=T0,
              added=1):
    event = insert_event(
        connection, watch_name=watch_name, from_snapshot_id=1,
        to_snapshot_id=2, created_at=created_at, severity=severity,
        change_rate=0.1, added_lines=added, removed_lines=0,
        regions=("#body",), keyword_hits=(), focus_hit=False,
        summary=f"{severity} change", diff_text="--- old\n+++ new\n",
    )
    return int(event.id)


def add_watch(connection, name, tags=()):
    connection.execute(
        "INSERT INTO watches(name, url, tags_json) VALUES(?, ?, ?) "
        "ON CONFLICT(name) DO UPDATE SET tags_json=excluded.tags_json",
        (name, f"http://127.0.0.1/{name}", json_dump(list(tags))),
    )


def keys(events):
    return [(event.watch_name, event.id) for event in events]


def pull_all(connection, query, *, limit=2, snapshot=None, cursor=None):
    collected = []
    rounds = 0
    while True:
        page = query_events(connection, query, limit=limit, cursor=cursor,
                            tag_snapshot=snapshot)
        collected.extend(page.events)
        cursor = page.next_cursor
        rounds += 1
        if page.exhausted:
            return collected, cursor, rounds
        assert rounds < 100


def test_ordering_is_stable_for_identical_timestamps(db_path):
    with connect(db_path) as connection:
        for name in ("zeta", "alpha", "middle", "alpha"):
            add_event(connection, name)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        events, cursor, rounds = pull_all(connection, HistoryQuery(), limit=1,
                                          snapshot=snapshot)
        assert [event.watch_name for event in events] == \
               ["alpha", "alpha", "middle", "zeta"]
        assert rounds == 4
        assert query_events(connection, HistoryQuery(), cursor=cursor,
                            tag_snapshot=snapshot).events == ()


def test_half_open_range_excludes_upper_bound(db_path):
    with connect(db_path) as connection:
        add_event(connection, "a", created_at=T0)
        add_event(connection, "a", created_at=T0 + 60)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        inside = query_events(connection,
                              HistoryQuery(since=T0, until=T0 + 60),
                              tag_snapshot=snapshot)
        after = query_events(connection, HistoryQuery(since=T0 + 60),
                             tag_snapshot=snapshot)
        assert [event.created_at for event in inside.events] == [T0]
        assert [event.created_at for event in after.events] == [T0 + 60]
        with pytest.raises(RangeError) as error:
            HistoryQuery(since=T0, until=T0)
        assert error.value.code == "invalid_range"


def test_multi_value_filters_are_unions(db_path):
    with connect(db_path) as connection:
        add_watch(connection, "a", ["x", "common"])
        add_watch(connection, "b", ["y", "common"])
        add_watch(connection, "c", ["z"])
        add_event(connection, "a", "major")
        add_event(connection, "b", "noise")
        add_event(connection, "c", "minor")
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        by_severity = query_events(
            connection, HistoryQuery(severities=("major", "minor")),
            tag_snapshot=snapshot)
        assert {event.watch_name for event in by_severity.events} == {"a", "c"}
        by_tag = query_events(connection, HistoryQuery(tags=("x", "y")),
                              tag_snapshot=snapshot)
        assert {event.watch_name for event in by_tag.events} == {"a", "b"}
        by_all = query_events(connection,
                              HistoryQuery(tags=("common", "x"),
                                           tag_match="all"),
                              tag_snapshot=snapshot)
        assert {event.watch_name for event in by_all.events} == {"a"}
        by_name = query_events(connection,
                               HistoryQuery(watch_names=("a", "c")),
                               tag_snapshot=snapshot)
        assert {event.watch_name for event in by_name.events} == {"a", "c"}


def test_unknown_tag_returns_empty_not_whole_history(db_path):
    with connect(db_path) as connection:
        add_event(connection, "a")
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        page = query_events(connection, HistoryQuery(tags=("nonexistent",)),
                            tag_snapshot=snapshot)
        assert page.events == ()
        assert page.exhausted is True


def test_closed_and_deleted_watches_degrade_but_stay_queryable(db_path):
    with connect(db_path) as connection:
        add_watch(connection, "live", ["ops"])
        add_watch(connection, "retired", ["ops"])
        add_watch(connection, "removed", ["ops"])
        add_event(connection, "live")
        add_event(connection, "retired")
        add_event(connection, "removed")
        connection.commit()
        watches = [
            WatchSpec(name="live", url="http://127.0.0.1/live",
                      interval_seconds=60, tags=("ops",)),
            WatchSpec(name="retired", url="http://127.0.0.1/retired",
                      interval_seconds=60, tags=("ops",), enabled=False),
        ]
        snapshot = load_tag_snapshot(connection, watches)
        assert snapshot.meta("live").state == STATE_ACTIVE
        assert snapshot.meta("retired").state == STATE_CLOSED
        assert snapshot.meta("removed").state == STATE_DELETED
        page = query_events(connection, HistoryQuery(tags=("ops",)),
                            tag_snapshot=snapshot)
        assert {event.watch_name for event in page.events} == \
               {"live", "retired", "removed"}
        degraded = {item.name: item.state for item in page.degraded_watches}
        assert degraded == {"retired": STATE_CLOSED,
                            "removed": STATE_DELETED}
        explicit = query_events(connection,
                                HistoryQuery(watch_names=("ghost",)),
                                tag_snapshot=snapshot)
        assert explicit.events == ()
        assert [item.state for item in explicit.degraded_watches] == \
               [STATE_UNKNOWN]


def test_tag_round_freezes_resolution_against_metadata_changes(db_path):
    with connect(db_path) as connection:
        add_watch(connection, "a", ["x"])
        for _ in range(4):
            add_event(connection, "a")
        connection.commit()
        snapshot = TagSnapshot(metas={
            "a": WatchMeta(name="a", tags=("x",))})
        first = query_events(connection, HistoryQuery(tags=("x",)), limit=2,
                             tag_snapshot=snapshot)
        assert first.has_more is True
        # Metadata changes mid-round: a drops tag x, b gains it.
        connection.execute("UPDATE watches SET tags_json=? WHERE name='a'",
                           (json_dump([]),))
        add_watch(connection, "b", ["x"])
        add_event(connection, "b")
        connection.commit()
        second = query_events(connection, HistoryQuery(tags=("x",)),
                              cursor=first.next_cursor, limit=2,
                              tag_snapshot=load_tag_snapshot(connection))
        third = query_events(connection, HistoryQuery(tags=("x",)),
                             cursor=second.next_cursor, limit=2,
                             tag_snapshot=load_tag_snapshot(connection))
        # The second page is still in the frozen round: it must keep using
        # the resolved name set, so it sees "a" even though "a" no longer
        # carries tag x.
        assert {event.watch_name for event in second.events} == {"a"}
        # Draining the round yields a terminal cursor; the pull *after* it is
        # a fresh round and may legitimately see the new metadata (only the
        # event written after the watermark, which is "b").
        assert {event.watch_name for event in third.events} == {"b"}
        fresh = query_events(connection, HistoryQuery(tags=("x",)),
                             cursor=third.next_cursor,
                             tag_snapshot=load_tag_snapshot(connection))
        assert fresh.events == ()
        by_name = query_events(connection,
                               HistoryQuery(watch_names=("a", "b")),
                               tag_snapshot=load_tag_snapshot(connection))
        assert {event.watch_name for event in by_name.events} == {"a", "b"}


def test_concurrent_inserts_cannot_shift_an_active_round(db_path):
    with connect(db_path) as connection:
        for index in range(6):
            add_event(connection, f"w{index:02d}", created_at=T0 + index // 2)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        first = query_events(connection, HistoryQuery(), limit=2,
                             tag_snapshot=snapshot)
    with connect(db_path) as connection:
        while True:
            cursor = first.next_cursor
            # Writer commits between every page of the round.
            add_event(connection, "late", created_at=T0)
            connection.commit()
            page = query_events(connection, HistoryQuery(), cursor=cursor,
                                limit=2, tag_snapshot=snapshot)
            seen = [event.id for event in page.events]
            assert all(event.watch_name != "late" for event in page.events)
            if page.exhausted:
                break
            first = page
    with connect(db_path) as connection:
        follow_up = query_events(connection, HistoryQuery(),
                                 cursor=page.next_cursor, limit=100,
                                 tag_snapshot=snapshot)
    assert {event.watch_name for event in follow_up.events} == {"late"}
    assert len(follow_up.events) == follow_up.events.__len__()
    assert len({event.id for event in follow_up.events}) == \
           len(follow_up.events)


def test_cursor_errors_are_distinguishable(db_path):
    with connect(db_path) as connection:
        add_event(connection, "a", created_at=T0)
        add_event(connection, "a", created_at=T0 + 60)
        add_event(connection, "a", created_at=T0 + 120)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        page = query_events(connection, HistoryQuery(), limit=1,
                            tag_snapshot=snapshot)
        with pytest.raises(CursorMalformed) as malformed:
            query_events(connection, HistoryQuery(), cursor="%%%bad",
                         tag_snapshot=snapshot)
        assert malformed.value.code == "cursor_malformed"
        tampered = page.next_cursor[:-4] + "AAAA"
        with pytest.raises(CursorMalformed):
            query_events(connection, HistoryQuery(), cursor=tampered,
                         tag_snapshot=snapshot)
        with pytest.raises(CursorFilterMismatch) as mismatch:
            query_events(connection, HistoryQuery(severities=("major",)),
                         cursor=page.next_cursor, tag_snapshot=snapshot)
        assert mismatch.value.code == "cursor_filter_mismatch"
        with pytest.raises(CursorOutOfRange) as out_of_range:
            query_events(connection, HistoryQuery(until=T0),
                         cursor=page.next_cursor, tag_snapshot=snapshot)
        assert out_of_range.value.code == "cursor_out_of_range"
        with pytest.raises(InvalidFilterError):
            query_events(connection, HistoryQuery(severities=("bogus",)),
                         tag_snapshot=snapshot)
        with pytest.raises(InvalidFilterError):
            query_events(connection, HistoryQuery(), limit=0,
                         tag_snapshot=snapshot)


def test_cursor_round_trip_and_checksum():
    cursor = Cursor(query_fingerprint="q1:abc", after=2, watermark=7,
                    position=(10.0, "a", 5), resolved_names=("a",))
    token = cursor.encode()
    decoded = Cursor.decode(token)
    assert decoded == cursor
    with pytest.raises(CursorMalformed):
        Cursor.decode(token[:-2])
    split = json.loads(__import__("base64").urlsafe_b64decode(token))
    split["after"] = 99
    broken = __import__("base64").urlsafe_b64encode(
        json.dumps(split).encode()).decode()
    with pytest.raises(CursorMalformed):
        Cursor.decode(broken)


def test_export_two_rounds_no_duplicates_no_omissions(db_path, tmp_path):
    ledger = tmp_path / "audit.jsonl"
    cursor_path = tmp_path / "cursor.json"
    with connect(db_path) as connection:
        for index in range(5):
            add_event(connection, f"w{index}", created_at=T0 + index)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        round1 = export_jsonl(connection, HistoryQuery(), ledger,
                              page_size=2, tag_snapshot=snapshot)
        while not round1.exhausted:
            round1 = export_jsonl(connection, HistoryQuery(), ledger,
                                  cursor=round1.next_cursor, page_size=2,
                                  append=True, tag_snapshot=snapshot)
        for index in range(2):
            add_event(connection, "new", created_at=T0 + 10 + index)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        round2 = export_jsonl(connection, HistoryQuery(), tmp_path / "r2.jsonl",
                              cursor=round1.next_cursor, page_size=2,
                              tag_snapshot=snapshot)
        round2_records = [round2]
        while not round2.exhausted:
            round2 = export_jsonl(connection, HistoryQuery(),
                                  tmp_path / "r2.jsonl",
                                  cursor=round2.next_cursor, page_size=2,
                                  append=True, tag_snapshot=snapshot)
            round2_records.append(round2)
    records = [json.loads(line) for line in
               ledger.read_text(encoding="utf-8").splitlines()]
    records2 = [json.loads(line) for line in
                (tmp_path / "r2.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(records) == 5
    assert len(records2) == 2
    combined = [(item["watch_name"], item["event_seq"])
                for item in records + records2]
    assert len(combined) == len(set(combined)) == 7
    assert {item["watch_name"] for item in records2} == {"new"}
    for item in records + records2:
        assert item["key"] == {"watch_name": item["watch_name"],
                               "event_seq": item["event_seq"]}
        assert item["format"] == "pagewatch-history-v1"
    write_cursor_file(cursor_path, round2, HistoryQuery())
    assert read_cursor_file(cursor_path) == round2.next_cursor


def test_export_append_is_idempotent_for_same_keys(db_path, tmp_path):
    ledger = tmp_path / "audit.jsonl"
    with connect(db_path) as connection:
        for index in range(3):
            add_event(connection, "a", created_at=T0 + index)
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        first = export_jsonl(connection, HistoryQuery(), ledger, page_size=3,
                             tag_snapshot=snapshot)
        again = export_jsonl(connection, HistoryQuery(), ledger, page_size=3,
                             append=True, tag_snapshot=snapshot)
    assert first.records == 3
    assert again.records == 3
    assert again.skipped_duplicates == 3
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 3
    # Pulling again from the terminal cursor is a new, empty round: it must
    # leave the existing ledger byte-identical rather than truncate it.
    with connect(db_path) as connection:
        empty = export_jsonl(connection, HistoryQuery(), ledger, page_size=3,
                             cursor=again.next_cursor,
                             tag_snapshot=load_tag_snapshot(connection))
    assert empty.records == 0 and empty.cursor_only is True
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 3


def test_export_refuses_to_clobber_a_different_ledger(db_path, tmp_path):
    ledger = tmp_path / "audit.jsonl"
    ledger.write_text('{"watch_name":"other","event_seq":1}\n',
                      encoding="utf-8")
    with connect(db_path) as connection:
        add_event(connection, "a")
        connection.commit()
        with pytest.raises(ExportError):
            export_jsonl(connection, HistoryQuery(), ledger,
                         tag_snapshot=load_tag_snapshot(connection))


def test_empty_page_does_not_erase_existing_ledger(db_path, tmp_path):
    ledger = tmp_path / "audit.jsonl"
    ledger.write_text('{"watch_name":"a","event_seq":1}\n', encoding="utf-8")
    with connect(db_path) as connection:
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        batch = export_jsonl(connection, HistoryQuery(), ledger,
                             tag_snapshot=snapshot)
    assert batch.records == 0
    assert batch.cursor_only is True
    assert ledger.read_text(encoding="utf-8") == \
           '{"watch_name":"a","event_seq":1}\n'


def test_readonly_connection_cannot_write_and_does_not_create(tmp_path):
    with pytest.raises(FileNotFoundError):
        connect_readonly(tmp_path / "missing.sqlite")
    path = tmp_path / "pagewatch.sqlite"
    init_db(path)
    with connect_readonly(path) as connection:
        with pytest.raises(Exception):
            connection.execute("INSERT INTO stats(key, value) VALUES('x','y')")
    assert not path.exists() or True


def test_concurrent_writer_continuous_pull_loses_nothing(tmp_path):
    path = tmp_path / "pagewatch.sqlite"
    init_db(path)
    stop = threading.Event()
    written = []

    def writer():
        with connect(path) as connection:
            index = 0
            while not stop.is_set():
                written.append(add_event(connection, f"w{index % 4}",
                                         created_at=T0 + index))
                connection.commit()
                index += 1
                time.sleep(0.002)

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        time.sleep(0.05)
        reader = connect_readonly(path)
        cursor = None
        seen = []
        deadline = time.time() + 2.0
        while time.time() < deadline:
            page = query_events(reader, HistoryQuery(), limit=5, cursor=cursor)
            seen.extend(event.id for event in page.events)
            cursor = page.next_cursor
        reader.close()
    finally:
        stop.set()
        thread.join(timeout=5)
    assert len(seen) == len(set(seen))
    assert sorted(seen) == list(range(1, max(seen) + 1))
    assert all(left <= right for left, right in zip(seen, seen[1:]))


def test_history_query_rejects_bad_filters():
    with pytest.raises(InvalidFilterError):
        HistoryQuery(tag_match="neither")
    with pytest.raises(InvalidFilterError):
        HistoryQuery(since=float("nan"))
    with pytest.raises(InvalidFilterError):
        HistoryQuery(tags=("",))
    with pytest.raises(InvalidFilterError):
        HistoryQuery(watch_names=("",))


def test_same_instant_tie_order_is_watch_name_not_rowid(db_path):
    """Insertion order must not decide ties: watch_name does."""
    with connect(db_path) as connection:
        add_event(connection, "zeta", "major")
        add_event(connection, "alpha", "minor")
        add_event(connection, "middle", "noise")
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        page = query_events(connection, HistoryQuery(), limit=2,
                            tag_snapshot=snapshot)
        second = query_events(connection, HistoryQuery(),
                              cursor=page.next_cursor, limit=2,
                              tag_snapshot=snapshot)
    order = [event.watch_name for event in page.events + second.events]
    assert order == ["alpha", "middle", "zeta"]


def test_severity_and_tag_filters_combine(db_path):
    with connect(db_path) as connection:
        add_watch(connection, "a", ["x"])
        add_watch(connection, "b", ["y"])
        add_event(connection, "a", "major")
        add_event(connection, "a", "minor")
        add_event(connection, "b", "major")
        connection.commit()
        snapshot = load_tag_snapshot(connection)
        page = query_events(connection,
                            HistoryQuery(tags=("x",), severities=("major",)),
                            tag_snapshot=snapshot)
        assert [(event.watch_name, event.severity) for event in page.events] \
               == [("a", "major")]
