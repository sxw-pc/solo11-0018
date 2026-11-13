from __future__ import annotations

import threading

from pagewatch.alert.outbox import append_alert, read_outbox
from pagewatch.alert.rules import alert_message
from pagewatch.storage.db import connect, init_db
from pagewatch.storage.events import (
    insert_alert, list_alerts, record_alert,
)

NOW = 1_800_000_000.0


def test_alert_cooldown_deduplicates_same_watch_and_severity(tmp_path):
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        first = insert_alert(
            connection, watch_name="docs", severity="major", event_id=1,
            created_at=NOW, cooldown_seconds=3600,
            message=alert_message("docs", "major", 1, 0.8),
        )
        duplicate = insert_alert(
            connection, watch_name="docs", severity="major", event_id=2,
            created_at=NOW + 100, cooldown_seconds=3600,
            message="duplicate",
        )
        after_cooldown = insert_alert(
            connection, watch_name="docs", severity="major", event_id=3,
            created_at=NOW + 3601, cooldown_seconds=3600, message="later",
        )
        other_severity = insert_alert(
            connection, watch_name="docs", severity="minor", event_id=4,
            created_at=NOW + 3602, cooldown_seconds=3600, message="minor",
        )
        no_cooldown = insert_alert(
            connection, watch_name="docs", severity="major", event_id=5,
            created_at=NOW + 3602, cooldown_seconds=0, message="no cooldown",
        )
        assert first is not None
        assert duplicate is None
        assert after_cooldown is not None
        assert other_severity is not None
        assert no_cooldown is not None
        assert len(list_alerts(connection)) == 4
        assert first.dedup_key == "docs:major"


def test_concurrent_inserts_respect_cooldown(tmp_path):
    """Threads hitting the same watch/severity at once emit a single alert."""
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    barrier = threading.Barrier(8)
    outcomes: list[int | None] = []
    outcomes_lock = threading.Lock()

    def worker(index: int) -> None:
        with connect(db_path) as connection:
            barrier.wait()
            # record_alert commits while holding the dedup lock, mirroring how
            # separate per-watch connections behave inside a worker pool.
            record = record_alert(
                connection, watch_name="docs", severity="major",
                event_id=index + 1, created_at=NOW, cooldown_seconds=3600,
                message=alert_message("docs", "major", index + 1, 0.8),
            )
            with outcomes_lock:
                outcomes.append(record.id if record else None)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len([value for value in outcomes if value is not None]) == 1
    with connect(db_path) as connection:
        assert len(list_alerts(connection)) == 1


def test_per_watch_cooldown_override_governs_dedup(tmp_path):
    """A short per-watch cooldown wins over the longer global window."""
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        first = insert_alert(
            connection, watch_name="docs", severity="major", event_id=1,
            created_at=NOW, cooldown_seconds=60,
            message=alert_message("docs", "major", 1, 0.8),
        )
        # 100s later: inside a 3600s global window but past the 60s override.
        allowed = insert_alert(
            connection, watch_name="docs", severity="major", event_id=2,
            created_at=NOW + 100, cooldown_seconds=60, message="override",
        )
        # 30s after that: still inside the override window, so suppressed.
        suppressed = insert_alert(
            connection, watch_name="docs", severity="major", event_id=3,
            created_at=NOW + 130, cooldown_seconds=60, message="dup",
        )
        assert first is not None and allowed is not None
        assert suppressed is None
        assert len(list_alerts(connection)) == 2


def test_record_alert_commits_and_is_visible_to_other_connections(tmp_path):
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        record = record_alert(
            connection, watch_name="docs", severity="major", event_id=1,
            created_at=NOW, cooldown_seconds=3600,
            message=alert_message("docs", "major", 1, 0.8),
        )
        assert record is not None
    # A brand-new connection sees the committed row and applies the cooldown.
    with connect(db_path) as other:
        duplicate = record_alert(
            other, watch_name="docs", severity="major", event_id=2,
            created_at=NOW + 5, cooldown_seconds=3600, message="dup",
        )
        assert duplicate is None
        assert len(list_alerts(other)) == 1


def test_insert_alert_does_not_commit_on_its_own(tmp_path):
    """Inside a larger transaction the caller owns the commit boundary."""
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    connection = connect(db_path)
    try:
        record = insert_alert(
            connection, watch_name="docs", severity="major", event_id=1,
            created_at=NOW, cooldown_seconds=0, message="pending",
        )
        assert record is not None
        # Uncommitted on this connection: a separate connection sees nothing.
        with connect(db_path) as other:
            assert list_alerts(other) == []
        connection.commit()
        with connect(db_path) as other:
            assert len(list_alerts(other)) == 1
    finally:
        connection.close()


def test_jsonl_outbox_append_and_read(tmp_path):
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    with connect(db_path) as connection:
        first = insert_alert(connection, watch_name="a", severity="major",
                             event_id=1, created_at=NOW, cooldown_seconds=0,
                             message="one")
        second = insert_alert(connection, watch_name="b", severity="major",
                              event_id=2, created_at=NOW, cooldown_seconds=0,
                              message="two")
    path = tmp_path / "outbox" / "alerts.jsonl"
    append_alert(path, first)
    append_alert(path, second)
    rows = read_outbox(path)
    assert [row["watch_name"] for row in rows] == ["a", "b"]
    assert rows[0]["outbox_version"] == 1
    assert rows[1]["message"] == "two"
    assert read_outbox(tmp_path / "missing.jsonl") == []
