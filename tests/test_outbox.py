from __future__ import annotations

from pagewatch.alert.outbox import append_alert, read_outbox
from pagewatch.alert.rules import alert_message
from pagewatch.storage.db import connect, init_db
from pagewatch.storage.events import insert_alert, list_alerts

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
