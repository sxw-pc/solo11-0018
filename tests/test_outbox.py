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
        at_exact_expiry = insert_alert(
            connection, watch_name="docs", severity="major", event_id=6,
            created_at=NOW + 3600, cooldown_seconds=3600,
            message="exact boundary",
        )
        other_severity = insert_alert(
            connection, watch_name="docs", severity="minor", event_id=4,
            created_at=NOW + 3602, cooldown_seconds=3600, message="minor",
        )
        no_cooldown = insert_alert(
            connection, watch_name="docs", severity="major", event_id=5,
            created_at=NOW + 3602, cooldown_seconds=0, message="no cooldown",
        )
        # A fresh major one second after the boundary alert is inside *that*
        # alert's new window, so it is correctly suppressed.
        after_boundary_alert = insert_alert(
            connection, watch_name="docs", severity="major", event_id=7,
            created_at=NOW + 3601, cooldown_seconds=3600, message="later",
        )
        assert first is not None
        assert duplicate is None
        # At t == prior + cooldown the window has just expired, so the alert
        # must be allowed again (strict comparison, not a rolling window).
        assert at_exact_expiry is not None
        assert after_boundary_alert is None
        assert other_severity is not None
        assert no_cooldown is not None
        # first, exact-boundary major, minor, and cooldown-disabled major.
        assert len(list_alerts(connection)) == 4
        assert first.dedup_key == "docs:major"


def test_cooldown_recovers_after_expiry_instead_of_permanent_silence(tmp_path):
    """Recurring changes just inside each window must recover once the
    original cooldown expires, never stay silent forever."""
    db_path = tmp_path / "db.sqlite"
    init_db(db_path)
    cooldown = 3600
    with connect(db_path) as connection:
        first = insert_alert(
            connection, watch_name="docs", severity="major", event_id=1,
            created_at=NOW, cooldown_seconds=cooldown, message="first",
        )
        emitted = []
        for round_no in range(1, 8):
            created_at = NOW + 600 * round_no  # change every 10 minutes
            alert = insert_alert(
                connection, watch_name="docs", severity="major",
                event_id=round_no + 1, created_at=created_at,
                cooldown_seconds=cooldown,
                message=f"round-{round_no}",
            )
            emitted.append((600 * round_no, alert is not None))
    assert first is not None
    # offsets 600..3000 are inside the window (suppressed); offset 3600 is
    # the exact expiry and must emit again, proving recovery.
    assert emitted == [
        (600, False), (1200, False), (1800, False), (2400, False),
        (3000, False), (3600, True), (4200, False),
    ]


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
