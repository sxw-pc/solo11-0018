"""Read-only Flask status page, localhost-only binding."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from flask import Flask, Response

from ..clock import SystemClock, utc_iso
from ..fetch.allowlist import is_host_allowed
from ..storage.db import connect, table_exists
from .render import esc, html_document, severity_badge

STATUS_SQL = """
SELECT w.name, w.url, w.last_checked_at, w.last_success_at, w.last_status,
       w.last_error, s.next_due_at, s.consecutive_failures, s.last_outcome,
       (SELECT version FROM snapshots sn WHERE sn.watch_name=w.name
        ORDER BY sn.version DESC LIMIT 1) AS latest_version,
       (SELECT severity FROM events e WHERE e.watch_name=w.name
        ORDER BY e.created_at DESC, e.id DESC LIMIT 1) AS latest_severity,
       (SELECT created_at FROM events e WHERE e.watch_name=w.name
        ORDER BY e.created_at DESC, e.id DESC LIMIT 1) AS latest_event_at
FROM watches w LEFT JOIN schedule_state s ON s.watch_name=w.name
ORDER BY w.name
"""

QUALITY_SQL = """
SELECT watch_name, status, consecutive_failures, rebaseline_version
FROM quality_watch_states
"""


def status_rows(db_path: str | Path) -> list[sqlite3.Row]:
    with connect(db_path) as connection:
        rows = connection.execute(STATUS_SQL).fetchall()
        if table_exists(connection, "quality_watch_states"):
            quality = {
                row["watch_name"]: row
                for row in connection.execute(QUALITY_SQL).fetchall()
            }
        else:
            quality = {}
    return [_merge_quality(row, quality.get(row["name"])) for row in rows]


def _merge_quality(row: sqlite3.Row, quality: sqlite3.Row | None) -> dict:
    value = dict(row)
    value["quality_status"] = quality["status"] if quality else None
    value["quality_failures"] = (quality["consecutive_failures"]
                                 if quality else 0)
    value["rebaseline_version"] = (quality["rebaseline_version"]
                                   if quality else None)
    return value


def _row_html(row: dict) -> str:
    severity = row["latest_severity"] or "none"
    badge = severity_badge(severity) if severity != "none" else '<span class="meta">none</span>'
    last_checked = utc_iso(row["last_checked_at"]) if row["last_checked_at"] else "-"
    next_due = utc_iso(row["next_due_at"]) if row["next_due_at"] else "-"
    error = f'<div class="stats"><span>error: {esc(row["last_error"])}</span></div>' \
        if row["last_error"] else ""
    quality_status = row.get("quality_status")
    if quality_status in ("quarantined", "rebaselined"):
        quality = (
            f'<span class="badge" style="background:#7c2d12;color:#fff">'
            f'quality: {esc(quality_status)} '
            f'({int(row.get("quality_failures") or 0)} failed round(s))</span>')
    else:
        quality = '<span class="meta">quality: ok</span>'
    return f"""
<article class="event">
  <h3>{esc(row["name"])} {badge} {quality}</h3>
  <div class="meta">{esc(row["url"])}</div>
  <div class="stats">
    <span>latest version {row["latest_version"] or "-"}</span>
    <span>last checked {esc(last_checked)}</span>
    <span>last status {row["last_status"] or "-"}</span>
    <span>next due {esc(next_due)}</span>
    <span>failures {row["consecutive_failures"] or 0}</span>
    <span>outcome {esc(row["last_outcome"] or "-")}</span>
    <span>last event {esc(row["latest_event_at"] and utc_iso(row["latest_event_at"]) or "-")}</span>
  </div>{error}
</article>"""


def create_status_app(db_path: str | Path) -> Flask:
    app = Flask(__name__)

    @app.get("/status")
    def status_page() -> Response:
        rows = status_rows(db_path)
        body = "".join(_row_html(row) for row in rows) or (
            '<section class="watch"><p class="empty">No watches recorded yet. '
            "Run pagewatch run-once first.</p></section>")
        document = html_document(
            title="pagewatch status", period_start="live read-only view",
            period_end="latest", generated_at=utc_iso(SystemClock().now()),
            body=body,
        )
        return Response(document, mimetype="text/html; charset=utf-8")

    @app.get("/healthz")
    def healthz() -> Response:
        return Response("ok\n", mimetype="text/plain; charset=utf-8")

    return app


def serve_status(db_path: str | Path, host: str = "127.0.0.1",
                 port: int = 8766) -> None:
    if host != "127.0.0.1" or not is_host_allowed(host):
        raise ValueError("status server may only bind to 127.0.0.1")
    app = create_status_app(db_path)
    app.run(host=host, port=port, debug=False, use_reloader=False)
