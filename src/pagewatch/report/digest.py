"""Deterministic digest generation and event JSON export."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence

from ..clock import utc_iso
from ..models import ChangeEvent
from .render import event_count_line, event_group_html, html_document


@dataclass(frozen=True)
class DigestResult:
    html_path: Path
    json_path: Path | None
    event_count: int


def _stable_generated_at(events: Sequence[ChangeEvent], value: str | None) -> str:
    if value is not None:
        return value
    latest = max((event.created_at for event in events), default=0.0)
    return utc_iso(latest)


def render_digest(events: Sequence[ChangeEvent], *, since: float | None = None,
                  until: float | None = None, generated_at: str | None = None,
                  title: str = "pagewatch change digest",
                  extra_sections: Sequence[str] = ()) -> str:
    ordered = sorted(events, key=lambda event: (
        event.watch_name, event.created_at, event.id))
    grouped: dict[str, list[ChangeEvent]] = {}
    for event in ordered:
        grouped.setdefault(event.watch_name, []).append(event)
    period_start = utc_iso(since) if since is not None else "beginning"
    period_end = utc_iso(until) if until is not None else "latest"
    body = [f'<p class="meta">{event_count_line(ordered)}</p>']
    for watch_name in sorted(grouped):
        body.append(event_group_html(watch_name, grouped[watch_name]))
    if not grouped:
        body.append('<section class="watch"><p class="empty">No events in '
                    "the selected period.</p></section>")
    body.extend(extra_sections)
    return html_document(
        title=title, period_start=period_start, period_end=period_end,
        generated_at=_stable_generated_at(events, generated_at),
        body="\n".join(body),
    )


def render_event_json(events: Sequence[ChangeEvent]) -> str:
    payload = [event.as_json() for event in sorted(
        events, key=lambda event: (event.watch_name, event.created_at, event.id))]
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      indent=2) + "\n"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def write_digest(events: Sequence[ChangeEvent], html_path: str | Path, *,
                 json_path: str | Path | None = None,
                 since: float | None = None, until: float | None = None,
                 generated_at: str | None = None,
                 title: str = "pagewatch change digest",
                 extra_sections: Sequence[str] = ()) -> DigestResult:
    html_destination = Path(html_path)
    _write_text(html_destination, render_digest(
        events, since=since, until=until, generated_at=generated_at,
        title=title, extra_sections=extra_sections))
    json_destination = Path(json_path) if json_path else None
    if json_destination is not None:
        _write_text(json_destination, render_event_json(events))
    return DigestResult(html_destination, json_destination, len(events))
