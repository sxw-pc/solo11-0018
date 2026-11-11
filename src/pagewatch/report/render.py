"""Static HTML rendering helpers.  No JavaScript and no external assets."""

from __future__ import annotations

import html
from collections.abc import Iterable, Sequence

from ..models import ChangeEvent

SEVERITY_CLASS = {"major": "major", "minor": "minor", "noise": "noise"}

CSS = """
:root{--bg:#f7f8fa;--panel:#fff;--ink:#17212b;--muted:#5b6775;
--line:#dde3ea;--major:#b42318;--minor:#a15c00;--noise:#5f6b7a;
--add:#e8f5ec;--del:#fdecea;--accent:#0b5cad}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 "Segoe UI",system-ui,-apple-system,sans-serif}
header{background:#11253d;color:#fff;padding:28px clamp(18px,4vw,52px)}
header h1{margin:0 0 6px;font-size:28px}header p{margin:2px 0;color:#c8d6e6}
main{max-width:1180px;margin:0 auto;padding:24px clamp(12px,3vw,28px) 60px}
.watch{background:var(--panel);border:1px solid var(--line);border-radius:10px;
margin:20px 0;overflow:hidden}.watch>h2{margin:0;padding:14px 18px;
font-size:18px;border-bottom:1px solid var(--line);background:#fbfcfe}
.watch-meta{font-weight:400;color:var(--muted);font-size:13px;margin-left:8px}
.event{padding:16px 18px;border-bottom:1px solid var(--line)}
.event:last-child{border-bottom:0}.event h3{margin:0 0 8px;font-size:15px;
display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.badge{display:inline-block;border-radius:999px;padding:1px 9px;color:#fff;
font-size:12px;font-weight:700;letter-spacing:.02em;text-transform:uppercase}
.badge.major{background:var(--major)}.badge.minor{background:var(--minor)}
.badge.noise{background:var(--noise)}
.meta{color:var(--muted);font-size:13px}.stats{display:flex;gap:14px;
flex-wrap:wrap;font-size:13px;color:var(--muted);margin:4px 0 10px}
pre.diff{background:#0f1720;color:#dbe6f2;padding:12px 14px;border-radius:7px;
overflow:auto;font:12.5px/1.5 Consolas,"Cascadia Mono",monospace;margin:8px 0 0}
pre.diff .add{background:var(--add);color:#135d30;display:block}
pre.diff .del{background:var(--del);color:#8f1d14;display:block}
pre.diff .hunk{color:#7cc7ff;display:block}
pre.diff .meta{color:#93a4b8;display:block}
details.noise{margin:12px 18px 18px;border:1px dashed var(--line);border-radius:8px;
padding:8px 12px;background:#fafbfc}details.noise summary{cursor:pointer;
color:var(--noise);font-weight:600}.empty{color:var(--muted);font-style:italic}
.period{display:flex;gap:18px;flex-wrap:wrap;margin:0 0 16px;color:var(--muted)}
.legend{display:flex;gap:10px;flex-wrap:wrap;font-size:12px;color:var(--muted)}
footer{color:var(--muted);font-size:12px;padding:0 4px}
a{color:var(--accent)}
"""


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def severity_badge(severity: str) -> str:
    css_class = SEVERITY_CLASS.get(severity, "noise")
    return f'<span class="badge {css_class}">{esc(severity)}</span>'


def render_diff(diff_text: str) -> str:
    lines: list[str] = []
    for line in diff_text.splitlines():
        if line.startswith(("+++", "---")):
            css = "meta"
        elif line.startswith("@@"):
            css = "hunk"
        elif line.startswith("+"):
            css = "add"
        elif line.startswith("-"):
            css = "del"
        else:
            css = "ctx"
        rendered = esc(line) or "&nbsp;"
        lines.append(f'<span class="{css}">{rendered}</span>')
    if not lines:
        lines.append('<span class="ctx">(no textual diff)</span>')
    return '<pre class="diff">' + "".join(lines) + "</pre>"


def render_event(event: ChangeEvent) -> str:
    from ..clock import utc_iso

    regions = ", ".join(event.regions) or "-"
    keywords = ", ".join(event.keyword_hits) or "-"
    focus = "yes" if event.focus_hit else "no"
    return f"""
<article class="event" id="event-{event.id}">
  <h3>{severity_badge(event.severity)} <span>{esc(event.summary)}</span></h3>
  <div class="meta">event #{event.id} · {esc(utc_iso(event.created_at))} · snapshots
    {event.from_snapshot_id} → {event.to_snapshot_id}</div>
  <div class="stats"><span>change rate {event.change_rate:.3f}</span>
    <span>+{event.added_lines} / −{event.removed_lines} lines</span>
    <span>focus {focus}</span><span>regions: {esc(regions)}</span>
    <span>keywords: {esc(keywords)}</span></div>
  {render_diff(event.diff_text)}
</article>"""


def event_group_html(watch_name: str, events: Sequence[ChangeEvent]) -> str:
    visible = [event for event in events if event.severity != "noise"]
    noise = [event for event in events if event.severity == "noise"]
    counts = {level: sum(1 for event in events if event.severity == level)
              for level in ("major", "minor", "noise")}
    meta = (f'{len(events)} event(s) · major {counts["major"]} · '
            f'minor {counts["minor"]} · noise {counts["noise"]}')
    parts = [f'<section class="watch"><h2>{esc(watch_name)}'
             f'<span class="watch-meta">{meta}</span></h2>']
    if visible:
        parts.extend(render_event(event) for event in visible)
    if noise:
        parts.append('<details class="noise"><summary>Noise events folded '
                     f'({len(noise)})</summary>')
        parts.extend(render_event(event) for event in noise)
        parts.append("</details>")
    if not visible and not noise:
        parts.append('<p class="empty">No changes in this period.</p>')
    parts.append("</section>")
    return "".join(parts)


def html_document(*, title: str, period_start: str, period_end: str,
                  generated_at: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)}</title><style>{CSS}</style></head>
<body>
<header><h1>{esc(title)}</h1>
<p>Page change digest · grouped by watch · static, offline report</p></header>
<main>
<div class="period"><span><strong>Period:</strong> {esc(period_start)} → {esc(period_end)}</span>
<span><strong>Generated:</strong> {esc(generated_at)}</span></div>
{body}
<footer>Generated by pagewatch. Alerts are recorded locally and are never sent
outside this machine.</footer>
</main></body></html>
"""


def event_count_line(events: Iterable[ChangeEvent]) -> str:
    events = list(events)
    counts = {level: sum(1 for event in events if event.severity == level)
              for level in ("major", "minor", "noise")}
    return (f"{len(events)} event(s) · major {counts['major']} · "
            f"minor {counts['minor']} · noise {counts['noise']}")
