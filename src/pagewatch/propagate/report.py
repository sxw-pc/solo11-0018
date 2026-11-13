"""Deterministic rendering of attribution results: JSON, HTML, files."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ..clock import utc_iso
from ..report.render import esc, html_document
from .model import AttributionResult


def render_attribution_json(result: AttributionResult) -> str:
    """Stable JSON export: sorted keys, fixed layout, trailing newline."""
    payload = asdict(result)
    payload["confidence"] = result.confidence
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      indent=2) + "\n"


def _link_by_id(result: AttributionResult):
    return {link.id: link for link in result.links}


def _cluster_html(result: AttributionResult, cluster) -> str:
    links = _link_by_id(result)
    watches = ", ".join(esc(watch) for watch in cluster.watches)
    span = (f"{esc(utc_iso(cluster.started_at))} → "
            f"{esc(utc_iso(cluster.ended_at))}")
    parts = [
        '<article class="event">',
        f"<h3>{esc(cluster.id)} · root: {esc(cluster.root_watch)} "
        f'<span class="watch-meta">confidence '
        f"{cluster.confidence:.3f}</span></h3>",
        f'<div class="meta">watches: {watches} · {span}</div>',
    ]
    for link_id in cluster.link_ids:
        link = links[link_id]
        basis = ", ".join(link.direction_basis)
        samples = "; ".join(esc(sample) for sample in link.shared_samples)
        parts.append(
            '<div class="stats">'
            f"<span>{esc(link.kind)}</span>"
            f"<span>{esc(link.source_watch)} → "
            f"{esc(link.downstream_watch)}</span>"
            f"<span>events #{link.source_event_id} → "
            f"#{link.downstream_event_id}</span>"
            f"<span>confidence {link.confidence:.3f}</span>"
            f"<span>direction {link.direction_confidence:.3f} "
            f"({esc(basis)})</span>"
            f"<span>gap {link.time_gap_seconds:.0f}s</span>"
            "</div>"
        )
        if samples:
            parts.append(f'<p class="meta">shared {link.shared_count} '
                         f"block(s): {samples}</p>")
    parts.append("</article>")
    return "".join(parts)


def attribution_section_html(result: AttributionResult) -> str:
    """Self-contained section, embeddable in the digest or standalone."""
    meta = (f'{result.event_count} event(s) · {result.watch_count} watch(es) · '
            f'window {result.window_seconds:.0f}s · '
            f'max span {result.max_span_seconds:.0f}s')
    parts = [f'<section class="watch propagation"><h2>Change propagation '
             f'attribution<span class="watch-meta">{esc(meta)}</span></h2>']
    if result.degraded:
        reasons = ", ".join(result.degrade_reasons)
        parts.append('<p class="empty">Attribution degraded '
                     f"({esc(reasons)}): no cross-watch correlation was "
                     "attempted; every event is listed as unattributed.</p>")
    elif not result.clusters:
        parts.append('<p class="empty">No cross-watch propagation detected '
                     "in the selected period.</p>")
    for cluster in result.clusters:
        parts.append(_cluster_html(result, cluster))
    if result.unattributed:
        items = "".join(
            f"<li>event #{esc(item.event_id)} ({esc(item.watch_name)}): "
            f"{esc(item.reason)}</li>" for item in result.unattributed)
        parts.append('<details class="noise"><summary>Unattributed events '
                     f"({len(result.unattributed)})</summary>"
                     f"<ul>{items}</ul></details>")
    parts.append("</section>")
    return "".join(parts)


def render_attribution_html(result: AttributionResult, *,
                            title: str = "pagewatch propagation attribution"
                            ) -> str:
    return html_document(
        title=title,
        period_start="attribution window "
        f"{result.window_seconds:.0f}s / span {result.max_span_seconds:.0f}s",
        period_end=f"generated {result.generated_at}",
        generated_at=result.generated_at,
        body=attribution_section_html(result),
    )


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def write_attribution(result: AttributionResult, json_path: str | Path, *,
                      html_path: str | Path | None = None,
                      title: str = "pagewatch propagation attribution",
                      ) -> tuple[Path, Path | None]:
    """Write the standalone JSON export and, optionally, an HTML report."""
    json_destination = Path(json_path)
    _write_text(json_destination, render_attribution_json(result))
    html_destination = Path(html_path) if html_path else None
    if html_destination is not None:
        _write_text(html_destination,
                    render_attribution_html(result, title=title))
    return json_destination, html_destination
