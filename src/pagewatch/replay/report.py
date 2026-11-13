"""Deterministic JSON and static HTML rendering of replay reports.

Both formats are pure functions of the report data: no wall clock, no
randomness, fixed key ordering.  Replaying the same records twice yields
byte-identical files.  The HTML is static, JavaScript-free and uses no
external assets.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..clock import utc_iso
from ..report.render import esc
from .chain import ReplayReport, RoundReplay, StageRecord, WatchChain

REPORT_VERSION = 1

CSS = """
:root{--bg:#f7f8fa;--panel:#fff;--ink:#17212b;--muted:#5b6775;
--line:#dde3ea;--ok:#1a7f37;--skip:#5f6b7a;--deg:#a15c00;--bad:#b42318;
--accent:#0b5cad}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 "Segoe UI",system-ui,-apple-system,sans-serif}
header{background:#11253d;color:#fff;padding:28px clamp(18px,4vw,52px)}
header h1{margin:0 0 6px;font-size:28px}header p{margin:2px 0;color:#c8d6e6}
main{max-width:1180px;margin:0 auto;padding:24px clamp(12px,3vw,28px) 60px}
.round{background:var(--panel);border:1px solid var(--line);border-radius:10px;
margin:20px 0;overflow:hidden}
.round>h2{margin:0;padding:14px 18px;font-size:18px;
border-bottom:1px solid var(--line);background:#fbfcfe}
.round-meta{font-weight:400;color:var(--muted);font-size:13px;margin-left:8px}
.watch{border-bottom:1px solid var(--line);padding:14px 18px}
.watch:last-child{border-bottom:0}
.watch h3{margin:0 0 8px;font-size:15px;display:flex;gap:10px;
align-items:center;flex-wrap:wrap}
.badge{display:inline-block;border-radius:999px;padding:1px 9px;color:#fff;
font-size:12px;font-weight:700;letter-spacing:.02em;text-transform:uppercase}
.badge.ok{background:var(--ok)}.badge.skipped{background:var(--skip)}
.badge.degraded{background:var(--deg)}.badge.missing,.badge.invalid,
.badge.broken{background:var(--bad)}
.meta{color:var(--muted);font-size:13px}
ol.chain{list-style:none;margin:8px 0 0;padding:0}
ol.chain li{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;
padding:7px 10px;border:1px solid var(--line);border-radius:7px;margin:4px 0;
background:#fbfcfe}
ol.chain li .stage-name{font-weight:700;min-width:86px}
ol.chain li.missing,ol.chain li.invalid{background:#fdf3f2;border-color:#f0cdc8}
ol.chain li.degraded{background:#fdf8ec;border-color:#ead9b0}
.reason{color:var(--bad);font-size:13px}
li.degraded .reason{color:var(--deg)}
.kv{display:flex;gap:12px;flex-wrap:wrap;font-size:12.5px;color:var(--muted);
width:100%;margin-top:4px}
.kv span{white-space:nowrap}
.kv code{color:var(--ink);font:12px Consolas,"Cascadia Mono",monospace}
.callout{border-left:4px solid var(--bad);background:#fdf3f2;padding:8px 12px;
margin:8px 0;font-size:13px;border-radius:0 6px 6px 0}
.callout.degraded{border-color:var(--deg);background:#fdf8ec}
details{margin:6px 0 0;width:100%}details summary{cursor:pointer;
color:var(--accent);font-size:12.5px;font-weight:600}
table.blocks{border-collapse:collapse;margin:8px 0;font-size:12.5px;width:100%}
table.blocks th,table.blocks td{border:1px solid var(--line);padding:3px 8px;
text-align:left}table.blocks th{background:#f2f5f9}
table.blocks code{font:11.5px Consolas,"Cascadia Mono",monospace}
.empty{color:var(--muted);font-style:italic}
.summary{display:flex;gap:18px;flex-wrap:wrap;margin:0 0 16px;
color:var(--muted)}
footer{color:var(--muted);font-size:12px;padding:0 4px}
"""


@dataclass(frozen=True)
class ReplayWriteResult:
    html_path: Path
    json_path: Path


# -- JSON -----------------------------------------------------------------


def _stage_json(stage: StageRecord) -> dict[str, Any]:
    value: dict[str, Any] = {"stage": stage.stage, "status": stage.status}
    if stage.reason:
        value["reason"] = stage.reason
    value["details"] = stage.details
    return value


def _chain_json(chain: WatchChain) -> dict[str, Any]:
    return {
        "watch_name": chain.watch_name,
        "status": chain.status,
        "breakpoints": list(chain.breakpoints),
        "degradations": list(chain.degradations),
        "chain": [_stage_json(stage) for stage in chain.stages],
    }


def _round_json(rnd: RoundReplay) -> dict[str, Any]:
    return {
        "index": rnd.index,
        "window": {
            "start": rnd.start,
            "end": rnd.end,
            "start_iso": utc_iso(rnd.start),
            "end_iso": utc_iso(rnd.end),
        },
        "watch_count": len(rnd.chains),
        "watches": [_chain_json(chain) for chain in rnd.chains],
    }


def report_to_dict(report: ReplayReport) -> dict[str, Any]:
    return {
        "report": "pagewatch-replay",
        "report_version": REPORT_VERSION,
        "generated_at": report.generated_at,
        "status": report.status,
        "round_count": len(report.rounds),
        "rounds": [_round_json(rnd) for rnd in report.rounds],
    }


def render_report_json(report: ReplayReport) -> str:
    return json.dumps(report_to_dict(report), ensure_ascii=False,
                      sort_keys=True, indent=2) + "\n"


# -- HTML -----------------------------------------------------------------


def _badge(status: str) -> str:
    return f'<span class="badge {esc(status)}">{esc(status)}</span>'


def _value_text(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if value is None:
        return "-"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _details_html(stage: StageRecord) -> str:
    details = stage.details
    if not details:
        return ""
    block_hashes = details.get("block_hashes")
    chips = []
    for key, value in details.items():
        if key == "block_hashes":
            continue
        chips.append(f"<span>{esc(key)} <code>{esc(_value_text(value))}</code></span>")
    parts = [f'<div class="kv">{"".join(chips)}</div>'] if chips else []
    if isinstance(block_hashes, list) and block_hashes:
        rows = "".join(
            "<tr><td>{index}</td><td><code>{hash}</code></td>"
            "<td>{region}</td><td>{focus}</td><td>{noise}</td></tr>".format(
                index=esc(entry.get("index", "")),
                hash=esc(entry.get("hash", "")),
                region=esc(entry.get("region", "")),
                focus=_value_text(bool(entry.get("is_focus", False))),
                noise=_value_text(bool(entry.get("is_noise", False))),
            )
            for entry in block_hashes
        )
        parts.append(
            f"<details><summary>block hashes ({len(block_hashes)})</summary>"
            '<table class="blocks"><thead><tr><th>#</th><th>sha256</th>'
            "<th>region</th><th>focus</th><th>noise</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></details>")
    return "".join(parts)


def _stage_html(stage: StageRecord) -> str:
    reason = (f'<span class="reason">{esc(stage.reason)}</span>'
              if stage.reason else "")
    return (f'<li class="{esc(stage.status)}">'
            f'<span class="stage-name">{esc(stage.stage)}</span>'
            f"{_badge(stage.status)}{reason}{_details_html(stage)}</li>")


def _chain_html(chain: WatchChain) -> str:
    parts = [f'<article class="watch"><h3>{esc(chain.watch_name)} '
             f"{_badge(chain.status)}</h3>"]
    for kind, entries in (("breakpoint", chain.breakpoints),
                          ("degraded", chain.degradations)):
        for entry in entries:
            css = "callout" if kind == "breakpoint" else "callout degraded"
            parts.append(
                f'<div class="{css}"><strong>{esc(kind)}</strong> at stage '
                f"<code>{esc(entry['stage'])}</code>: "
                f"{esc(entry['reason'])}</div>")
    parts.append('<ol class="chain">')
    parts.extend(_stage_html(stage) for stage in chain.stages)
    parts.append("</ol></article>")
    return "".join(parts)


def _round_html(rnd: RoundReplay) -> str:
    window = f"{utc_iso(rnd.start)} → {utc_iso(rnd.end)}"
    counts = {"ok": 0, "degraded": 0, "broken": 0}
    for chain in rnd.chains:
        counts[chain.status] += 1
    meta = (f"{esc(window)} · {len(rnd.chains)} watch(es) · "
            f"ok {counts['ok']} · degraded {counts['degraded']} · "
            f"broken {counts['broken']}")
    parts = [f'<section class="round" id="round-{rnd.index}">'
             f"<h2>Round {rnd.index}"
             f'<span class="round-meta">{meta}</span></h2>']
    if rnd.chains:
        parts.extend(_chain_html(chain) for chain in rnd.chains)
    else:
        parts.append('<p class="empty">No watch records in this round.</p>')
    parts.append("</section>")
    return "".join(parts)


def render_report_html(report: ReplayReport, *,
                       title: str = "pagewatch replay report") -> str:
    chains = [chain for rnd in report.rounds for chain in rnd.chains]
    breakpoints = sum(len(chain.breakpoints) for chain in chains)
    degradations = sum(len(chain.degradations) for chain in chains)
    body = [
        '<div class="summary">'
        f"<span><strong>Rounds:</strong> {len(report.rounds)}</span>"
        f"<span><strong>Chains:</strong> {len(chains)}</span>"
        f"<span><strong>Status:</strong> {_badge(report.status)}</span>"
        f"<span><strong>Breakpoints:</strong> {breakpoints}</span>"
        f"<span><strong>Degradations:</strong> {degradations}</span>"
        f"<span><strong>Generated:</strong> {esc(report.generated_at)}</span>"
        "</div>"
    ]
    body.extend(_round_html(rnd) for rnd in report.rounds)
    if not report.rounds:
        body.append('<section class="round"><p class="empty">No monitoring '
                    "rounds recorded.</p></section>")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)}</title><style>{CSS}</style></head>
<body>
<header><h1>{esc(title)}</h1>
<p>Per-watch replay of monitoring rounds · static, offline, read-only report</p></header>
<main>
{"".join(body)}
<footer>Generated by pagewatch replay. Chain order follows the actual
monitoring pipeline: probe → archive → snapshot → normalize → diff →
classify → alert. Breakpoints mark missing or inconsistent links;
degradations mark links inferred from legacy records.</footer>
</main></body></html>
"""


# -- writing ----------------------------------------------------------------


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def write_report(report: ReplayReport, html_path: str | Path,
                 json_path: str | Path, *,
                 title: str = "pagewatch replay report") -> ReplayWriteResult:
    html_destination = Path(html_path)
    json_destination = Path(json_path)
    _write_text(html_destination, render_report_html(report, title=title))
    _write_text(json_destination, render_report_json(report))
    return ReplayWriteResult(html_destination, json_destination)
