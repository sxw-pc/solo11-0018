"""Round replay: read-only, reproducible per-watch chain reconstruction."""

from __future__ import annotations

from .chain import (
    STAGE_ORDER,
    ReplayReport,
    RoundReplay,
    StageRecord,
    WatchChain,
    build_replay,
    connect_readonly,
)
from .report import ReplayWriteResult, render_report_html, render_report_json, write_report
from .rounds import DEFAULT_ROUND_GAP_SECONDS, Round, discover_rounds, select_rounds
from .workspace import ReplayWorkspace

__all__ = [
    "DEFAULT_ROUND_GAP_SECONDS",
    "STAGE_ORDER",
    "ReplayReport",
    "ReplayWorkspace",
    "ReplayWriteResult",
    "Round",
    "RoundReplay",
    "StageRecord",
    "WatchChain",
    "build_replay",
    "connect_readonly",
    "discover_rounds",
    "render_report_html",
    "render_report_json",
    "select_rounds",
    "write_report",
]
