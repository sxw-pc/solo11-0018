"""Deterministic severity classification."""

from __future__ import annotations

from ..models import DiffMetrics, Rules

SEVERITIES = ("noise", "minor", "major")


def classify(metrics: DiffMetrics, rules: Rules) -> str:
    """Classify a change.

    Priority: explicit importance signals (focus region or keyword) and a high
    change rate make a major change; only-noise or below-floor changes are
    noise; everything else is minor.
    """
    if (metrics.focus_hit
            or bool(metrics.keyword_hits)
            or metrics.change_rate >= rules.major_change_rate):
        return "major"
    if metrics.noise_only or metrics.change_rate < rules.noise_max_change_rate:
        return "noise"
    return "minor"


def reasons(metrics: DiffMetrics, rules: Rules, severity: str) -> list[str]:
    values: list[str] = []
    if metrics.focus_hit:
        values.append("focus-selector")
    if metrics.keyword_hits:
        values.append("keyword:" + ",".join(metrics.keyword_hits))
    if metrics.change_rate >= rules.major_change_rate:
        values.append("high-change-rate")
    if severity == "noise" and metrics.noise_only:
        values.append("noise-region-only")
    if severity == "noise" and metrics.change_rate < rules.noise_max_change_rate:
        values.append("below-noise-floor")
    return values
