from __future__ import annotations

import pytest

from pagewatch.alert.rules import alert_message, should_alert
from pagewatch.diff.classify import classify, reasons
from pagewatch.models import DiffMetrics, Rules


def metric(**overrides):
    values = dict(
        base_lines=10, new_lines=10, added_lines=1, removed_lines=1,
        changed_lines=2, change_rate=0.2, regions=("#main",),
        focus_hit=False, noise_only=False, keyword_hits=(),
    )
    values.update(overrides)
    return DiffMetrics(**values)


def test_severity_boundaries():
    rules = Rules().model_copy(update={
        "noise_max_change_rate": 0.02, "major_change_rate": 0.5})
    assert classify(metric(change_rate=0.019), rules) == "noise"
    assert classify(metric(change_rate=0.02), rules) == "minor"
    assert classify(metric(change_rate=0.499), rules) == "minor"
    assert classify(metric(change_rate=0.5), rules) == "major"


def test_focus_and_keywords_override_low_change_rate():
    rules = Rules()
    assert classify(metric(change_rate=0.001, focus_hit=True), rules) == "major"
    assert classify(metric(change_rate=0.001, keyword_hits=("重要",)), rules) == "major"


def test_noise_region_only_is_noise_but_focus_wins():
    rules = Rules()
    assert classify(metric(change_rate=0.2, noise_only=True), rules) == "noise"
    assert classify(metric(change_rate=0.2, noise_only=True,
                           focus_hit=True), rules) == "major"


def test_reasons_and_alert_eligibility():
    rules = Rules()
    metrics = metric(change_rate=0.8, focus_hit=True,
                     keyword_hits=("critical",))
    assert classify(metrics, rules) == "major"
    values = reasons(metrics, rules, "major")
    assert "focus-selector" in values
    assert "high-change-rate" in values
    assert should_alert("major", rules) is True
    assert should_alert("minor", rules) is False
    assert "example" in alert_message("example", "major", 7, 0.8)
    noise = metric(change_rate=0.001, noise_only=True)
    noise_reasons = reasons(noise, rules, "noise")
    assert "noise-region-only" in noise_reasons
    assert "below-noise-floor" in noise_reasons


@pytest.mark.parametrize("severity, expected", [
    ("noise", False), ("minor", False), ("major", True),
])
def test_custom_alert_severities(severity, expected):
    rules = Rules().model_copy(update={"alert_severities": ("major",)})
    assert should_alert(severity, rules) is expected
