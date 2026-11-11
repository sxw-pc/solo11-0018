from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pagewatch.config import (
    ConfigError, effective_rules, load_rules, load_watchlist,
)
from pagewatch.models import Rules

ROOT = Path(__file__).parents[1]


def watch(**overrides):
    value = {
        "name": "example",
        "url": "http://127.0.0.1:8765/docs",
        "interval_seconds": 60,
        "tags": ["docs"],
        "include_selectors": ["#content"],
        "exclude_selectors": [],
        "noise_patterns": [],
        "keywords": [],
    }
    value.update(overrides)
    return value


def write_watchlist(tmp_path: Path, watches):
    path = tmp_path / "watchlist.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "watches": watches},
                                   allow_unicode=True), encoding="utf-8")
    return path


def write_rules(tmp_path: Path, data=None):
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(data or {"version": 1}), encoding="utf-8")
    return path


def test_example_files_load_and_overrides_apply():
    watches = load_watchlist(ROOT / "configs/watchlist.example.yaml")
    rules = load_rules(ROOT / "configs/rules.example.yaml")
    assert [item.name for item in watches] == [
        "announcements", "docs", "pricing", "noisy-status"]
    assert rules.max_workers == 4
    merged = effective_rules(rules, watches[0])
    assert merged.alert_cooldown_seconds == 600


def test_per_watch_rule_override_is_validated_and_merged(tmp_path):
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "major_change_rate": 0.5, "focus_selectors": ["#notice"]})])
    watch_spec = load_watchlist(path)[0]
    merged = effective_rules(Rules(), watch_spec)
    assert merged.major_change_rate == 0.5
    assert merged.focus_selectors == ("#notice",)


@pytest.mark.parametrize("mutation, fragment", [
    ({"name": None}, "name is required"),
    ({"url": "https://outside.invalid/page"}, "blocked by the local allowlist"),
    ({"url": "http://user:pass@127.0.0.1/x"}, "must not contain credentials"),
    ({"url": "ftp://127.0.0.1/x"}, "absolute http"),
    ({"interval_seconds": 9}, ">= 10"),
    ({"interval_seconds": True}, "integer"),
    ({"include_selectors": ["p["]}, "invalid CSS selector"),
    ({"noise_patterns": ["("]}, "invalid noise pattern"),
    ({"enabled": "yes"}, "enabled must be a boolean"),
    ({"unknown": 1}, "unknown field"),
])
def test_watch_validation_errors(tmp_path, mutation, fragment):
    path = write_watchlist(tmp_path, [watch(**mutation)])
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    assert fragment in str(error.value)
    assert str(path) in str(error.value)


def test_duplicate_names_and_empty_list_rejected(tmp_path):
    duplicate = write_watchlist(tmp_path, [watch(), watch()])
    with pytest.raises(ConfigError, match="duplicate watch name"):
        load_watchlist(duplicate)
    empty = tmp_path / "empty.yaml"
    empty.write_text("version: 1\nwatches: []\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="non-empty list"):
        load_watchlist(empty)
    path = tmp_path / "unknown.yaml"
    path.write_text("version: 1\nwatches: []\nextra: true\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_watchlist(path)


def test_invalid_watch_override_selector_and_severity(tmp_path):
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "focus_selectors": ["bad["]})])
    with pytest.raises(ConfigError, match="invalid CSS selector"):
        load_watchlist(path)
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "alert_severities": ["emergency"]})])
    with pytest.raises(ConfigError, match="noise, minor or major"):
        load_watchlist(path)


def test_rules_validation(tmp_path):
    with pytest.raises(ConfigError, match="must not exceed"):
        load_rules(write_rules(tmp_path, {
            "classification": {"noise_max_change_rate": 0.8,
                               "major_change_rate": 0.3}}))
    with pytest.raises(ConfigError, match="noise, minor or major"):
        load_rules(write_rules(tmp_path, {
            "alerts": {"severities": ["fatal"]}}))
    with pytest.raises(ConfigError, match="invalid CSS selector"):
        load_rules(write_rules(tmp_path, {
            "classification": {"focus_selectors": ["p["]}}))
    with pytest.raises(ConfigError, match="unknown field"):
        load_rules(write_rules(tmp_path, {"classification": {"typo": 1}}))
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_rules(write_rules(tmp_path, {"unexpected": 1}))
    with pytest.raises(ConfigError, match="major_keep_forever"):
        load_rules(write_rules(tmp_path, {
            "retention": {"major_keep_forever": "not-a-bool"}}))
