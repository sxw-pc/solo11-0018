from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pagewatch.config import (
    ConfigError, effective_rules, load_all, load_rules, load_watchlist,
)
from pagewatch.diff.classify import rule_snapshot
from pagewatch.models import Rules, WatchSpec

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
    # The legacy scalar is also expressed as a rule patch, last layer wins.
    configured = {item["id"]: item for item in rule_snapshot(merged)}
    assert configured["high-change-rate"]["threshold"] == 0.5
    assert configured["high-change-rate"]["source"] == "watch:example"


def test_global_rule_patch_inherits_and_watch_disables_one_rule(tmp_path):
    rules_path = write_rules(tmp_path, {
        "version": 1,
        "rules": [
            {"id": "keyword-promote", "priority": 90},
            {"id": "pricing-spike", "kind": "threshold", "severity": "major",
             "threshold": 0.1, "direction": ">="},
        ],
    })
    rules = load_rules(rules_path)
    snapshot = {item["id"]: item for item in rule_snapshot(rules)}
    # Inherited fields stay: only the named field changed.
    assert snapshot["keyword-promote"]["severity"] == "major"
    assert snapshot["keyword-promote"]["priority"] == 90
    assert snapshot["keyword-promote"]["source"] == "global"
    # A new rule with kind is appended without dropping built-ins.
    assert snapshot["pricing-spike"]["kind"] == "threshold"
    assert len(snapshot) == len(rule_snapshot(Rules())) + 1

    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "rules": [{"id": "keyword-promote", "enabled": False}]})])
    watch_spec = load_watchlist(path)[0]
    merged = effective_rules(rules, watch_spec)
    effective = {item["id"]: item for item in rule_snapshot(merged)}
    assert effective["keyword-promote"]["enabled"] is False
    assert effective["pricing-spike"]["threshold"] == 0.1
    # Other watch items inherit the untouched global chain.
    plain = effective_rules(rules, WatchSpec(name="other",
                                             url="http://127.0.0.1/other",
                                             interval_seconds=60))
    assert {item["id"]: item for item in rule_snapshot(plain)}[
        "keyword-promote"]["enabled"] is True


def test_watch_override_may_omit_id_to_patch_the_named_rule(tmp_path):
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "rules": {"high-change-rate": {"enabled": False}}})])
    watch_spec = load_watchlist(path)[0]
    merged = effective_rules(Rules(), watch_spec)
    effective = {item["id"]: item for item in rule_snapshot(merged)}
    assert effective["high-change-rate"]["enabled"] is False
    assert effective["focus-promote"]["enabled"] is True


def test_rule_override_errors_name_the_rule_and_file(tmp_path):
    rules_path = write_rules(tmp_path, {"version": 1})
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "rules": [{"id": "nope", "enabled": False}]})])
    # Watch patches may reference rules introduced by the global file, so
    # cross-file consistency is checked by load_all, not load_watchlist.
    with pytest.raises(ConfigError, match="unknown rule override id") as error:
        load_all(path, rules_path)
    assert "nope" in str(error.value) and str(path) in str(error.value)

    with pytest.raises(ConfigError, match="needs a threshold"):
        load_rules(write_rules(tmp_path, {
            "rules": [{"id": "new", "kind": "threshold"}]}))
    with pytest.raises(ConfigError, match="unknown rule field"):
        load_rules(write_rules(tmp_path, {
            "rules": [{"id": "focus-promote", "typo": 1}]}))
    with pytest.raises(ConfigError, match="duplicate rule override"):
        load_rules(write_rules(tmp_path, {
            "rules": [{"id": "a", "kind": "default"},
                      {"id": "a", "kind": "default"}]}))
    with pytest.raises(ConfigError, match="kind must be one of"):
        load_rules(write_rules(tmp_path, {
            "rules": [{"id": "a", "kind": "mystery"}]}))
    with pytest.raises(ConfigError, match="threshold must be between"):
        load_rules(write_rules(tmp_path, {
            "rules": [{"id": "high-change-rate", "threshold": 1.5}]}))


def test_rule_thresholds_must_stay_ordered_after_patching(tmp_path):
    with pytest.raises(ConfigError, match="must not exceed"):
        load_rules(write_rules(tmp_path, {
            "classification": {"noise_max_change_rate": 0.02,
                               "major_change_rate": 0.35},
            "rules": [{"id": "high-change-rate", "threshold": 0.01}]}))


def test_watch_thresholds_are_checked_after_merge(tmp_path):
    rules_path = write_rules(tmp_path, {"version": 1})
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "noise_max_change_rate": 0.6, "major_change_rate": 0.4})])
    # Parsing the watch alone is fine; the conflict only appears once merged.
    assert load_watchlist(path)[0].rules_overrides["noise_max_change_rate"] == 0.6
    with pytest.raises(ConfigError, match="must not exceed") as error:
        load_all(path, rules_path)
    assert "example" in str(error.value)


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
