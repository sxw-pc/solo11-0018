from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pagewatch.config import (
    ConfigError, RuleOverrides, WatchSpec, effective_rules, load_all,
    load_rules, load_watchlist, strip_unknown_fields,
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


# ---------------------------------------------------------------------------
# Pydantic migration behaviour
# ---------------------------------------------------------------------------

def test_loaded_objects_are_typed_pydantic_models():
    watches = load_watchlist(ROOT / "configs/watchlist.example.yaml")
    rules = load_rules(ROOT / "configs/rules.example.yaml")
    assert isinstance(watches[0], WatchSpec)
    assert isinstance(rules, Rules)
    assert isinstance(watches[0].rule_overrides, RuleOverrides)
    # Frozen: callers cannot mutate configuration in place.
    with pytest.raises(Exception):
        watches[0].enabled = False


def test_default_list_fields_are_independent_and_immutable():
    first = WatchSpec(name="a", url="http://127.0.0.1/a", interval_seconds=60)
    second = WatchSpec(name="b", url="http://127.0.0.1/b", interval_seconds=60)
    assert first.tags == () and first.keywords == ()
    assert first.rule_overrides is not second.rule_overrides
    with pytest.raises(AttributeError):
        first.tags.append("shared")


def test_error_details_identify_watch_field_and_rule():
    # noise=0.9 is valid on its own but violates the ordering against a
    # stricter global major threshold, so the failure surfaces at merge time.
    bad = WatchSpec(
        name="loc", url="http://127.0.0.1/x", interval_seconds=60,
        rule_overrides=RuleOverrides(noise_max_change_rate=0.9))
    with pytest.raises(ConfigError) as error:
        effective_rules(Rules(major_change_rate=0.5), bad)
    payload = error.value.error_payload()
    assert payload["error"] == "config_validation_failed"
    assert "loc" in str(error.value)
    assert "must not exceed" in str(error.value)


def test_json_payload_aggregates_multiple_errors(tmp_path):
    path = write_watchlist(tmp_path, [watch(interval_seconds=3, enabled="yes")])
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    details = error.value.error_payload()["details"]
    fields = {item["field"] for item in details}
    assert {"interval_seconds", "enabled"} <= fields
    locations = [item["location"] for item in details]
    assert all("watches[0]" in loc for loc in locations)


def test_unknown_fields_rejected_by_default_at_every_level(tmp_path):
    top = tmp_path / "top.yaml"
    top.write_text(yaml.safe_dump({
        "version": 1, "future": 1,
        "watches": [watch()],
    }), encoding="utf-8")
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_watchlist(top)

    nested = write_watchlist(tmp_path, [watch(rule_overrides={"typo_key": 1})])
    with pytest.raises(ConfigError) as error:
        load_watchlist(nested)
    assert "unknown field(s): typo_key" in str(error.value)
    assert "rule_overrides" in error.value.details[0]["location"]


def test_allow_unknown_fields_tolerates_top_and_nested_keys(tmp_path):
    path = tmp_path / "tolerant.yaml"
    path.write_text(yaml.safe_dump({
        "version": 1, "future_top": True,
        "watches": [watch(unknown_watch_key=9, rule_overrides={
            "max_workers": 2, "future_override": "x"})],
    }), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_watchlist(path)
    watches = load_watchlist(path, allow_unknown_fields=True)
    assert watches[0].rule_overrides.max_workers == 2
    assert watches[0].rule_overrides.merged_values() == {"max_workers": 2}


def test_allow_unknown_fields_tolerates_rules_sections(tmp_path):
    path = write_rules(tmp_path, {
        "future_section": {"x": 1},
        "schedule": {"max_workers": 1, "future_knob": 2},
    })
    with pytest.raises(ConfigError, match="unknown top-level"):
        load_rules(path)
    rules = load_rules(path, allow_unknown_fields=True)
    assert rules.max_workers == 1


def test_deep_merge_keeps_unrelated_overrides_and_global_defaults():
    base = Rules()
    watch_spec = WatchSpec(
        name="w", url="http://127.0.0.1/w", interval_seconds=60,
        rule_overrides=RuleOverrides(
            major_change_rate=0.5, focus_selectors=("#notice",)))
    merged = effective_rules(base, watch_spec)
    assert merged.major_change_rate == 0.5
    assert merged.focus_selectors == ("#notice",)
    # Unmentioned global rules survive the merge.
    assert merged.max_workers == base.max_workers
    assert merged.alert_cooldown_seconds == base.alert_cooldown_seconds
    # The global base is never mutated.
    assert base.major_change_rate == 0.35 and base.focus_selectors == ()


def test_partial_override_vs_global_base_rate_ordering_fails_at_load(tmp_path):
    # Override lowers major below the global noise floor.
    path = write_watchlist(tmp_path, [watch(rule_overrides={
        "major_change_rate": 0.001})])
    rules_path = write_rules(tmp_path, {"classification": {
        "noise_max_change_rate": 0.5}})
    with pytest.raises(ConfigError, match="must not exceed"):
        load_all(path, rules_path)


def test_no_overrides_returns_same_rules_instance():
    base = Rules()
    watch_spec = WatchSpec(name="w", url="http://127.0.0.1/w",
                           interval_seconds=60)
    assert effective_rules(base, watch_spec) is base


def test_explicit_null_override_is_rejected_but_null_watch_fields_default(tmp_path):
    path = write_watchlist(tmp_path, [
        watch(rule_overrides={"max_workers": None})])
    with pytest.raises(ConfigError, match="must not be null"):
        load_watchlist(path)

    nulls = tmp_path / "nulls.yaml"
    nulls.write_text(yaml.safe_dump({
        "version": 1,
        "watches": [dict(watch(), tags=None, keywords=None,
                         rule_overrides=None)],
    }), encoding="utf-8")
    loaded = load_watchlist(nulls)[0]
    assert loaded.tags == () and loaded.keywords == ()
    assert loaded.rule_overrides.merged_values() == {}


def test_strip_unknown_fields_recurses_into_nested_models():
    raw = {
        "name": "w", "bogus": 1,
        "rule_overrides": {"max_workers": 2, "nested_bogus": 3},
    }
    cleaned = strip_unknown_fields(raw, WatchSpec)
    assert "bogus" not in cleaned
    assert cleaned["rule_overrides"] == {"max_workers": 2}
