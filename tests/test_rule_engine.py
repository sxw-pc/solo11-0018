"""Rule engine: layering, priority adjudication, ties and trace replay."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from pagewatch.diff.classify import (
    ENGINE_VERSION, RuleConfigError, build_rule_set, classify,
    classify_detailed, effective_thresholds, reasons, replay_severity,
    rule_snapshot, trace_to_json,
)
from pagewatch.models import DiffMetrics, RulePatch, Rules


def metric(**overrides):
    values = dict(
        base_lines=10, new_lines=10, added_lines=1, removed_lines=1,
        changed_lines=2, change_rate=0.2, regions=("#main",),
        focus_hit=False, noise_only=False, keyword_hits=(),
    )
    values.update(overrides)
    return DiffMetrics(**values)


def patch(rule_id, **fields):
    return RulePatch(rule_id=rule_id, source="test", **fields)


def test_engine_reproduces_legacy_classification():
    rules = replace(Rules(), noise_max_change_rate=0.02, major_change_rate=0.5)
    assert classify(metric(change_rate=0.019), rules) == "noise"
    assert classify(metric(change_rate=0.02), rules) == "minor"
    assert classify(metric(change_rate=0.499), rules) == "minor"
    assert classify(metric(change_rate=0.5), rules) == "major"
    assert classify(metric(change_rate=0.001, focus_hit=True), rules) == "major"
    assert classify(metric(change_rate=0.001, keyword_hits=("重要",)), rules) == "major"
    assert classify(metric(change_rate=0.2, noise_only=True), rules) == "noise"
    assert classify(metric(change_rate=0.2, noise_only=True, focus_hit=True),
                    rules) == "major"


def test_trace_is_ordered_labelled_and_replayable():
    metrics = metric(change_rate=0.8, focus_hit=True,
                     keyword_hits=("critical",))
    classification = classify_detailed(metrics, Rules())
    assert classification.severity == "major"
    assert classification.engine_version == ENGINE_VERSION
    # Every rule is reported once; matching ones carry provenance and reasons.
    assert len(classification.trace) == len(build_rule_set(Rules()))
    matched = {verdict.rule_id: verdict for verdict in classification.matched}
    assert matched["focus-promote"].reason == "focus-selector"
    assert matched["keyword-promote"].reason == "keyword:critical"
    assert matched["high-change-rate"].reason == "high-change-rate"
    assert {verdict.source for verdict in classification.trace} == {"default"}
    # Ordering is priority-desc, so the first match is the adjudicated winner.
    priorities = [verdict.priority for verdict in classification.trace]
    assert priorities == sorted(priorities, reverse=True)
    record = trace_to_json(classification)
    assert replay_severity(record) == "major"
    assert json.loads(record)["final"] == "major"


def test_priority_beats_code_order_and_conflicts_are_visible():
    # A focus hit (built-in priority 60) against a custom noise rule that
    # deliberately outranks it: priority wins, not definition order.
    rules = Rules(rules=(patch("quiet-window", kind="threshold", severity="noise",
                               priority=90, threshold=0.9),))
    metrics = metric(change_rate=0.95, focus_hit=True, keyword_hits=("alert",))
    result = classify_detailed(metrics, rules)
    assert result.severity == "noise"
    assert result.trace[0].rule_id == "quiet-window"
    # The superseded majority stays visible in the trace for auditing.
    ids = {verdict.rule_id for verdict in result.matched}
    assert {"quiet-window", "focus-promote", "keyword-promote"} <= ids


def test_tie_is_resolved_by_kind_then_rule_id():
    ties = Rules(rules=(
        patch("aaa-threshold", kind="threshold", severity="noise",
              priority=42, threshold=0.5),
        patch("zzz-focus", kind="focus", severity="major", priority=42),
    ))
    metrics = metric(change_rate=0.6, focus_hit=True)
    first = classify_detailed(metrics, ties)
    second = classify_detailed(metrics, ties)
    # focus ranks before threshold in the fixed kind order.
    assert first.severity == "major"
    assert [verdict.as_dict() for verdict in first.trace] == \
        [verdict.as_dict() for verdict in second.trace]

    same_kind = Rules(rules=(
        patch("rule-b", kind="threshold", severity="minor",
              priority=42, threshold=0.5),
        patch("rule-a", kind="threshold", severity="noise",
              priority=42, threshold=0.5),
    ))
    result = classify_detailed(metric(change_rate=0.6), same_kind)
    assert result.severity == "noise"
    tied = [verdict.rule_id for verdict in result.trace
            if verdict.matched and verdict.priority == 42]
    assert tied == ["rule-a", "rule-b"]


def test_severity_mapping_reacts_to_other_verdicts():
    rules = Rules(rules=(
        patch("focus-and-keyword", kind="severity_mapping", severity="minor",
              priority=80, when=("focus-promote", "keyword-promote")),
    ))
    both = metric(change_rate=0.01, focus_hit=True, keyword_hits=("x",))
    assert classify(both, rules) == "minor"
    only_focus = metric(change_rate=0.01, focus_hit=True)
    assert classify(only_focus, rules) == "major"
    trace = classify_detailed(only_focus, rules).trace
    mapping = next(verdict for verdict in trace
                   if verdict.rule_id == "focus-and-keyword")
    assert mapping.matched is False


def test_disabled_rule_is_reported_but_never_wins():
    rules = Rules(rules=(patch("keyword-promote", enabled=False),))
    classification = classify_detailed(
        metric(change_rate=0.001, keyword_hits=("重要",)), rules)
    assert classification.severity == "noise"
    verdict = next(item for item in classification.trace
                   if item.rule_id == "keyword-promote")
    assert verdict.matched is False
    assert verdict.detail == "disabled by override"
    # Other rules still fire for the same watch.
    assert classify(metric(change_rate=0.001, keyword_hits=("重要",),
                           focus_hit=True), rules) == "major"


def test_patch_inherits_and_retunes_without_replacing_other_rules():
    rules = Rules(rules=(patch("high-change-rate", threshold=0.6),))
    snapshot = {item["id"]: item for item in rule_snapshot(rules)}
    assert snapshot["high-change-rate"]["threshold"] == 0.6
    assert snapshot["high-change-rate"]["severity"] == "major"
    assert len(snapshot) == len(build_rule_set(Rules()))
    assert effective_thresholds(rules) == (0.02, 0.6)
    # Boundary follows the patched value, the rest of the chain is intact.
    assert classify(metric(change_rate=0.59, focus_hit=False), rules) == "minor"
    assert classify(metric(change_rate=0.6), rules) == "major"
    assert classify(metric(change_rate=0.001, focus_hit=True), rules) == "major"


def test_rule_validation_errors():
    with pytest.raises(RuleConfigError, match="needs a threshold"):
        build_rule_set(Rules(rules=(patch("custom", kind="threshold"),)))
    with pytest.raises(RuleConfigError, match="severity"):
        build_rule_set(Rules(rules=(
            patch("custom", kind="default", severity="fatal"),)))
    with pytest.raises(RuleConfigError, match="unknown kind"):
        build_rule_set(Rules(rules=(patch("custom", kind="mystery"),)))
    with pytest.raises(RuleConfigError, match="unknown rule override"):
        build_rule_set(Rules(rules=(patch("custom", enabled=False),)))
    with pytest.raises(RuleConfigError, match="change it to"):
        build_rule_set(Rules(rules=(patch("focus-promote", kind="keyword"),)))
    with pytest.raises(RuleConfigError, match="unknown rule"):
        build_rule_set(Rules(rules=(
            patch("mapping", kind="severity_mapping", severity="minor",
                  when=("missing-rule",)),)))
    with pytest.raises(RuleConfigError, match="needs when"):
        build_rule_set(Rules(rules=(
            patch("mapping", kind="severity_mapping", severity="minor"),)))
    with pytest.raises(ValueError, match="priority"):
        RulePatch(rule_id="bad", source="test", priority=20_000)


def test_replay_ignores_stored_final_and_tolerates_missing_trace():
    classification = classify_detailed(
        metric(change_rate=0.001, focus_hit=True), Rules())
    record = json.loads(trace_to_json(classification))
    record["final"] = "noise"  # tampered stored answer
    assert replay_severity(record) == "major"
    assert replay_severity("") is None
    assert replay_severity("not-json") is None
    assert replay_severity({"verdicts": []}) == "minor"
    assert replay_severity({"no_verdicts": True}) is None


def test_reasons_are_scoped_to_the_winning_level():
    metrics = metric(change_rate=0.001, noise_only=True,
                     keyword_hits=("x",))
    classification = classify_detailed(metrics, Rules())
    # Keyword promotion wins; lower-level noise reasons are not in the summary.
    assert classification.severity == "major"
    values = reasons(metrics, Rules(), "major")
    assert "keyword:x" in values
    assert "noise-region-only" not in values
    assert reasons(metrics, Rules(), "noise") == [
        "below-noise-floor", "noise-region-only"]


def test_structural_rule_marks_insert_delete_as_minor():
    rules = Rules(rules=(patch("below-noise-floor", enabled=False),))
    inserted = metric(change_rate=0.0, block_ops=("insert",), added_blocks=1)
    rewording = metric(change_rate=0.0, block_ops=("replace",))
    assert classify(inserted, rules) == "minor"
    verdict = next(item for item in classify_detailed(inserted, rules).trace
                   if item.rule_id == "block-structure-change")
    assert verdict.matched is True
    assert verdict.detail == "ops=insert"
    assert classify(rewording, rules) == "minor"
