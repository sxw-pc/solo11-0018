"""Composable, auditable severity rule engine.

Every configured rule is evaluated on every change and produces a
:class:`RuleVerdict` carrying its own severity, priority, provenance and
reason.  The final level is the severity of the highest-priority matching
rule; ties are broken by a fixed ``(kind order, rule id)`` sequence so the
outcome is stable for identical inputs.

The built-in rule set reproduces the pre-engine classification exactly, so
events written before the engine existed keep the same status when the same
metrics are replayed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any

from ..models import (
    MAX_RULE_PRIORITY,
    RULE_DIRECTIONS,
    RULE_KINDS,
    SEVERITIES,
    Classification,
    DiffMetrics,
    RulePatch,
    RuleVerdict,
    Rules,
)

ENGINE_VERSION = "rule-engine-v1"

# Higher priority wins.  A user-defined rule may use any value in
# 0..MAX_RULE_PRIORITY, so it can deliberately outrank a built-in rule.
KIND_PRIORITY = {
    "severity_mapping": 70,
    "focus": 60,
    "keyword": 50,
    "threshold": 40,
    "noise_region": 30,
    "block_structure": 25,
    "default": 0,
}

# Deterministic tie-breaker when two matching rules share a priority.
KIND_ORDER = (
    "severity_mapping",
    "focus",
    "keyword",
    "threshold",
    "block_structure",
    "noise_region",
    "default",
)
KIND_RANK = {kind: rank for rank, kind in enumerate(KIND_ORDER)}

FALLBACK_SEVERITY = "minor"
_CLASSIFY_FIELDS = ("kind", "severity", "priority", "enabled", "threshold",
                    "direction", "when", "label")


class RuleConfigError(ValueError):
    """An invalid rule definition or override."""


@dataclass(frozen=True)
class EffectiveRule:
    """A rule after applying every default/global/watch layer."""

    rule_id: str
    kind: str
    severity: str
    priority: int
    source: str
    enabled: bool = True
    threshold: float | None = None
    direction: str = ">="
    when: tuple[str, ...] = ()
    label: str | None = None

    @property
    def order_key(self) -> tuple[int, int, str]:
        return (-self.priority, KIND_RANK.get(self.kind, len(KIND_ORDER)),
                self.rule_id)


def default_rule_set(rules: Rules) -> tuple[EffectiveRule, ...]:
    """The built-in rules, expressed with the legacy configuration fields."""
    return (
        EffectiveRule("focus-promote", "focus", "major",
                      KIND_PRIORITY["focus"], source="default",
                      label="focus-selector"),
        EffectiveRule("keyword-promote", "keyword", "major",
                      KIND_PRIORITY["keyword"], source="default"),
        EffectiveRule("high-change-rate", "threshold", "major",
                      KIND_PRIORITY["threshold"], source="default",
                      threshold=rules.major_change_rate, direction=">=",
                      label="high-change-rate"),
        EffectiveRule("below-noise-floor", "threshold", "noise",
                      KIND_PRIORITY["threshold"], source="default",
                      threshold=rules.noise_max_change_rate, direction="<",
                      label="below-noise-floor"),
        EffectiveRule("noise-region-only", "noise_region", "noise",
                      KIND_PRIORITY["noise_region"], source="default",
                      label="noise-region-only"),
        EffectiveRule("block-structure-change", "block_structure", "minor",
                      KIND_PRIORITY["block_structure"], source="default",
                      label="block-structure-change"),
        EffectiveRule("fallback-minor", "default", "minor",
                      KIND_PRIORITY["default"], source="default",
                      label="content-change"),
    )


def _patch_fields(patch: RulePatch) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in _CLASSIFY_FIELDS:
        value = getattr(patch, name)
        if value is not None:
            values[name] = value
    return values


def _new_rule(patch: RulePatch) -> EffectiveRule:
    if patch.kind is None:
        raise RuleConfigError(
            f"unknown rule override id {patch.rule_id!r}; define it with a kind")
    if patch.kind not in RULE_KINDS:
        raise RuleConfigError(
            f"rule {patch.rule_id!r}: unknown kind {patch.kind!r}")
    priority = (KIND_PRIORITY[patch.kind] if patch.priority is None
                else patch.priority)
    rule = EffectiveRule(
        rule_id=patch.rule_id, kind=patch.kind,
        severity=patch.severity or "minor", priority=priority,
        source=patch.source, enabled=patch.enabled if patch.enabled is not None else True,
        threshold=patch.threshold,
        direction=patch.direction or ">=",
        when=patch.when or (), label=patch.label,
    )
    _validate_rule(rule, require_explicit=patch)
    return rule


def _validate_rule(rule: EffectiveRule, *, require_explicit: RulePatch | None = None) -> None:
    if rule.severity not in SEVERITIES:
        raise RuleConfigError(
            f"rule {rule.rule_id!r}: severity must be one of "
            f"{', '.join(SEVERITIES)}")
    if not 0 <= rule.priority <= MAX_RULE_PRIORITY:
        raise RuleConfigError(
            f"rule {rule.rule_id!r}: priority must be between 0 and "
            f"{MAX_RULE_PRIORITY}")
    if rule.kind == "threshold":
        if rule.threshold is None:
            if require_explicit is None or require_explicit.threshold is None:
                raise RuleConfigError(
                    f"rule {rule.rule_id!r}: threshold rule needs a threshold")
        elif not 0.0 <= rule.threshold <= 1.0:
            raise RuleConfigError(
                f"rule {rule.rule_id!r}: threshold must be between 0 and 1")
        if rule.direction not in RULE_DIRECTIONS:
            raise RuleConfigError(
                f"rule {rule.rule_id!r}: direction must be '>=' or '<'")
    else:
        if rule.threshold is not None or rule.direction != ">=":
            raise RuleConfigError(
                f"rule {rule.rule_id!r}: threshold/direction only apply to "
                "threshold rules")
        if rule.when and rule.kind != "severity_mapping":
            raise RuleConfigError(
                f"rule {rule.rule_id!r}: when only applies to severity_mapping rules")
    if rule.kind == "severity_mapping" and not rule.when:
        raise RuleConfigError(
            f"rule {rule.rule_id!r}: severity_mapping rule needs when")


def build_rule_set(rules: Rules) -> tuple[EffectiveRule, ...]:
    """Resolve defaults, global patches and watch patches by rule id.

    Each layer merges field-by-field: omitted patch fields inherit the value
    from the lower layer, ``enabled: false`` disables a rule without deleting
    it, and an unknown id plus ``kind`` adds a new rule.
    """
    current = {rule.rule_id: rule for rule in default_rule_set(rules)}
    for patch in rules.rules:
        existing = current.get(patch.rule_id)
        if existing is None:
            current[patch.rule_id] = _new_rule(patch)
            continue
        values = _patch_fields(patch)
        kind = values.get("kind")
        if kind is not None and kind != existing.kind:
            raise RuleConfigError(
                f"rule {patch.rule_id!r}: existing rule has kind "
                f"{existing.kind!r}, patch tried to change it to {kind!r}")
        values["source"] = patch.source
        rule = replace(existing, **values)
        _validate_rule(rule)
        current[patch.rule_id] = rule
    for rule in current.values():
        if rule.kind == "severity_mapping":
            missing = [ref for ref in rule.when if ref not in current]
            if missing:
                raise RuleConfigError(
                    f"rule {rule.rule_id!r}: when references unknown rule(s): "
                    + ", ".join(sorted(missing)))
    return tuple(sorted(current.values(), key=lambda rule: rule.order_key))


def effective_thresholds(rules: Rules) -> tuple[float, float]:
    """Resolved ``(noise floor, major threshold)`` after all rule patches.

    The legacy ``classification.*`` scalars seed the built-in threshold rules;
    an id-addressed patch can retune them without touching the scalars, so
    this is the pair to validate, not the raw dataclass fields.
    """
    noise = rules.noise_max_change_rate
    major = rules.major_change_rate
    for rule in build_rule_set(rules):
        if rule.kind != "threshold" or rule.threshold is None:
            continue
        if rule.rule_id == "below-noise-floor":
            noise = rule.threshold
        elif rule.rule_id == "high-change-rate":
            major = rule.threshold
    return noise, major


def _keyword_reason(rule: EffectiveRule, metrics: DiffMetrics) -> str:
    if rule.label:
        return rule.label
    return "keyword:" + ",".join(metrics.keyword_hits)


def _threshold_reason(rule: EffectiveRule) -> str:
    if rule.label:
        return rule.label
    return f"change-rate{rule.direction}{rule.threshold:g}"


def _condition(rule: EffectiveRule,
               metrics: DiffMetrics) -> tuple[bool, str, str]:
    """Return ``(matched, reason, detail)`` for a non-mapping rule."""
    if rule.kind == "focus":
        if metrics.focus_hit:
            regions = ", ".join(metrics.focus_regions) or "focus region"
            return True, rule.label or "focus-selector", f"focus={regions}"
        return False, "", "no focus region touched"
    if rule.kind == "keyword":
        if metrics.keyword_hits:
            return True, _keyword_reason(rule, metrics), \
                "hits=" + ",".join(metrics.keyword_hits)
        return False, "", "no keyword in changed text"
    if rule.kind == "threshold":
        rate = metrics.change_rate
        floor = rule.threshold if rule.threshold is not None else 0.0
        if rule.direction == "<":
            matched = rate < floor
            detail = f"rate={rate:.3f} < {floor:.3f}"
        else:
            matched = rate >= floor
            detail = f"rate={rate:.3f} >= {floor:.3f}"
        return matched, _threshold_reason(rule) if matched else "", detail
    if rule.kind == "noise_region":
        if metrics.noise_only:
            regions = ", ".join(metrics.noise_regions) or "noise region"
            return True, rule.label or "noise-region-only", f"noise={regions}"
        return False, "", "changed blocks are not all noise regions"
    if rule.kind == "block_structure":
        structural = tuple(op for op in metrics.block_ops
                           if op in {"insert", "delete"})
        if structural:
            return True, rule.label or "block-structure-change", \
                "ops=" + ",".join(structural)
        return False, "", "no inserted or deleted blocks"
    # Unconditional fallback rule.
    return True, rule.label or "content-change", "always matches"


def evaluate(rules: Rules) -> tuple[EffectiveRule, ...]:
    """Resolved rule set for a watch, ready for evaluation."""
    return build_rule_set(rules)


def _verdict(rule: EffectiveRule, *, matched: bool, reason: str,
             detail: str = "") -> RuleVerdict:
    return RuleVerdict(
        rule_id=rule.rule_id, kind=rule.kind, source=rule.source,
        severity=rule.severity, priority=rule.priority, matched=matched,
        reason=reason if matched else "", detail=detail,
    )


def classify_detailed(metrics: DiffMetrics, rules: Rules) -> Classification:
    """Evaluate every rule and return the winning level with a full trace."""
    rule_set = evaluate(rules)
    outcomes: dict[str, tuple[bool, str, str]] = {}
    mappings: list[EffectiveRule] = []
    for rule in rule_set:
        if rule.kind == "severity_mapping":
            mappings.append(rule)
            continue
        if not rule.enabled:
            outcomes[rule.rule_id] = (False, "", "disabled by override")
            continue
        outcomes[rule.rule_id] = _condition(rule, metrics)

    # Severity mappings may reference earlier rules, including other
    # mappings.  ``when`` is a conjunction: the mapping fires only when *all*
    # referenced rules matched, which is the useful primitive for capping an
    # otherwise-overdetermined change.  Iterate to a fixed point (bounded by
    # rule count); an unmatched cycle simply stays unmapped, deterministically.
    mapped: dict[str, bool] = {rule.rule_id: False for rule in mappings}

    def matched_ref(ref: str) -> bool:
        if ref in mapped:
            return mapped[ref]
        return outcomes.get(ref, (False, "", ""))[0]

    for _ in range(len(mappings) + 1):
        changed = False
        for rule in mappings:
            if not rule.enabled:
                continue
            value = all(matched_ref(ref) for ref in rule.when)
            if value and not mapped[rule.rule_id]:
                mapped[rule.rule_id] = True
                changed = True
        if not changed:
            break

    trace: list[RuleVerdict] = []
    for rule in rule_set:
        if rule.kind == "severity_mapping":
            if not rule.enabled:
                trace.append(_verdict(rule, matched=False, reason="",
                                      detail="disabled by override"))
            elif mapped[rule.rule_id]:
                trace.append(_verdict(
                    rule, matched=True,
                    reason=rule.label or f"severity-mapping:{rule.rule_id}",
                    detail="when=" + ",".join(rule.when)))
            else:
                trace.append(_verdict(rule, matched=False, reason="",
                                      detail="no referenced rule matched"))
            continue
        matched, reason, detail = outcomes[rule.rule_id]
        trace.append(_verdict(rule, matched=matched, reason=reason, detail=detail))

    winner = next((verdict for verdict in trace if verdict.matched), None)
    severity = winner.severity if winner is not None else FALLBACK_SEVERITY
    return Classification(severity=severity, trace=tuple(trace),
                          engine_version=ENGINE_VERSION)


def classify(metrics: DiffMetrics, rules: Rules) -> str:
    """Compatibility wrapper returning only the final severity."""
    return classify_detailed(metrics, rules).severity


def reasons(metrics: DiffMetrics, rules: Rules,
            severity: str | None = None) -> list[str]:
    """Human-readable labels of matching rules supporting ``severity``."""
    classification = classify_detailed(metrics, rules)
    level = severity if severity is not None else classification.severity
    return [verdict.reason for verdict in classification.matched
            if verdict.severity == level and verdict.reason]


def trace_to_json(classification: Classification) -> str:
    return json.dumps(classification.explain(), ensure_ascii=False,
                      sort_keys=True, separators=(",", ":"))


def replay_severity(record: str | dict[str, Any] | None) -> str | None:
    """Recompute the winning severity from a stored verdict trace.

    This deliberately does not read the stored ``final`` field: replaying the
    verdicts through the same ordering must produce the same answer, which is
    what makes the record auditable.
    """
    if not record:
        return None
    if isinstance(record, str):
        try:
            record = json.loads(record)
        except (TypeError, ValueError):
            return None
    if not isinstance(record, dict):
        return None
    verdicts = record.get("verdicts")
    if not isinstance(verdicts, list):
        return None
    matched = [item for item in verdicts if isinstance(item, dict)
               and item.get("matched")]
    if not matched:
        return FALLBACK_SEVERITY
    winner = min(matched, key=lambda item: (
        -int(item.get("priority", 0)),
        KIND_RANK.get(str(item.get("kind", "")), len(KIND_ORDER)),
        str(item.get("rule_id", "")),
    ))
    return str(winner.get("severity", FALLBACK_SEVERITY))


def rule_snapshot(rules: Rules) -> list[dict[str, Any]]:
    """Flat description of the resolved rule chain (for diagnostics/tests)."""
    return [
        {
            "id": rule.rule_id, "kind": rule.kind, "severity": rule.severity,
            "priority": rule.priority, "enabled": rule.enabled,
            "source": rule.source,
            **({"threshold": rule.threshold} if rule.threshold is not None else {}),
            **({"direction": rule.direction} if rule.kind == "threshold" else {}),
            **({"when": list(rule.when)} if rule.when else {}),
        }
        for rule in build_rule_set(rules)
    ]
