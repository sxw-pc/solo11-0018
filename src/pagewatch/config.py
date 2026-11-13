"""YAML configuration loading and complete validation."""

from __future__ import annotations

import re
from dataclasses import fields, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import soupsieve
import yaml

from .fetch.allowlist import AllowlistError, ensure_url_allowed
from .models import Rules, WatchSpec

MIN_INTERVAL_SECONDS = 10
RULE_FIELDS = {item.name for item in fields(Rules)}
WATCH_FIELDS = {
    "name", "url", "interval_seconds", "tags", "include_selectors",
    "exclude_selectors", "noise_patterns", "keywords", "enabled",
    "rule_overrides",
}


class ConfigError(ValueError):
    """A configuration error with a human-readable location."""

    def __init__(self, message: str, path: str | Path | None = None,
                 location: str | None = None) -> None:
        prefix = str(path) if path else "config"
        if location:
            prefix = f"{prefix}: {location}"
        super().__init__(f"{prefix}: {message}")
        self.path = str(path) if path else None
        self.location = location


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read file: {exc}", path) from exc
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML: {exc}", path) from exc
    if not isinstance(data, dict):
        raise ConfigError("top-level value must be a mapping", path)
    return data


def _strings(value: Any, path: Path, location: str, field: str,
             default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if value is None:
        return default
    if not isinstance(value, list) or any(not isinstance(v, str) or not v for v in value):
        raise ConfigError(f"{field} must be a list of non-empty strings", path, location)
    return tuple(value)


def _int(value: Any, path: Path, location: str, field: str,
         minimum: int, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{field} must be an integer >= {minimum}", path, location)
    return value


def _number(value: Any, path: Path, location: str, field: str,
            minimum: float, maximum: float, default: float | None = None) -> float:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{field} must be a number", path, location)
    number = float(value)
    if not minimum <= number <= maximum:
        raise ConfigError(f"{field} must be between {minimum} and {maximum}",
                          path, location)
    return number


def _compile_selectors(selectors: tuple[str, ...], path: Path, location: str) -> None:
    for selector in selectors:
        try:
            soupsieve.compile(selector)
        except Exception as exc:  # soupsieve exposes several syntax errors
            raise ConfigError(f"invalid CSS selector {selector!r}: {exc}",
                              path, location) from exc


def _validate_url(url: str, path: Path, location: str) -> None:
    if not isinstance(url, str) or not url or any(ch.isspace() for ch in url):
        raise ConfigError("url must be a non-empty string without whitespace", path, location)
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ConfigError("url must be an absolute http(s) URL", path, location)
    if parts.username or parts.password:
        raise ConfigError("url must not contain credentials", path, location)
    try:
        parts.port
    except ValueError as exc:
        raise ConfigError(f"url has an invalid port: {exc}", path, location) from exc
    try:
        ensure_url_allowed(url)
    except AllowlistError as exc:
        raise ConfigError(str(exc), path, location) from exc


def _validate_regexes(patterns: tuple[str, ...], path: Path, location: str) -> None:
    for pattern in patterns:
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"invalid noise pattern {pattern!r}: {exc}",
                              path, location) from exc


def _watch(data: Any, path: Path, index: int) -> WatchSpec:
    location = f"watches[{index}]"
    if not isinstance(data, dict):
        raise ConfigError("watch entry must be a mapping", path, location)
    unknown = set(data) - WATCH_FIELDS
    if unknown:
        raise ConfigError(f"unknown field(s): {', '.join(sorted(unknown))}", path, location)
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("name is required and must be a non-empty string", path, location)
    location = f"watches[{index}] ({name})"
    url = data.get("url")
    _validate_url(url, path, location)
    interval = _int(data.get("interval_seconds"), path, location,
                    "interval_seconds", MIN_INTERVAL_SECONDS)
    tags = _strings(data.get("tags"), path, location, "tags")
    includes = _strings(data.get("include_selectors"), path, location,
                        "include_selectors")
    excludes = _strings(data.get("exclude_selectors"), path, location,
                        "exclude_selectors")
    patterns = _strings(data.get("noise_patterns"), path, location, "noise_patterns")
    keywords = _strings(data.get("keywords"), path, location, "keywords")
    _compile_selectors(includes + excludes, path, location)
    _validate_regexes(patterns, path, location)
    enabled = data.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ConfigError("enabled must be a boolean", path, location)
    overrides = data.get("rule_overrides") or {}
    if not isinstance(overrides, dict):
        raise ConfigError("rule_overrides must be a mapping", path, location)
    unknown_rules = set(overrides) - RULE_FIELDS
    if unknown_rules:
        raise ConfigError("unknown rule override(s): " +
                          ", ".join(sorted(unknown_rules)), path, location)
    overrides = _clean_overrides(overrides, path, location)
    return WatchSpec(
        name=name.strip(), url=url, interval_seconds=interval, tags=tags,
        include_selectors=includes, exclude_selectors=excludes,
        noise_patterns=patterns, keywords=keywords, enabled=enabled,
        rules_overrides=overrides,
    )


def _clean_overrides(values: dict[str, Any], path: Path, location: str) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    sequence_fields = {"focus_selectors", "alert_severities"}
    for key, value in values.items():
        if key in sequence_fields:
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ConfigError(f"{key} must be a list of strings", path, location)
            if key == "focus_selectors":
                _compile_selectors(tuple(value), path, location)
            elif any(v not in {"noise", "minor", "major"} for v in value):
                raise ConfigError(f"{key} values must be noise, minor or major",
                                  path, location)
            cleaned[key] = tuple(value)
        elif key in {"major_keep_forever"}:
            if not isinstance(value, bool):
                raise ConfigError(f"{key} must be a boolean", path, location)
            cleaned[key] = value
        elif key == "timeout_seconds":
            cleaned[key] = _number(value, path, location, key, 0.1, 300)
        elif key == "max_bytes":
            cleaned[key] = _int(value, path, location, key, 1024)
        elif key in {"noise_max_change_rate", "major_change_rate"}:
            cleaned[key] = _number(value, path, location, key, 0.0, 1.0)
        elif key in RULE_FIELDS:
            minimum = 0 if key in {"alert_cooldown_seconds", "jitter_seconds",
                                   "archive_max_age_days"} else 1
            cleaned[key] = _int(value, path, location, key, minimum)
    return cleaned


def load_watchlist(path: str | Path) -> list[WatchSpec]:
    path = Path(path)
    data = _load_yaml(path)
    unknown = set(data) - {"version", "watches"}
    if unknown:
        raise ConfigError(f"unknown top-level field(s): {', '.join(sorted(unknown))}", path)
    watches_data = data.get("watches")
    if not isinstance(watches_data, list) or not watches_data:
        raise ConfigError("watches must be a non-empty list", path)
    watches = [_watch(item, path, i) for i, item in enumerate(watches_data)]
    names = [watch.name for watch in watches]
    duplicate = next((name for name in names if names.count(name) > 1), None)
    if duplicate:
        raise ConfigError(f"duplicate watch name: {duplicate}", path)
    return watches


def _rules_from_data(data: dict[str, Any], path: Path) -> Rules:
    classification = data.get("classification", {})
    alerts = data.get("alerts", {})
    diff = data.get("diff", {})
    retention = data.get("retention", {})
    schedule = data.get("schedule", {})
    fetch = data.get("fetch", {})
    section_keys = {
        "classification": {"noise_max_change_rate", "major_change_rate",
                           "focus_selectors"},
        "alerts": {"severities", "cooldown_seconds"},
        "diff": {"max_chars"},
        "retention": {"keep_last", "major_keep_forever", "archive_max_age_days",
                      "quarantine_rebaseline_limit"},
        "schedule": {"max_workers", "jitter_seconds"},
        "fetch": {"timeout_seconds", "max_bytes"},
    }
    for label, value in (("classification", classification), ("alerts", alerts),
                         ("diff", diff), ("retention", retention),
                         ("schedule", schedule), ("fetch", fetch)):
        if not isinstance(value, dict):
            raise ConfigError(f"{label} must be a mapping", path, label)
        unknown = set(value) - section_keys[label]
        if unknown:
            raise ConfigError(f"unknown field(s): {', '.join(sorted(unknown))}",
                              path, label)
    focus = _strings(classification.get("focus_selectors"), path,
                     "classification.focus_selectors", "focus_selectors")
    _compile_selectors(focus, path, "classification.focus_selectors")
    severities = _strings(alerts.get("severities"), path, "alerts.severities",
                          "severities", ("major",))
    if any(severity not in {"noise", "minor", "major"} for severity in severities):
        raise ConfigError("alerts.severities values must be noise, minor or major", path)
    rules = Rules(
        noise_max_change_rate=_number(
            classification.get("noise_max_change_rate"), path,
            "classification.noise_max_change_rate", "noise_max_change_rate", 0, 1, 0.02),
        major_change_rate=_number(
            classification.get("major_change_rate"), path,
            "classification.major_change_rate", "major_change_rate", 0.01, 1, 0.35),
        focus_selectors=focus,
        alert_severities=severities,
        alert_cooldown_seconds=_int(
            alerts.get("cooldown_seconds"), path, "alerts.cooldown_seconds",
            "cooldown_seconds", 0, 3600),
        diff_max_chars=_int(diff.get("max_chars"), path, "diff.max_chars",
                            "max_chars", 100, 12_000),
        keep_last=_int(retention.get("keep_last"), path, "retention.keep_last",
                       "keep_last", 1, 5),
        major_keep_forever=retention.get("major_keep_forever", True),
        archive_max_age_days=_int(
            retention.get("archive_max_age_days"), path,
            "retention.archive_max_age_days", "archive_max_age_days", 0, 30),
        quarantine_rebaseline_limit=_int(
            retention.get("quarantine_rebaseline_limit", 3), path,
            "retention.quarantine_rebaseline_limit",
            "quarantine_rebaseline_limit", 1),
        max_workers=_int(schedule.get("max_workers"), path, "schedule.max_workers",
                         "max_workers", 1, 4),
        jitter_seconds=_int(schedule.get("jitter_seconds"), path,
                            "schedule.jitter_seconds", "jitter_seconds", 0, 10),
        timeout_seconds=_number(fetch.get("timeout_seconds"), path,
                                "fetch.timeout_seconds", "timeout_seconds",
                                0.1, 300, 5.0),
        max_bytes=_int(fetch.get("max_bytes"), path, "fetch.max_bytes",
                       "max_bytes", 1024, 2_097_152),
    )
    if not isinstance(rules.major_keep_forever, bool):
        raise ConfigError("retention.major_keep_forever must be a boolean", path)
    if rules.noise_max_change_rate > rules.major_change_rate:
        raise ConfigError("noise_max_change_rate must not exceed major_change_rate", path)
    return rules


def load_rules(path: str | Path) -> Rules:
    path = Path(path)
    data = _load_yaml(path)
    unknown = set(data) - {"version", "classification", "alerts", "diff",
                           "retention", "schedule", "fetch"}
    if unknown:
        raise ConfigError(f"unknown top-level field(s): {', '.join(sorted(unknown))}", path)
    return _rules_from_data(data, path)


def effective_rules(base: Rules, watch: WatchSpec) -> Rules:
    """Merge a watch's validated overrides onto the global rule set."""
    if not watch.rules_overrides:
        return base
    values = dict(watch.rules_overrides)
    return replace(base, **values)


def load_all(watchlist_path: str | Path, rules_path: str | Path) -> tuple[list[WatchSpec], Rules]:
    return load_watchlist(watchlist_path), load_rules(rules_path)

