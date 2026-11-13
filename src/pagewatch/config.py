"""Typed YAML configuration: Pydantic v2 models, loading and rule merging.

The public configuration objects (:class:`WatchSpec`, :class:`Rules` and
:class:`RuleOverrides`) are frozen Pydantic models.  YAML files are validated
against them and every error is translated into a :class:`ConfigError` that
identifies the offending watch, field or rules section.

Unknown fields are rejected by default; ``allow_unknown_fields=True`` (or the
``--allow-unknown-fields`` CLI flag) drops them instead so upgrades cannot
block a deployment on data the running version does not know about.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import soupsieve
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .fetch.allowlist import AllowlistError, ensure_url_allowed

MIN_INTERVAL_SECONDS = 10
ALERT_SEVERITIES = ("noise", "minor", "major")


class ConfigError(ValueError):
    """A configuration error with a human-readable location."""

    def __init__(self, message: str, path: str | Path | None = None,
                 location: str | None = None,
                 details: list[dict[str, Any]] | None = None) -> None:
        prefix = str(path) if path else "config"
        if location:
            prefix = f"{prefix}: {location}"
        super().__init__(f"{prefix}: {message}")
        self.path = str(path) if path else None
        self.location = location
        self.message = message
        self.details: list[dict[str, Any]] = details or [{
            "location": location, "message": message,
        }]

    def error_payload(self) -> dict[str, Any]:
        """Machine-readable representation used by the ``--json-errors`` flag."""
        return {
            "error": "config_validation_failed",
            "path": self.path,
            "details": self.details,
        }


class _StrictModel(BaseModel):
    """Base for every configuration model: frozen and intolerant of typos."""

    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# Shared low-level validators
# ---------------------------------------------------------------------------

def _reject_bool(value: Any, field: str, article: str = "an integer") -> Any:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be {article}")
    return value


def _non_empty_strings(value: Any, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)) or any(
            not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} must be a list of non-empty strings")
    return tuple(value)


def _string_list(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not all(
            isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a list of strings")
    return tuple(value)


def _compile_selectors(selectors: tuple[str, ...], field: str = "selectors") -> tuple[str, ...]:
    for selector in selectors:
        try:
            soupsieve.compile(selector)
        except Exception as exc:  # soupsieve exposes several syntax errors
            raise ValueError(f"invalid CSS selector {selector!r}: {exc}") from exc
    return selectors


def _compile_regexes(patterns: tuple[str, ...]) -> tuple[str, ...]:
    for pattern in patterns:
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"invalid noise pattern {pattern!r}: {exc}") from exc
    return patterns


def _validate_url(url: Any) -> str:
    """Structural URL checks shared by the model and the loader."""
    if not isinstance(url, str) or not url or any(ch.isspace() for ch in url):
        raise ValueError("url must be a non-empty string without whitespace")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError("url must be an absolute http(s) URL")
    if parts.username or parts.password:
        raise ValueError("url must not contain credentials")
    try:
        parts.port
    except ValueError as exc:
        raise ValueError(f"url has an invalid port: {exc}") from exc
    return url


def _enforce_allowlist(url: str) -> None:
    """Policy check applied while loading YAML, not on direct construction."""
    try:
        ensure_url_allowed(url)
    except AllowlistError as exc:
        raise ValueError(str(exc)) from exc


def _validate_severities(value: tuple[str, ...], field: str) -> tuple[str, ...]:
    if any(item not in ALERT_SEVERITIES for item in value):
        raise ValueError(f"{field} values must be noise, minor or major")
    return value


# ---------------------------------------------------------------------------
# Rule overrides (per-watch nested model)
# ---------------------------------------------------------------------------

class RuleOverrides(_StrictModel):
    """Subset of :class:`Rules` a single watch is allowed to override.

    Every field defaults to ``None``; only fields present in the YAML are
    tracked in :attr:`model_fields_set`, which drives the deep merge in
    :func:`effective_rules` so omitted keys never overwrite global rules.
    """

    noise_max_change_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    major_change_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    focus_selectors: tuple[str, ...] | None = None
    alert_severities: tuple[str, ...] | None = None
    alert_cooldown_seconds: int | None = Field(default=None, ge=0)
    diff_max_chars: int | None = Field(default=None, ge=100)
    keep_last: int | None = Field(default=None, ge=1)
    major_keep_forever: bool | None = None
    archive_max_age_days: int | None = Field(default=None, ge=0)
    max_workers: int | None = Field(default=None, ge=1)
    jitter_seconds: int | None = Field(default=None, ge=0)
    timeout_seconds: float | None = Field(default=None, ge=0.1, le=300)
    max_bytes: int | None = Field(default=None, ge=1024)

    @model_validator(mode="before")
    @classmethod
    def _reject_explicit_null(cls, data: Any) -> Any:
        if isinstance(data, dict):
            for key, value in data.items():
                if value is None and key in cls.model_fields:
                    raise ValueError(
                        f"{key} must not be null; omit the key to keep the global value")
        return data

    @field_validator(
        "alert_cooldown_seconds", "diff_max_chars", "keep_last",
        "archive_max_age_days", "max_workers", "jitter_seconds", "max_bytes",
        mode="before",
    )
    @classmethod
    def _integer_fields_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name)

    @field_validator("noise_max_change_rate", "major_change_rate",
                     "timeout_seconds", mode="before")
    @classmethod
    def _number_fields_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name, article="a number")

    @field_validator("major_keep_forever", mode="before")
    @classmethod
    def _bool_field(cls, value: Any) -> Any:
        if not isinstance(value, bool):
            raise ValueError("major_keep_forever must be a boolean")
        return value

    @field_validator("focus_selectors", mode="before")
    @classmethod
    def _focus_is_string_list(cls, value: Any) -> Any:
        if value is not None:
            value = _string_list(value, "focus_selectors")
        return value

    @field_validator("focus_selectors", mode="after")
    @classmethod
    def _focus_selectors_compile(cls, value: tuple[str, ...] | None) -> Any:
        if value is not None:
            _compile_selectors(value)
        return value

    @field_validator("alert_severities", mode="before")
    @classmethod
    def _severities_are_strings(cls, value: Any) -> Any:
        if value is not None:
            value = _string_list(value, "alert_severities")
            _validate_severities(value, "alert_severities")
        return value

    @model_validator(mode="after")
    def _cross_field_rates(self) -> "RuleOverrides":
        if (self.noise_max_change_rate is not None
                and self.major_change_rate is not None
                and self.noise_max_change_rate > self.major_change_rate):
            raise ValueError(
                "noise_max_change_rate must not exceed major_change_rate")
        return self

    def merged_values(self) -> dict[str, Any]:
        """Return only the explicitly provided override values."""
        return {name: getattr(self, name) for name in self.model_fields_set}


# ---------------------------------------------------------------------------
# Watchlist models
# ---------------------------------------------------------------------------

class WatchSpec(_StrictModel):
    """One monitored page, as declared in ``watchlist.yaml``."""

    name: str
    url: str
    interval_seconds: int = Field(ge=MIN_INTERVAL_SECONDS)
    tags: tuple[str, ...] = Field(default_factory=tuple)
    include_selectors: tuple[str, ...] = Field(default_factory=tuple)
    exclude_selectors: tuple[str, ...] = Field(default_factory=tuple)
    noise_patterns: tuple[str, ...] = Field(default_factory=tuple)
    keywords: tuple[str, ...] = Field(default_factory=tuple)
    enabled: bool = True
    rule_overrides: RuleOverrides = Field(default_factory=RuleOverrides)

    @model_validator(mode="before")
    @classmethod
    def _watch_must_be_mapping(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("watch entry must be a mapping")
        if "rule_overrides" in data and data["rule_overrides"] is None:
            # Legacy loader treated an explicit null as "no overrides".
            data = dict(data)
            data["rule_overrides"] = {}
        elif ("rule_overrides" in data and data["rule_overrides"] is not None
                and not isinstance(data["rule_overrides"], (dict, RuleOverrides))):
            raise ValueError("rule_overrides must be a mapping")
        return data

    @field_validator("name", mode="before")
    @classmethod
    def _name_valid(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("name is required and must be a non-empty string")
        return value.strip()

    @field_validator("url")
    @classmethod
    def _url_valid(cls, value: Any) -> str:
        return _validate_url(value)

    @field_validator("interval_seconds", mode="before")
    @classmethod
    def _interval_not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "interval_seconds")

    @field_validator("enabled", mode="before")
    @classmethod
    def _enabled_is_bool(cls, value: Any) -> Any:
        if not isinstance(value, bool):
            raise ValueError("enabled must be a boolean")
        return value

    @field_validator("tags", "include_selectors", "exclude_selectors",
                     "noise_patterns", "keywords", mode="before")
    @classmethod
    def _string_sequence(cls, value: Any, info: Any) -> Any:
        return _non_empty_strings(value, info.field_name)

    @field_validator("include_selectors", "exclude_selectors", mode="after")
    @classmethod
    def _selectors_compile(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _compile_selectors(value)

    @field_validator("noise_patterns", mode="after")
    @classmethod
    def _patterns_compile(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _compile_regexes(value)


class WatchlistFile(_StrictModel):
    """Top-level shape of ``watchlist.yaml``; watches are then parsed one by
    one so validation errors can name the offending watch entry."""

    version: int = 1
    watches: list[dict[str, Any]]

    @field_validator("version", mode="before")
    @classmethod
    def _version_not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "version")

    @model_validator(mode="after")
    def _unique_watch_names(self) -> "WatchlistFile":
        names = [item.get("name") for item in self.watches if isinstance(item, dict)]
        duplicate = next((name for name in names
                          if isinstance(name, str) and names.count(name) > 1), None)
        if duplicate is not None:
            raise ValueError(f"duplicate watch name: {duplicate}")
        return self


# ---------------------------------------------------------------------------
# Rules models (nested sections plus the flat runtime object)
# ---------------------------------------------------------------------------

class ClassificationSettings(_StrictModel):
    noise_max_change_rate: float = Field(default=0.02, ge=0.0, le=1.0)
    major_change_rate: float = Field(default=0.35, ge=0.01, le=1.0)
    focus_selectors: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("noise_max_change_rate", "major_change_rate", mode="before")
    @classmethod
    def _not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name, article="a number")

    @field_validator("focus_selectors", mode="before")
    @classmethod
    def _strings(cls, value: Any) -> Any:
        return _non_empty_strings(value, "focus_selectors")

    @field_validator("focus_selectors", mode="after")
    @classmethod
    def _selectors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _compile_selectors(value)

    @model_validator(mode="after")
    def _rate_ordering(self) -> "ClassificationSettings":
        if self.noise_max_change_rate > self.major_change_rate:
            raise ValueError(
                "noise_max_change_rate must not exceed major_change_rate")
        return self


class AlertSettings(_StrictModel):
    severities: tuple[str, ...] = ("major",)
    cooldown_seconds: int = Field(default=3600, ge=0)

    @field_validator("cooldown_seconds", mode="before")
    @classmethod
    def _not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "cooldown_seconds")

    @field_validator("severities", mode="before")
    @classmethod
    def _strings(cls, value: Any) -> Any:
        value = _non_empty_strings(value, "severities")
        return _validate_severities(value, "severities")


class DiffSettings(_StrictModel):
    max_chars: int = Field(default=12_000, ge=100)

    @field_validator("max_chars", mode="before")
    @classmethod
    def _not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "max_chars")


class RetentionSettings(_StrictModel):
    keep_last: int = Field(default=5, ge=1)
    major_keep_forever: bool = True
    archive_max_age_days: int = Field(default=30, ge=0)

    @field_validator("keep_last", "archive_max_age_days", mode="before")
    @classmethod
    def _ints_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name)

    @field_validator("major_keep_forever", mode="before")
    @classmethod
    def _bool_only(cls, value: Any) -> Any:
        if not isinstance(value, bool):
            raise ValueError("major_keep_forever must be a boolean")
        return value


class ScheduleSettings(_StrictModel):
    max_workers: int = Field(default=4, ge=1)
    jitter_seconds: int = Field(default=10, ge=0)

    @field_validator("max_workers", "jitter_seconds", mode="before")
    @classmethod
    def _ints_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name)


class FetchSettings(_StrictModel):
    timeout_seconds: float = Field(default=5.0, ge=0.1, le=300)
    max_bytes: int = Field(default=2_097_152, ge=1024)

    @field_validator("timeout_seconds", mode="before")
    @classmethod
    def _number_not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "timeout_seconds", article="a number")

    @field_validator("max_bytes", mode="before")
    @classmethod
    def _int_not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "max_bytes")


class RulesFile(_StrictModel):
    version: int = 1
    classification: ClassificationSettings = Field(default_factory=ClassificationSettings)
    alerts: AlertSettings = Field(default_factory=AlertSettings)
    diff: DiffSettings = Field(default_factory=DiffSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)
    schedule: ScheduleSettings = Field(default_factory=ScheduleSettings)
    fetch: FetchSettings = Field(default_factory=FetchSettings)

    @model_validator(mode="before")
    @classmethod
    def _null_sections_become_defaults(cls, data: Any) -> Any:
        # Legacy loader read every section via data.get(section, {}), so an
        # explicit null section meant "all defaults".
        if isinstance(data, dict):
            data = {key: ({} if value is None and key in cls.model_fields
                          and key != "version" else value)
                    for key, value in data.items()}
        return data

    @field_validator("version", mode="before")
    @classmethod
    def _version_not_bool(cls, value: Any) -> Any:
        return _reject_bool(value, "version")

    def to_rules(self) -> "Rules":
        return Rules(
            noise_max_change_rate=self.classification.noise_max_change_rate,
            major_change_rate=self.classification.major_change_rate,
            focus_selectors=self.classification.focus_selectors,
            alert_severities=self.alerts.severities,
            alert_cooldown_seconds=self.alerts.cooldown_seconds,
            diff_max_chars=self.diff.max_chars,
            keep_last=self.retention.keep_last,
            major_keep_forever=self.retention.major_keep_forever,
            archive_max_age_days=self.retention.archive_max_age_days,
            max_workers=self.schedule.max_workers,
            jitter_seconds=self.schedule.jitter_seconds,
            timeout_seconds=self.fetch.timeout_seconds,
            max_bytes=self.fetch.max_bytes,
        )


class Rules(_StrictModel):
    """Flat, runtime rule set consumed by the runner and the alert/diff layer."""

    noise_max_change_rate: float = Field(default=0.02, ge=0.0, le=1.0)
    major_change_rate: float = Field(default=0.35, ge=0.01, le=1.0)
    focus_selectors: tuple[str, ...] = Field(default_factory=tuple)
    alert_severities: tuple[str, ...] = ("major",)
    alert_cooldown_seconds: int = Field(default=3600, ge=0)
    diff_max_chars: int = Field(default=12_000, ge=100)
    keep_last: int = Field(default=5, ge=1)
    major_keep_forever: bool = True
    archive_max_age_days: int = Field(default=30, ge=0)
    max_workers: int = Field(default=4, ge=1)
    jitter_seconds: int = Field(default=10, ge=0)
    timeout_seconds: float = Field(default=5.0, ge=0.1, le=300)
    max_bytes: int = Field(default=2_097_152, ge=1024)

    @field_validator(
        "alert_cooldown_seconds", "diff_max_chars", "keep_last",
        "archive_max_age_days", "max_workers", "jitter_seconds", "max_bytes",
        mode="before",
    )
    @classmethod
    def _ints_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name)

    @field_validator("noise_max_change_rate", "major_change_rate",
                     "timeout_seconds", mode="before")
    @classmethod
    def _numbers_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool(value, info.field_name, article="a number")

    @field_validator("major_keep_forever", mode="before")
    @classmethod
    def _bool_only(cls, value: Any) -> Any:
        if not isinstance(value, bool):
            raise ValueError("major_keep_forever must be a boolean")
        return value

    @field_validator("alert_severities", mode="before")
    @classmethod
    def _severities(cls, value: Any) -> Any:
        return _validate_severities(tuple(value), "alert_severities")

    @model_validator(mode="after")
    def _rate_ordering(self) -> "Rules":
        if self.noise_max_change_rate > self.major_change_rate:
            raise ValueError(
                "noise_max_change_rate must not exceed major_change_rate")
        return self


# ---------------------------------------------------------------------------
# Unknown-field tolerance
# ---------------------------------------------------------------------------

def _nested_model(annotation: Any) -> type[BaseModel] | None:
    """Resolve a nested :class:`BaseModel` from a field annotation."""
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for arg in getattr(annotation, "__args__", ()):
        found = _nested_model(arg)
        if found is not None:
            return found
    return None


def strip_unknown_fields(data: Any, model_cls: type[BaseModel]) -> Any:
    """Recursively drop keys the model does not declare (tolerant mode)."""
    if not isinstance(data, dict):
        return data
    cleaned: dict[str, Any] = {}
    for key, value in data.items():
        if key not in model_cls.model_fields:
            continue
        nested = _nested_model(model_cls.model_fields[key].annotation)
        if nested is None:
            cleaned[key] = value
        elif isinstance(value, list):
            cleaned[key] = [strip_unknown_fields(item, nested)
                            if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            cleaned[key] = strip_unknown_fields(value, nested)
        else:
            cleaned[key] = value
    return cleaned


# ---------------------------------------------------------------------------
# Pydantic error -> ConfigError translation
# ---------------------------------------------------------------------------

def _render_location(loc: tuple[Any, ...]) -> str:
    return ".".join(str(part) for part in loc)


_BOUND_RANGES: dict[str, tuple[float, float]] = {
    "noise_max_change_rate": (0.0, 1.0),
    "major_change_rate": (0.01, 1.0),
    "timeout_seconds": (0.1, 300.0),
}


def _bound_message(err: dict[str, Any], field: str) -> str:
    ctx = err.get("ctx") or {}
    if field in _BOUND_RANGES:
        low, high = _BOUND_RANGES[field]
        return f"{field} must be between {low} and {high}"
    if "ge" in ctx:
        return f"{field} must be an integer >= {ctx['ge']}"
    if "le" in ctx:
        return f"{field} must be an integer <= {ctx['le']}"
    return err["msg"]


def _render_error(err: dict[str, Any]) -> str:
    err_type = err["type"]
    loc = _render_location(err["loc"])
    field = str(err["loc"][-1]) if err["loc"] else "value"
    if err_type == "extra_forbidden":
        return f"unknown field(s): {field}"
    if err_type == "missing":
        return f"{loc} is required"
    if err_type == "value_error":
        ctx = err.get("ctx") or {}
        nested = ctx.get("error")
        if nested is not None and nested.args:
            return str(nested.args[0])
        return err["msg"]
    if err_type in {"int_type", "int_parsing", "int_from_float"}:
        return f"{field} must be an integer"
    if err_type in {"float_type", "float_parsing"}:
        return f"{field} must be a number"
    if err_type == "bool_type":
        return f"{field} must be a boolean"
    if err_type in {"tuple_type", "list_type"}:
        return f"{field} must be a list"
    if err_type == "string_type":
        return f"{field} must be a non-empty string"
    if err_type in {"greater_than_equal", "greater_than",
                    "less_than_equal", "less_than"}:
        return _bound_message(err, field)
    return err["msg"]


def _raise_validation_error(error: ValidationError, path: Path,
                            location_prefix: str | None = None) -> None:
    details: list[dict[str, Any]] = []
    for err in error.errors():
        loc = _render_location(err["loc"])
        full_location = ".".join(part for part in (location_prefix, loc) if part)
        message = _render_error(err)
        details.append({
            "location": full_location or None,
            "field": loc or None,
            "type": err["type"],
            "message": message,
        })
    first = details[0]
    raise ConfigError(first["message"], path, first["location"], details) from error


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

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


def _prepare(data: dict[str, Any], model_cls: type[BaseModel],
             allow_unknown_fields: bool) -> dict[str, Any]:
    if allow_unknown_fields:
        return strip_unknown_fields(data, model_cls)
    return data


def _reject_top_level_unknown(data: dict[str, Any], allowed: set[str],
                              path: Path) -> None:
    unknown = set(data) - allowed
    if unknown:
        raise ConfigError(
            f"unknown top-level field(s): {', '.join(sorted(unknown))}", path)


def load_watchlist(path: str | Path, *,
                   allow_unknown_fields: bool = False) -> list[WatchSpec]:
    path = Path(path)
    data = _load_yaml(path)
    if not allow_unknown_fields:
        _reject_top_level_unknown(data, set(WatchlistFile.model_fields), path)
    data = _prepare(data, WatchlistFile, allow_unknown_fields)

    watches_data = data.get("watches")
    if not isinstance(watches_data, list) or not watches_data:
        raise ConfigError("watches must be a non-empty list", path)

    # Document-level check: duplicate names across the whole watchlist.
    try:
        WatchlistFile.model_validate(
            {"version": data.get("version", 1), "watches": watches_data})
    except ValidationError as exc:
        _raise_validation_error(exc, path)

    watches: list[WatchSpec] = []
    for index, item in enumerate(watches_data):
        label = item.get("name") if isinstance(item, dict) else None
        location = (f"watches[{index}] ({label})" if isinstance(label, str)
                    and label.strip() else f"watches[{index}]")
        if allow_unknown_fields and isinstance(item, dict):
            # Strip again per watch so nested rule_overrides unknown keys are
            # tolerated too (the document-level pass only knew raw dicts).
            item = strip_unknown_fields(item, WatchSpec)
        try:
            watch = WatchSpec.model_validate(item)
            _enforce_allowlist(watch.url)
        except (ValidationError, ValueError) as exc:
            if isinstance(exc, ValidationError):
                _raise_validation_error(exc, path, location)
            raise ConfigError(str(exc), path, location) from exc
        watches.append(watch)

    return watches


def load_rules(path: str | Path, *,
               allow_unknown_fields: bool = False) -> Rules:
    path = Path(path)
    data = _load_yaml(path)
    if not allow_unknown_fields:
        _reject_top_level_unknown(data, set(RulesFile.model_fields), path)
    data = _prepare(data, RulesFile, allow_unknown_fields)
    try:
        document = RulesFile.model_validate(data)
    except ValidationError as exc:
        _raise_validation_error(exc, path)
    return document.to_rules()


def load_all(watchlist_path: str | Path, rules_path: str | Path, *,
             allow_unknown_fields: bool = False
             ) -> tuple[list[WatchSpec], Rules]:
    watches = load_watchlist(watchlist_path,
                             allow_unknown_fields=allow_unknown_fields)
    rules = load_rules(rules_path, allow_unknown_fields=allow_unknown_fields)
    # Cross-check every override against the global base so that a partial
    # override creating an invalid noise/major combination fails at load time.
    for watch in watches:
        effective_rules(rules, watch)
    return watches, rules


# ---------------------------------------------------------------------------
# Rule query / merge (the only place override semantics live)
# ---------------------------------------------------------------------------

def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge override into a copy of base; nested mappings merge recursively.

    All current rule fields are scalars or tuples, but the merge is deep so
    future nested rule structures cannot be silently replaced wholesale.
    """
    merged = dict(base)
    for key, value in override.items():
        if (key in merged and isinstance(merged[key], dict)
                and isinstance(value, dict)):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def effective_rules(base: Rules, watch: WatchSpec) -> Rules:
    """Return base rules with the watch's validated overrides deep-merged."""
    overrides = watch.rule_overrides.merged_values()
    if not overrides:
        return base
    merged_values = _deep_merge(dict(base.model_dump()), overrides)
    try:
        # model_copy skips validators, so re-validate to catch cross-field
        # constraints (e.g. noise floor vs. major threshold) after merging.
        return Rules.model_validate(merged_values)
    except ValidationError as exc:
        rendered = "; ".join(_render_error(err) for err in exc.errors())
        raise ConfigError(
            f"rule_overrides produce invalid rules when merged with global "
            f"rules: {rendered}",
            location=f"watches ({watch.name})") from exc
