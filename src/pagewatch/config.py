"""YAML configuration loading, inheritance resolution and validation.

A watchlist may declare reusable ``templates:`` at the top level and let
concrete watches inherit from them (or from other watches) via ``extends:``.
``generate`` entries expand a template plus a list of path suffixes into
multiple concrete watches. ``name``, ``url`` and ``interval_seconds`` are
identity fields: they are never inherited and must be declared by every
concrete watch (or, for generated watches, derived inside the generate
entry). All other fields resolve nearest-first along the extends chain;
``rule_overrides`` merges key by key. Loading always returns the fully
resolved, flat list of :class:`WatchSpec`, so the scheduler, tag filtering
and per-watch rule overrides behave exactly as for hand-written watches.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields, replace
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
# Identity fields are required on every concrete watch and never inherited.
IDENTITY_FIELDS = ("name", "url", "interval_seconds")
INHERITABLE_FIELDS = (
    "tags", "include_selectors", "exclude_selectors", "noise_patterns",
    "keywords", "enabled", "rule_overrides",
)
GENERATE_FIELDS = {
    "extends", "base_url", "name_prefix", "interval_seconds", "paths",
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


def _clean_overrides(values: dict[str, Any], path: Path, location: str,
                     key_sources: dict[str, str] | None = None) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    sequence_fields = {"focus_selectors", "alert_severities"}
    for key, value in values.items():
        key_location = (key_sources or {}).get(key, location)
        if key in sequence_fields:
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ConfigError(f"{key} must be a list of strings", path, key_location)
            if key == "focus_selectors":
                _compile_selectors(tuple(value), path, key_location)
            elif any(v not in {"noise", "minor", "major"} for v in value):
                raise ConfigError(f"{key} values must be noise, minor or major",
                                  path, key_location)
            cleaned[key] = tuple(value)
        elif key in {"major_keep_forever"}:
            if not isinstance(value, bool):
                raise ConfigError(f"{key} must be a boolean", path, key_location)
            cleaned[key] = value
        elif key == "timeout_seconds":
            cleaned[key] = _number(value, path, key_location, key, 0.1, 300)
        elif key == "max_bytes":
            cleaned[key] = _int(value, path, key_location, key, 1024)
        elif key in {"noise_max_change_rate", "major_change_rate"}:
            cleaned[key] = _number(value, path, key_location, key, 0.0, 1.0)
        elif key in RULE_FIELDS:
            minimum = 0 if key in {"alert_cooldown_seconds", "jitter_seconds",
                                   "archive_max_age_days"} else 1
            cleaned[key] = _int(value, path, key_location, key, minimum)
    return cleaned


def _validate_inheritable(data: dict[str, Any], path: Path, location: str) -> None:
    """Eagerly validate the inheritable fields declared at *location*."""
    tags = _strings(data.get("tags"), path, location, "tags")
    includes = _strings(data.get("include_selectors"), path, location,
                        "include_selectors")
    excludes = _strings(data.get("exclude_selectors"), path, location,
                        "exclude_selectors")
    patterns = _strings(data.get("noise_patterns"), path, location, "noise_patterns")
    _strings(data.get("keywords"), path, location, "keywords")
    _compile_selectors(includes, path, location)
    _compile_selectors(excludes, path, location)
    _validate_regexes(patterns, path, location)
    enabled = data.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        raise ConfigError("enabled must be a boolean", path, location)
    overrides = data.get("rule_overrides")
    if overrides is not None:
        if not isinstance(overrides, dict):
            raise ConfigError("rule_overrides must be a mapping", path, location)
        unknown_rules = set(overrides) - RULE_FIELDS
        if unknown_rules:
            raise ConfigError("unknown rule override(s): " +
                              ", ".join(sorted(unknown_rules)), path, location)
        _clean_overrides(overrides, path, location)


def _validate_watch(data: dict[str, Any], path: Path, location: str,
                    field_locations: dict[str, str] | None = None,
                    override_sources: dict[str, str] | None = None) -> WatchSpec:
    """Validate one fully resolved watch mapping into a :class:`WatchSpec`.

    *field_locations* maps an inherited field to the location that declared
    it, so errors keep pointing at the original file position instead of the
    expanded watch alone.
    """
    sources = field_locations or {}

    def loc(field: str) -> str:
        return sources.get(field, location)

    if not isinstance(data, dict):
        raise ConfigError("watch entry must be a mapping", path, location)
    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("name is required and must be a non-empty string",
                          path, loc("name"))
    location = f"{location} ({name})"
    if data.get("url") is None:
        raise ConfigError("url is required on every concrete watch and is "
                          "never inherited", path, loc("url"))
    _validate_url(data["url"], path, loc("url"))
    if data.get("interval_seconds") is None:
        raise ConfigError("interval_seconds is required on every concrete "
                          "watch and is never inherited", path, loc("interval_seconds"))
    interval = _int(data.get("interval_seconds"), path, loc("interval_seconds"),
                    "interval_seconds", MIN_INTERVAL_SECONDS)
    tags = _strings(data.get("tags"), path, loc("tags"), "tags")
    includes = _strings(data.get("include_selectors"), path,
                        loc("include_selectors"), "include_selectors")
    excludes = _strings(data.get("exclude_selectors"), path,
                        loc("exclude_selectors"), "exclude_selectors")
    patterns = _strings(data.get("noise_patterns"), path, loc("noise_patterns"),
                        "noise_patterns")
    keywords = _strings(data.get("keywords"), path, loc("keywords"), "keywords")
    _compile_selectors(includes, path, loc("include_selectors"))
    _compile_selectors(excludes, path, loc("exclude_selectors"))
    _validate_regexes(patterns, path, loc("noise_patterns"))
    enabled = data.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ConfigError("enabled must be a boolean", path, loc("enabled"))
    overrides = data.get("rule_overrides") or {}
    if not isinstance(overrides, dict):
        raise ConfigError("rule_overrides must be a mapping", path,
                          loc("rule_overrides"))
    unknown_rules = set(overrides) - RULE_FIELDS
    if unknown_rules:
        raise ConfigError("unknown rule override(s): " +
                          ", ".join(sorted(unknown_rules)),
                          path, loc("rule_overrides"))
    overrides = _clean_overrides(overrides, path, loc("rule_overrides"),
                                 override_sources)
    return WatchSpec(
        name=name.strip(), url=data["url"], interval_seconds=interval, tags=tags,
        include_selectors=includes, exclude_selectors=excludes,
        noise_patterns=patterns, keywords=keywords, enabled=enabled,
        rules_overrides=overrides,
    )


# ---------------------------------------------------------------------------
# Inheritance resolution
# ---------------------------------------------------------------------------


@dataclass
class _Node:
    """One inheritable declaration site: a template or a concrete watch."""

    location: str
    extends: str | None
    values: dict[str, Any]      # declared inheritable fields (non-None)
    overrides: dict[str, Any]   # declared rule_overrides mapping


@dataclass
class _Inheritance:
    """Fields resolved along an extends chain, with declaration sites."""

    values: dict[str, Any]
    sources: dict[str, str]
    overrides: dict[str, Any]
    override_sources: dict[str, str]


@dataclass
class _GenerateItem:
    path: str
    name: str | None
    interval: int | None
    location: str


@dataclass
class _Generate:
    location: str
    extends: str
    base_url: str
    name_prefix: str
    interval: int | None
    values: dict[str, Any]
    overrides: dict[str, Any]
    items: list[_GenerateItem]


def _declared_values(data: dict[str, Any]) -> dict[str, Any]:
    return {field: data[field] for field in INHERITABLE_FIELDS
            if field != "rule_overrides" and data.get(field) is not None}


def _declared_overrides(data: dict[str, Any], path: Path, location: str) -> dict[str, Any]:
    overrides = data.get("rule_overrides")
    if overrides is None:
        return {}
    if not isinstance(overrides, dict):
        raise ConfigError("rule_overrides must be a mapping", path, location)
    return dict(overrides)


def _check_extends(value: Any, path: Path, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("extends must be a non-empty string naming a "
                          "template or watch", path, location)
    return value.strip()


def _parse_templates(raw: Any, path: Path) -> dict[str, _Node]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("templates must be a mapping of template name to "
                          "watch fields", path, "templates")
    nodes: dict[str, _Node] = {}
    for name, body in raw.items():
        location = f"templates.{name}"
        if not isinstance(name, str) or not name.strip():
            raise ConfigError("template names must be non-empty strings",
                              path, "templates")
        if not isinstance(body, dict):
            raise ConfigError("template must be a mapping of watch fields",
                              path, location)
        forbidden = sorted(set(body) & set(IDENTITY_FIELDS))
        if forbidden:
            raise ConfigError(
                f"template must not declare {', '.join(forbidden)}: name, url "
                "and interval_seconds are required on every concrete watch "
                "and are never inherited", path, location)
        unknown = set(body) - set(INHERITABLE_FIELDS) - {"extends"}
        if unknown:
            raise ConfigError(f"unknown field(s): {', '.join(sorted(unknown))}",
                              path, location)
        _validate_inheritable(body, path, location)
        nodes[name] = _Node(
            location=location,
            extends=_check_extends(body.get("extends"), path, location),
            values=_declared_values(body),
            overrides=_declared_overrides(body, path, location),
        )
    return nodes


def _node_label(node_id: tuple[str, str]) -> str:
    kind, key = node_id
    return f"templates.{key}" if kind == "template" else f"watch {key!r}"


def _find_target(name: str, nodes: dict[tuple[str, str], _Node]) -> tuple[str, str] | None:
    for kind in ("template", "watch"):
        if (kind, name) in nodes:
            return (kind, name)
    return None


def _resolve(node_id: tuple[str, str], nodes: dict[tuple[str, str], _Node],
             memo: dict[tuple[str, str], _Inheritance],
             stack: list[tuple[str, str]], path: Path) -> _Inheritance:
    """Resolve inheritable fields for *node_id*, nearest declaration wins."""
    if node_id in memo:
        return memo[node_id]
    node = nodes[node_id]
    if node_id in stack:
        cycle = stack[stack.index(node_id):] + [node_id]
        chain = " -> ".join(_node_label(item) for item in cycle)
        raise ConfigError(f"circular extends detected: {chain}", path, node.location)
    stack.append(node_id)
    if node.extends is not None:
        target_id = _find_target(node.extends, nodes)
        if target_id is None:
            raise ConfigError(
                f"extends target {node.extends!r} not found: no template or "
                "watch with that name", path, node.location)
        base = _resolve(target_id, nodes, memo, stack, path)
        values = {**base.values, **node.values}
        sources = {**base.sources,
                   **{key: node.location for key in node.values}}
        overrides = {**base.overrides, **node.overrides}
        override_sources = {**base.override_sources,
                            **{key: node.location for key in node.overrides}}
    else:
        values = dict(node.values)
        sources = {key: node.location for key in node.values}
        overrides = dict(node.overrides)
        override_sources = {key: node.location for key in node.overrides}
    stack.pop()
    result = _Inheritance(values, sources, overrides, override_sources)
    memo[node_id] = result
    return result


def _parse_generate(body: Any, index: int, path: Path) -> _Generate:
    location = f"watches[{index}] (generate)"
    if not isinstance(body, dict):
        raise ConfigError("generate must be a mapping", path, location)
    unknown = set(body) - GENERATE_FIELDS - set(INHERITABLE_FIELDS)
    if unknown:
        raise ConfigError(f"unknown field(s): {', '.join(sorted(unknown))}",
                          path, location)
    extends = body.get("extends")
    if not isinstance(extends, str) or not extends.strip():
        raise ConfigError("generate requires 'extends' naming a template or "
                          "watch", path, location)
    base_url = body.get("base_url")
    _validate_base_url(base_url, path, location)
    name_prefix = body.get("name_prefix", "")
    if not isinstance(name_prefix, str):
        raise ConfigError("name_prefix must be a string", path, location)
    interval = body.get("interval_seconds")
    if interval is not None:
        interval = _int(interval, path, location, "interval_seconds",
                        MIN_INTERVAL_SECONDS)
    _validate_inheritable(body, path, location)
    raw_paths = body.get("paths")
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ConfigError("generate requires 'paths': a non-empty list of "
                          "path suffixes", path, location)
    items = [_parse_generate_item(item, index, path) for item in raw_paths]
    return _Generate(
        location=location, extends=extends.strip(), base_url=base_url,
        name_prefix=name_prefix, interval=interval,
        values=_declared_values(body),
        overrides=_declared_overrides(body, path, location),
        items=items,
    )


def _validate_base_url(value: Any, path: Path, location: str) -> None:
    if not isinstance(value, str) or not value or any(ch.isspace() for ch in value):
        raise ConfigError("base_url must be a non-empty string without "
                          "whitespace", path, location)
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ConfigError("base_url must be an absolute http(s) URL", path, location)
    if parts.username or parts.password:
        raise ConfigError("base_url must not contain credentials", path, location)
    if parts.query or parts.fragment:
        raise ConfigError("base_url must not contain a query string or "
                          "fragment", path, location)
    try:
        parts.port
    except ValueError as exc:
        raise ConfigError(f"base_url has an invalid port: {exc}", path,
                          location) from exc
    try:
        ensure_url_allowed(value)
    except AllowlistError as exc:
        raise ConfigError(str(exc), path, location) from exc


def _parse_generate_item(item: Any, index: int, path: Path) -> _GenerateItem:
    location = f"watches[{index}] (generate)"
    if isinstance(item, str):
        item_path, name, interval = item, None, None
    elif isinstance(item, dict):
        unknown = set(item) - {"path", "name", "interval_seconds"}
        if unknown:
            raise ConfigError(
                f"unknown field(s) in generate path entry: "
                f"{', '.join(sorted(unknown))}", path, location)
        item_path = item.get("path")
        name = item.get("name")
        interval = item.get("interval_seconds")
    else:
        raise ConfigError("generate paths entries must be path strings or "
                          "mappings with 'path'", path, location)
    if not isinstance(item_path, str) or not item_path.strip() or any(
            ch.isspace() for ch in item_path):
        raise ConfigError("generate path entries must be non-empty strings "
                          "without whitespace", path, location)
    location = f"watches[{index}] (generate item {item_path!r})"
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise ConfigError("generate item 'name' must be a non-empty string",
                          path, location)
    if interval is not None:
        interval = _int(interval, path, location, "interval_seconds",
                        MIN_INTERVAL_SECONDS)
    return _GenerateItem(path=item_path, name=name.strip() if name else None,
                         interval=interval, location=location)


def _slug(text: str) -> str:
    return re.sub(r"[^\w]+", "-", text, flags=re.UNICODE).strip("-")


def _expand_generate(gen: _Generate, nodes: dict[tuple[str, str], _Node],
                     memo: dict[tuple[str, str], _Inheritance],
                     path: Path) -> list[tuple[WatchSpec, str]]:
    target_id = _find_target(gen.extends, nodes)
    if target_id is None:
        raise ConfigError(
            f"extends target {gen.extends!r} not found: no template or watch "
            "with that name", path, gen.location)
    base = _resolve(target_id, nodes, memo, [], path)
    values = {**base.values, **gen.values}
    sources = {**base.sources, **{key: gen.location for key in gen.values}}
    overrides = {**base.overrides, **gen.overrides}
    override_sources = {**base.override_sources,
                        **{key: gen.location for key in gen.overrides}}
    produced: list[tuple[WatchSpec, str]] = []
    for item in gen.items:
        name = item.name if item.name is not None else gen.name_prefix + _slug(item.path)
        if not name:
            raise ConfigError(
                f"cannot derive a watch name from path {item.path!r}: set "
                "'name' on the entry or provide a name_prefix", path, item.location)
        url = gen.base_url.rstrip("/") + "/" + item.path.lstrip("/")
        raw: dict[str, Any] = dict(values)
        if overrides:
            raw["rule_overrides"] = dict(overrides)
        raw["name"] = name
        raw["url"] = url
        raw["interval_seconds"] = (item.interval if item.interval is not None
                                   else gen.interval)
        watch = _validate_watch(raw, path, item.location, sources,
                                override_sources)
        produced.append((watch, item.location))
    return produced


def _parse_entry(entry: Any, index: int, path: Path) -> tuple[str, Any]:
    location = f"watches[{index}]"
    if not isinstance(entry, dict):
        raise ConfigError("watch entry must be a mapping", path, location)
    if "generate" in entry:
        if set(entry) != {"generate"}:
            raise ConfigError("a generate entry must contain only the "
                              "'generate' key", path, location)
        return ("generate", _parse_generate(entry["generate"], index, path))
    unknown = set(entry) - WATCH_FIELDS - {"extends"}
    if unknown:
        raise ConfigError(f"unknown field(s): {', '.join(sorted(unknown))}",
                          path, location)
    name = entry.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("name is required and must be a non-empty string",
                          path, location)
    named_location = f"{location} ({name})"
    _check_extends(entry.get("extends"), path, named_location)
    _declared_overrides(entry, path, named_location)
    return ("watch", (index, entry))


def _check_unique(named_origins: list[tuple[str, str]], path: Path) -> None:
    seen: dict[str, list[str]] = {}
    for name, origin in named_origins:
        seen.setdefault(name, []).append(origin)
    duplicates = {name: origins for name, origins in seen.items()
                  if len(origins) > 1}
    if duplicates:
        details = "; ".join(
            f"{name!r} declared at {', '.join(origins)}"
            for name, origins in sorted(duplicates.items()))
        raise ConfigError(f"duplicate watch name(s): {details}", path)


def load_watchlist(path: str | Path) -> list[WatchSpec]:
    """Load a watchlist and return the fully resolved flat watch list.

    Inheritance (``extends``/``templates``) and ``generate`` expansion are
    resolved here; any error aborts the whole load, so a rejected file never
    produces a partially applied watch set.
    """
    path = Path(path)
    data = _load_yaml(path)
    unknown = set(data) - {"version", "templates", "watches"}
    if unknown:
        raise ConfigError(f"unknown top-level field(s): {', '.join(sorted(unknown))}", path)
    watches_data = data.get("watches")
    if not isinstance(watches_data, list) or not watches_data:
        raise ConfigError("watches must be a non-empty list", path)
    templates = _parse_templates(data.get("templates"), path)
    entries = [_parse_entry(entry, i, path)
               for i, entry in enumerate(watches_data)]
    concrete = [parsed for kind, parsed in entries if kind == "watch"]
    generators = [parsed for kind, parsed in entries if kind == "generate"]
    _check_unique([(entry["name"].strip(), f"watches[{index}]")
                   for index, entry in concrete], path)
    watch_names = {entry["name"].strip() for _, entry in concrete}
    ambiguous = sorted(set(templates) & watch_names)
    if ambiguous:
        first = next(index for index, entry in concrete
                     if entry["name"].strip() == ambiguous[0])
        raise ConfigError(
            f"name(s) {', '.join(ambiguous)} used by both a template and a "
            "watch; extends targets must be unambiguous", path,
            f"watches[{first}] ({ambiguous[0]})")
    nodes: dict[tuple[str, str], _Node] = {
        ("template", name): node for name, node in templates.items()
    }
    for index, entry in concrete:
        name = entry["name"].strip()
        nodes[("watch", name)] = _Node(
            location=f"watches[{index}] ({name})",
            extends=_check_extends(entry.get("extends"), path,
                                   f"watches[{index}] ({name})"),
            values=_declared_values(entry),
            overrides=_declared_overrides(entry, path,
                                          f"watches[{index}] ({name})"),
        )
    memo: dict[tuple[str, str], _Inheritance] = {}
    for node_id in nodes:  # resolve every node so unused broken chains fail too
        _resolve(node_id, nodes, memo, [], path)
    resolved: list[tuple[WatchSpec, str]] = []
    for kind, parsed in entries:
        if kind == "generate":
            resolved.extend(_expand_generate(parsed, nodes, memo, path))
            continue
        index, entry = parsed
        name = entry["name"].strip()
        inheritance = memo[("watch", name)]
        raw: dict[str, Any] = dict(inheritance.values)
        if inheritance.overrides:
            raw["rule_overrides"] = dict(inheritance.overrides)
        raw["name"] = name
        raw["url"] = entry.get("url")
        raw["interval_seconds"] = entry.get("interval_seconds")
        watch = _validate_watch(raw, path, f"watches[{index}]",
                                inheritance.sources,
                                inheritance.override_sources)
        resolved.append((watch, f"watches[{index}]"))
    _check_unique([(watch.name, origin) for watch, origin in resolved], path)
    return [watch for watch, _ in resolved]


# ---------------------------------------------------------------------------
# Flat export
# ---------------------------------------------------------------------------


def watch_to_mapping(watch: WatchSpec) -> dict[str, Any]:
    """Serialize a resolved watch to a plain mapping with every field explicit."""
    overrides = {
        key: (list(value) if isinstance(value, tuple) else value)
        for key, value in watch.rules_overrides.items()
    }
    return {
        "name": watch.name,
        "url": watch.url,
        "interval_seconds": watch.interval_seconds,
        "tags": list(watch.tags),
        "include_selectors": list(watch.include_selectors),
        "exclude_selectors": list(watch.exclude_selectors),
        "noise_patterns": list(watch.noise_patterns),
        "keywords": list(watch.keywords),
        "enabled": watch.enabled,
        "rule_overrides": overrides,
    }


def dump_flat_watchlist(watches: list[WatchSpec],
                        source: str | None = None) -> str:
    """Dump resolved watches as a flat watchlist YAML with no extends left.

    The output contains only ``version`` and ``watches``; loading it again
    yields an equivalent configuration, so it can be archived for audit and
    replayed as-is.
    """
    payload = {"version": 1,
               "watches": [watch_to_mapping(watch) for watch in watches]}
    text = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    header = "# Flattened watchlist"
    if source:
        header += f" resolved from {source}"
    header += " (all inheritance expanded; safe to archive and replay)\n"
    return header + text


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
        "retention": {"keep_last", "major_keep_forever", "archive_max_age_days"},
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
