"""Tests for watchlist inheritance (extends/templates), generate expansion
and flat export."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

import pagewatch.cli as cli
from pagewatch.config import (
    ConfigError, dump_flat_watchlist, effective_rules, load_rules,
    load_watchlist,
)
from pagewatch.models import Rules
from pagewatch.schedule.scheduler import select_watches
from pagewatch.storage.db import connect
from pagewatch.storage.events import list_events
from pagewatch.storage.snapshots import list_schedules

ROOT = Path(__file__).parents[1]

NOISE = [r"当前访问计数[：:]\s*\d+", r"更新时间[：:]\s*[0-9:\- ]+"]


def write_watchlist(tmp_path: Path, data: dict, name: str = "watchlist.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def base_templates(**overrides):
    templates = {
        "base": {
            "exclude_selectors": [".analytics"],
            "rule_overrides": {"alert_cooldown_seconds": 600},
        },
        "notice": {
            "extends": "base",
            "tags": ["notice"],
            "include_selectors": ["#notice-area"],
            "keywords": ["重要"],
            "rule_overrides": {"major_change_rate": 0.5},
        },
    }
    templates.update(overrides)
    return templates


def by_name(watches):
    return {watch.name: watch for watch in watches}


# ---------------------------------------------------------------------------
# extends / templates
# ---------------------------------------------------------------------------


def test_multi_level_inheritance_nearest_wins(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": base_templates(),
        "watches": [
            {"name": "a", "extends": "notice",
             "url": "http://127.0.0.1:8765/a", "interval_seconds": 60},
            {"name": "b", "extends": "notice",
             "url": "http://127.0.0.1:8765/b", "interval_seconds": 120,
             "tags": ["custom"], "exclude_selectors": []},
        ],
    })
    watches = by_name(load_watchlist(path))
    a, b = watches["a"], watches["b"]
    # Fields not declared on the child come from the nearest ancestor.
    assert a.tags == ("notice",)
    assert a.include_selectors == ("#notice-area",)
    assert a.keywords == ("重要",)
    assert a.exclude_selectors == (".analytics",)  # grandparent template
    assert a.enabled is True
    # Declared fields replace the inherited value wholesale.
    assert b.tags == ("custom",)
    assert b.exclude_selectors == ()  # explicit empty list clears the ancestor
    assert b.include_selectors == ("#notice-area",)
    # rule_overrides merge key by key across the chain, nearest key wins.
    assert a.rules_overrides == {"alert_cooldown_seconds": 600,
                                 "major_change_rate": 0.5}
    merged = effective_rules(Rules(), a)
    assert merged.alert_cooldown_seconds == 600
    assert merged.major_change_rate == 0.5


def test_enabled_is_inherited_and_overridable(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": {"off": {"enabled": False, "tags": ["t"]}},
        "watches": [
            {"name": "off-watch", "extends": "off",
             "url": "http://127.0.0.1:8765/a", "interval_seconds": 60},
            {"name": "on-watch", "extends": "off", "enabled": True,
             "url": "http://127.0.0.1:8765/b", "interval_seconds": 60},
        ],
    })
    watches = by_name(load_watchlist(path))
    assert watches["off-watch"].enabled is False
    assert watches["on-watch"].enabled is True
    # Disabled resolved watches are skipped by selection, as handwritten ones.
    selected = select_watches(list(watches.values()))
    assert [watch.name for watch in selected] == ["on-watch"]


def test_watch_extends_watch_but_identity_fields_are_not_inherited(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "watches": [
            {"name": "parent", "url": "http://127.0.0.1:8765/p",
             "interval_seconds": 60, "tags": ["t"],
             "include_selectors": ["#x"]},
            {"name": "child", "extends": "parent",
             "url": "http://127.0.0.1:8765/c", "interval_seconds": 90},
        ],
    })
    watches = by_name(load_watchlist(path))
    assert watches["child"].tags == ("t",)
    assert watches["child"].include_selectors == ("#x",)
    assert watches["child"].url == "http://127.0.0.1:8765/c"
    assert watches["child"].interval_seconds == 90
    # The parent remains a normal concrete watch.
    assert watches["parent"].url == "http://127.0.0.1:8765/p"


def test_identity_fields_must_not_appear_in_templates(tmp_path):
    for field in ("name", "url", "interval_seconds"):
        path = write_watchlist(tmp_path, {
            "version": 1,
            "templates": {"bad": {field: "x" if field == "name" else
                                          ("http://127.0.0.1/" if field == "url" else 60)}},
            "watches": [{"name": "a", "url": "http://127.0.0.1:8765/a",
                         "interval_seconds": 60}],
        })
        with pytest.raises(ConfigError) as error:
            load_watchlist(path)
        assert "never inherited" in str(error.value)
        assert "templates.bad" in str(error.value)


def test_missing_identity_fields_error_points_at_concrete_watch(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": base_templates(),
        "watches": [
            {"name": "no-url", "extends": "notice", "interval_seconds": 60},
        ],
    })
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    message = str(error.value)
    assert "url is required" in message and "never inherited" in message
    assert "watches[0] (no-url)" in message

    path = write_watchlist(tmp_path, {
        "version": 1,
        "watches": [
            {"name": "parent", "url": "http://127.0.0.1:8765/p",
             "interval_seconds": 60},
            {"name": "child", "extends": "parent",
             "url": "http://127.0.0.1:8765/c"},  # interval missing
        ],
    })
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    message = str(error.value)
    assert "interval_seconds is required" in message
    assert "watches[1] (child)" in message


@pytest.mark.parametrize("templates, watches, fragment", [
    # template -> template cycle
    ({"a": {"extends": "b"}, "b": {"extends": "a"}}, None,
     "circular extends detected: templates.a -> templates.b -> templates.a"),
    # watch -> watch cycle
    ({}, [{"name": "x", "extends": "y"}, {"name": "y", "extends": "x"}],
     "circular extends detected: watch 'x' -> watch 'y' -> watch 'x'"),
    # self inheritance
    ({"a": {"extends": "a"}}, None, "circular extends detected: templates.a -> templates.a"),
])
def test_circular_extends_rejected(tmp_path, templates, watches, fragment):
    if watches is None:
        watches = [{"name": "w", "extends": "a",
                    "url": "http://127.0.0.1:8765/w", "interval_seconds": 60}]
    else:
        for entry in watches:
            entry.setdefault("url", "http://127.0.0.1:8765/x")
            entry.setdefault("interval_seconds", 60)
    path = write_watchlist(tmp_path, {
        "version": 1, "templates": templates, "watches": watches})
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    assert fragment in str(error.value)
    assert str(tmp_path) in str(error.value)


def test_unknown_extends_target_and_ambiguous_names(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "watches": [{"name": "a", "extends": "ghost",
                     "url": "http://127.0.0.1:8765/a", "interval_seconds": 60}],
    })
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    assert "extends target 'ghost' not found" in str(error.value)
    assert "watches[0] (a)" in str(error.value)

    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": {"dup": {"tags": ["t"]}},
        "watches": [{"name": "dup", "url": "http://127.0.0.1:8765/a",
                     "interval_seconds": 60}],
    })
    with pytest.raises(ConfigError, match="both a template and a watch"):
        load_watchlist(path)


def test_broken_chain_fails_even_when_unused(tmp_path):
    # An unused template with a dangling extends must not load silently.
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": {"dangling": {"extends": "ghost"}},
        "watches": [{"name": "a", "url": "http://127.0.0.1:8765/a",
                     "interval_seconds": 60}],
    })
    with pytest.raises(ConfigError, match="extends target 'ghost' not found"):
        load_watchlist(path)


def test_inherited_field_errors_point_at_declaration_site(tmp_path):
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": {"bad-css": {"include_selectors": ["p["]}},
        "watches": [{"name": "a", "extends": "bad-css",
                     "url": "http://127.0.0.1:8765/a", "interval_seconds": 60}],
    })
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    message = str(error.value)
    assert "invalid CSS selector" in message
    assert "templates.bad-css" in message  # not just the expanded watch name

    # An invalid rule override inherited from a template points at the key's
    # declaration site too.
    path = write_watchlist(tmp_path, {
        "version": 1,
        "templates": {"bad-rule": {"rule_overrides": {"max_bytes": 10}}},
        "watches": [{"name": "a", "extends": "bad-rule",
                     "url": "http://127.0.0.1:8765/a", "interval_seconds": 60}],
    })
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    assert "templates.bad-rule" in str(error.value)


# ---------------------------------------------------------------------------
# generate expansion
# ---------------------------------------------------------------------------


def generate_watchlist(base_url="http://127.0.0.1:8765"):
    return {
        "version": 1,
        "templates": base_templates(),
        "watches": [
            {"name": "plain", "url": f"{base_url}/plain",
             "interval_seconds": 60, "tags": ["plain"]},
            {"generate": {
                "extends": "notice",
                "base_url": base_url,
                "name_prefix": "site-",
                "interval_seconds": 300,
                "tags": ["generated"],
                "paths": [
                    "docs/a",
                    "docs/b",
                    {"path": "docs/c", "name": "custom-c",
                     "interval_seconds": 120},
                ],
            }},
        ],
    }


def test_generate_expands_and_resolves_like_handwritten(tmp_path):
    path = write_watchlist(tmp_path, generate_watchlist())
    watches = by_name(load_watchlist(path))
    assert set(watches) == {"plain", "site-docs-a", "site-docs-b", "custom-c"}
    a = watches["site-docs-a"]
    assert a.url == "http://127.0.0.1:8765/docs/a"
    assert a.interval_seconds == 300
    assert a.tags == ("generated",)  # generate-level overrides template tags
    assert a.include_selectors == ("#notice-area",)  # from the template chain
    assert a.exclude_selectors == (".analytics",)
    assert a.rules_overrides == {"alert_cooldown_seconds": 600,
                                 "major_change_rate": 0.5}
    # Per-entry overrides win over generate-level values.
    c = watches["custom-c"]
    assert c.interval_seconds == 120
    assert c.url == "http://127.0.0.1:8765/docs/c"
    # Generated watches participate in tag filtering like any other watch.
    selected = select_watches(list(watches.values()), tags=["generated"])
    assert {watch.name for watch in selected} == {
        "site-docs-a", "site-docs-b", "custom-c"}


def test_generate_name_conflicts_reject_the_whole_file(tmp_path):
    data = generate_watchlist()
    # "docs/a" and "docs-a" slugify to the same name; the batch also collides
    # with the existing "plain" watch. All conflicts must be reported and
    # nothing may load.
    data["watches"][1]["generate"]["paths"] = [
        "docs/a", "docs-a", {"path": "x", "name": "plain"}]
    path = write_watchlist(tmp_path, data)
    with pytest.raises(ConfigError) as error:
        load_watchlist(path)
    message = str(error.value)
    assert "duplicate watch name" in message
    assert "site-docs-a" in message
    assert "plain" in message
    assert "generate item" in message  # origins point at the generate entry


def test_generate_validation_errors(tmp_path):
    def load(data):
        with pytest.raises(ConfigError) as error:
            load_watchlist(write_watchlist(tmp_path, data))
        return str(error.value)

    def gen(body):
        return {"version": 1, "templates": base_templates(),
                "watches": [{"generate": body}]}

    base = {"extends": "notice", "base_url": "http://127.0.0.1:8765",
            "interval_seconds": 60, "paths": ["a"]}
    assert "requires 'extends'" in load(gen({"base_url": "http://127.0.0.1",
                                             "paths": ["a"]}))
    assert "base_url must be" in load(gen({**base, "base_url": "ftp://x/"}))
    assert "query string or fragment" in load(
        gen({**base, "base_url": "http://127.0.0.1/?x=1"}))
    assert "allowlist" in load(
        gen({**base, "base_url": "http://203.0.113.9/"}))
    assert "non-empty list" in load(gen({**base, "paths": []}))
    assert "unknown field" in load(gen({**base, "surprise": 1}))
    assert "cannot derive a watch name" in load(gen({**base, "paths": ["/"]}))
    assert "unknown field(s) in generate path entry" in load(
        gen({**base, "paths": [{"path": "a", "url": "http://127.0.0.1/a"}]}))
    # interval_seconds may not be inherited from the template chain either.
    assert "interval_seconds is required" in load(
        gen({"extends": "notice", "base_url": "http://127.0.0.1",
             "paths": ["a"]}))
    # A generate entry must not mix generate with sibling keys.
    message = load({"version": 1, "templates": base_templates(),
                    "watches": [{"generate": base, "name": "x"}]})
    assert "only the 'generate' key" in message
    # Generated urls are validated like handwritten ones (whitespace here).
    assert "without whitespace" in load(gen({**base, "paths": ["a b"]}))


# ---------------------------------------------------------------------------
# flat export
# ---------------------------------------------------------------------------


def test_export_roundtrip_is_equivalent_and_has_no_extends(tmp_path):
    source = write_watchlist(tmp_path, generate_watchlist())
    resolved = load_watchlist(source)
    flat_text = dump_flat_watchlist(resolved, source=str(source))
    assert "extends" not in flat_text
    assert "templates" not in yaml.safe_load(flat_text)
    flat_path = tmp_path / "flat.yaml"
    flat_path.write_text(flat_text, encoding="utf-8")
    reloaded = load_watchlist(flat_path)
    assert reloaded == resolved
    # rules_overrides is compare=False on WatchSpec: compare it explicitly.
    assert [w.rules_overrides for w in reloaded] == [
        w.rules_overrides for w in resolved]
    # Exporting the exported file again is a fixed point.
    assert dump_flat_watchlist(reloaded) == dump_flat_watchlist(resolved)


def test_export_config_cli_writes_file(tmp_path, capsys):
    source = write_watchlist(tmp_path, generate_watchlist())
    output = tmp_path / "out.yaml"
    assert cli.main(["export-config", "--config", str(source),
                     "--output", str(output)]) == 0
    assert "exported 4 flat watch(es)" in capsys.readouterr().out
    assert load_watchlist(output) == load_watchlist(source)


def test_export_config_cli_stdout(tmp_path, capsys):
    source = write_watchlist(tmp_path, {
        "version": 1,
        "templates": base_templates(),
        "watches": [{"name": "a", "extends": "notice",
                     "url": "http://127.0.0.1:8765/a", "interval_seconds": 60}],
    })
    assert cli.main(["export-config", "--config", str(source)]) == 0
    out = capsys.readouterr().out
    assert "extends" not in out
    assert yaml.safe_load(out)["watches"][0]["name"] == "a"


def test_inheritance_example_file_loads_and_matches_handwritten():
    watches = by_name(load_watchlist(
        ROOT / "configs/watchlist.inheritance.example.yaml"))
    assert set(watches) == {"announcements", "noisy-status", "pricing",
                            "mirror-docs", "mirror-announcements"}
    assert watches["announcements"].include_selectors == ("#notice-area",)
    assert watches["announcements"].rules_overrides == {
        "alert_cooldown_seconds": 600}
    assert watches["noisy-status"].noise_patterns == tuple(
        ['当前访问计数[：:]\\s*\\d+', '更新时间[：:]\\s*[0-9:\\- ]+',
         '随机会话[：:]\\s*[A-Za-z0-9_-]+'])
    assert watches["pricing"].exclude_selectors == (".ad",)
    assert watches["mirror-docs"].interval_seconds == 900
    assert watches["mirror-announcements"].interval_seconds == 300


# ---------------------------------------------------------------------------
# end-to-end: monitoring runs on the resolved flat watches
# ---------------------------------------------------------------------------


def write_inherited_watchlist(path: Path, base_url: str) -> Path:
    data = {
        "version": 1,
        "templates": {
            "base-fields": {
                "exclude_selectors": [".analytics"],
                "rule_overrides": {"alert_cooldown_seconds": 600},
            },
            "notice-fields": {
                "extends": "base-fields",
                "include_selectors": ["#notice-area"],
                "keywords": ["重要", "计费"],
                "tags": ["notice"],
            },
            "noisy-fields": {
                "extends": "base-fields",
                "include_selectors": ["main"],
                "noise_patterns": NOISE + [r"随机会话[：:]\s*[A-Za-z0-9_-]+"],
                "tags": ["status"],
            },
        },
        "watches": [
            {"name": "announcements", "extends": "notice-fields",
             "url": f"{base_url}/announcements", "interval_seconds": 60,
             "tags": ["notice", "priority"]},
            {"name": "noisy-status", "extends": "noisy-fields",
             "url": f"{base_url}/noisy", "interval_seconds": 60},
            {"generate": {
                "extends": "notice-fields",
                "base_url": base_url,
                "name_prefix": "extra-",
                "interval_seconds": 60,
                "paths": ["announcements"],
            }},
            {"generate": {
                "extends": "base-fields",
                "base_url": base_url,
                "name_prefix": "site-",
                "interval_seconds": 60,
                "include_selectors": ["#doc-body"],
                "tags": ["docs"],
                "paths": ["docs"],
            }},
        ],
    }
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def test_monitoring_round_uses_resolved_flat_watches(tmp_path, mock_server,
                                                     frozen_clock, monkeypatch):
    base_url, scenario = mock_server
    monkeypatch.setattr(cli, "build_clock", lambda: frozen_clock)
    config = write_inherited_watchlist(tmp_path / "watchlist.yaml", base_url)
    rules_path = ROOT / "configs/rules.example.yaml"
    data_dir = tmp_path / "data"

    resolved = by_name(load_watchlist(config))
    assert set(resolved) == {"announcements", "noisy-status",
                             "extra-announcements", "site-docs"}
    # Inherited noise patterns / selectors / overrides are really in effect.
    assert len(resolved["noisy-status"].noise_patterns) == 3
    assert resolved["site-docs"].include_selectors == ("#doc-body",)
    rules = load_rules(rules_path)
    assert effective_rules(rules, resolved["announcements"]
                           ).alert_cooldown_seconds == 600
    # Tag filtering sees the expanded watches.
    assert {w.name for w in select_watches(
        list(resolved.values()), tags=["notice"])} == {
        "announcements", "extra-announcements"}

    def run():
        return cli.main(["run-once", "--config", str(config),
                         "--rules", str(rules_path),
                         "--data-dir", str(data_dir)])

    scenario.set_step(0)
    assert run() == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM snapshots").fetchone()[0] == 4

    scenario.set_step(1)  # docs paragraph changes
    frozen_clock.advance(60)
    assert run() == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        events = list_events(connection)
        assert [(e.watch_name, e.severity) for e in events] == [
            ("site-docs", "minor")]

    scenario.set_step(2)  # only the masked counter changes on /noisy
    frozen_clock.advance(60)
    assert run() == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        assert len(list_events(connection)) == 1  # inherited noise mask held

    scenario.set_step(3)  # focus notice + keywords on /announcements
    frozen_clock.advance(60)
    assert run() == 0
    with connect(data_dir / "pagewatch.sqlite") as connection:
        events = list_events(connection)
        major = {event.watch_name for event in events
                 if event.severity == "major"}
        assert major == {"announcements", "extra-announcements"}
        assert connection.execute(
            "SELECT COUNT(*) FROM alerts").fetchone()[0] == 2
        # Scheduling state (jitter/backoff) is tracked per resolved watch.
        assert len(list_schedules(connection)) == 4

    # A run filtered by tag only schedules the matching resolved watches.
    frozen_clock.advance(60)
    assert cli.main(["run-once", "--config", str(config),
                     "--rules", str(rules_path), "--data-dir", str(data_dir),
                     "--tag", "docs"]) == 0


def test_bad_configs_refuse_to_start(tmp_path, capsys):
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text("version: 1\n", encoding="utf-8")
    bad_configs = {
        "cycle": {"version": 1,
                  "templates": {"a": {"extends": "b"}, "b": {"extends": "a"}},
                  "watches": [{"name": "w", "extends": "a",
                               "url": "http://127.0.0.1/w",
                               "interval_seconds": 60}]},
        "missing": {"version": 1,
                    "watches": [{"name": "w", "interval_seconds": 60}]},
        "conflict": {"version": 1,
                     "watches": [
                         {"name": "dup", "url": "http://127.0.0.1/a",
                          "interval_seconds": 60},
                         {"generate": {"extends": "dup",
                                       "base_url": "http://127.0.0.1",
                                       "interval_seconds": 60,
                                       "paths": [{"path": "b",
                                                  "name": "dup"}]}}]},
    }
    for label, data in bad_configs.items():
        config = write_watchlist(tmp_path, data, name=f"{label}.yaml")
        exit_code = cli.main(["run-once", "--config", str(config),
                              "--rules", str(rules_path),
                              "--data-dir", str(tmp_path / label)])
        assert exit_code == 2, label
        assert not (tmp_path / label / "pagewatch.sqlite").exists(), label
    err = capsys.readouterr().err
    assert "circular extends" in err
    assert "url is required" in err
    assert "duplicate watch name" in err
