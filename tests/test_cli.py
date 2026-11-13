"""CLI behaviour for configuration loading, error codes and tolerance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from pagewatch.cli import main

ROOT = Path(__file__).parents[1]
GOOD_RULES = ROOT / "configs" / "rules.example.yaml"


def _watchlist(tmp_path: Path, *watches: dict) -> Path:
    path = tmp_path / "watchlist.yaml"
    path.write_text(
        yaml.safe_dump({"version": 1, "watches": list(watches)}),
        encoding="utf-8")
    return path


def _watch(**overrides) -> dict:
    value = {
        "name": "example",
        "url": "http://127.0.0.1:8765/docs",
        "interval_seconds": 60,
    }
    value.update(overrides)
    return value


def test_check_config_ok_exit_zero(tmp_path, capsys):
    path = _watchlist(tmp_path, _watch())
    assert main(["check-config", "--config", str(path),
                 "--rules", str(GOOD_RULES)]) == 0
    assert "config OK" in capsys.readouterr().out


def test_bad_config_exit_code_two_plain_text(tmp_path, capsys):
    path = _watchlist(tmp_path, _watch(interval_seconds=-1))
    assert main(["check-config", "--config", str(path),
                 "--rules", str(GOOD_RULES)]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: ")
    assert "interval_seconds" in err
    with pytest.raises(json.JSONDecodeError):
        json.loads(err)


def test_json_errors_emit_machine_readable_payload(tmp_path, capsys):
    path = _watchlist(tmp_path, _watch(rule_overrides={"max_workerz": 1}))
    rc = main(["--json-errors", "check-config", "--config", str(path),
               "--rules", str(GOOD_RULES)])
    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "config_validation_failed"
    assert payload["path"] == str(path)
    detail = payload["details"][0]
    assert "rule_overrides" in detail["location"]
    assert detail["type"] == "extra_forbidden"


def test_allow_unknown_fields_flag_rescues_otherwise_bad_config(tmp_path, capsys):
    path = _watchlist(tmp_path, _watch(future_field=True,
                                       rule_overrides={"future": 1}))
    # Strict mode fails.
    assert main(["check-config", "--config", str(path),
                 "--rules", str(GOOD_RULES)]) == 2
    # Tolerant mode loads successfully.
    capsys.readouterr()
    assert main(["check-config", "--config", str(path),
                 "--rules", str(GOOD_RULES),
                 "--allow-unknown-fields"]) == 0
    assert "config OK" in capsys.readouterr().out


def test_run_once_accepts_tolerance_flag(tmp_path):
    # The flag must parse on run-once even though the watch cannot fetch.
    path = _watchlist(tmp_path, _watch(future_field=True))
    rc = main(["run-once", "--config", str(path),
               "--rules", str(GOOD_RULES), "--data-dir", str(tmp_path / "data"),
               "--allow-unknown-fields"])
    # Fetch failure yields exit 1, not a config error (2).
    assert rc == 1
