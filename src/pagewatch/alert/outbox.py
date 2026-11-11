"""Append-only local JSONL alert outbox (never sends externally)."""

from __future__ import annotations

import json
from pathlib import Path

from ..models import AlertRecord

OUTBOX_VERSION = 1


def alert_line(alert: AlertRecord) -> str:
    payload = {"outbox_version": OUTBOX_VERSION, **alert.as_jsonl()}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def append_alert(path: str | Path, alert: AlertRecord) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(alert_line(alert) + "\n")
        handle.flush()
    return destination


def read_outbox(path: str | Path) -> list[dict[str, object]]:
    source = Path(path)
    if not source.exists():
        return []
    values: list[dict[str, object]] = []
    with source.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                values.append(json.loads(line))
    return values
