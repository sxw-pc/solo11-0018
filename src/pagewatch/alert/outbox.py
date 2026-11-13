"""Append-only local JSONL alert outbox (never sends externally).

Appends and compaction rewrites are serialized through an advisory lock on a
sibling lock file so a rewrite can never lose a concurrently appended alert.
Rewrites go through a temporary file and ``os.replace``: readers therefore see
either the old or the new outbox, never a half-written one.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

from ..models import AlertRecord

OUTBOX_VERSION = 1


def alert_line(alert: AlertRecord) -> str:
    payload = {"outbox_version": OUTBOX_VERSION, **alert.as_jsonl()}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _lock_path(path: Path) -> Path:
    return path.with_name(path.name + ".lock")


@contextlib.contextmanager
def outbox_lock(path: str | Path) -> Iterator[None]:
    """Exclusive cross-process lock backing both append and rewrite.

    The lock is tied to an open file description; callers must not open the
    lock file a second time while holding it (flock is not reentrant across
    separate descriptors, even within one process).
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _lock_path(destination)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def append_alert(path: str | Path, alert: AlertRecord) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    line = alert_line(alert) + "\n"
    with outbox_lock(destination):
        with destination.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
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


def _retain_lines(destination: Path, keep_ids: set[int]) -> list[str]:
    kept_lines: list[str] = []
    if not destination.exists():
        return kept_lines
    with destination.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            text = raw_line.strip()
            if not text:
                continue
            record = json.loads(text)
            if "id" not in record:
                raise ValueError(
                    f"outbox line without an alert id cannot be reconciled: "
                    f"{text[:120]}")
            if int(record["id"]) in keep_ids:
                kept_lines.append(text)
    return kept_lines


def stage_rewrite(path: str | Path, keep_ids: set[int]) -> Path:
    """Write the filtered outbox to a sibling temp file.

    Assumes the caller already holds :func:`outbox_lock`.  The original file
    is not modified; callers atomically ``os.replace`` the returned temp path
    once the database transaction has committed.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    kept_lines = _retain_lines(destination, keep_ids)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            for line in kept_lines:
                handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise
    return Path(temp_name)


def rewrite_outbox(path: str | Path, keep_ids: set[int]) -> int:
    """Atomically rewrite the outbox, keeping only lines whose id is retained.

    Returns the number of retained lines.  Every retained line is preserved
    byte-for-byte; lines appended while waiting for the lock survive because
    their id is unknown to an earlier plan only when the database row is also
    new, in which case the caller re-reads the alert ledger under the lock.
    """
    destination = Path(path)
    with outbox_lock(destination):
        temp_path = stage_rewrite(destination, keep_ids)
        os.replace(temp_path, destination)
    return len(keep_ids)
