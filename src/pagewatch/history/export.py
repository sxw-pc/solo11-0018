"""Cursor-driven JSONL export for downstream audit systems.

Stable key
----------

Every record carries ``watch_name`` and ``event_seq`` (the event's
pre-existing, monotonic ``events.id``).  The pair is the record's identity:
a downstream consumer can upsert by ``(watch_name, event_seq)`` and repeated
pulls of the same round are idempotent.  New events are never given an old
``event_seq`` because ``events.id`` is ``AUTOINCREMENT`` and never reused.

Incremental pull protocol
-------------------------

1. Pull with no cursor (or with the cursor from the previous round).
2. Receive at most ``page_size`` records plus ``--out`` JSONL and cursor file.
3. Repeat with ``--cursor`` until the cursor file reports ``exhausted=true``.
4. Persist the ``next_cursor``; the pull after that starts a new round and
   picks up events written since the previous watermark.

The export never rewrites or deletes an output file it does not own: an
existing ``--out`` with different keys is refused unless ``--append`` turns
the write into an append.  Appending is deduplicated against the existing
file by ``(watch_name, event_seq)``, so ``--append`` across rounds is safe
even if the operator reuses the same destination.  A page with no records
(``cursor_only``) advances and persists the cursor but leaves an existing
ledger byte-identical, which is what makes draining a round and re-pulling
from its terminal cursor safe.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..models import ChangeEvent, WatchSpec
from .errors import CursorMalformed, ExportError
from .cursor import Cursor
from .query import (
    DEFAULT_LIMIT, HistoryQuery, HistoryResult, query_events,
)
from .tags import TagSnapshot, load_tag_snapshot

EXPORT_FORMAT = "pagewatch-history-v1"


@dataclass(frozen=True)
class ExportBatch:
    path: Path
    records: int
    next_cursor: str
    exhausted: bool
    has_more: bool
    created: bool
    skipped_duplicates: int
    cursor_only: bool = False
    degraded_watches: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "format": EXPORT_FORMAT,
            "path": str(self.path),
            "records": self.records,
            "next_cursor": self.next_cursor,
            "exhausted": self.exhausted,
            "has_more": self.has_more,
            "created": self.created,
            "skipped_duplicates": self.skipped_duplicates,
            "cursor_only": self.cursor_only,
            "degraded_watches": list(self.degraded_watches),
        }


def record_key(event: ChangeEvent) -> tuple[str, int]:
    return (event.watch_name, int(event.id))


def event_record(event: ChangeEvent) -> dict[str, object]:
    payload = event.as_json()
    payload["event_seq"] = int(event.id)
    payload["key"] = {"watch_name": event.watch_name,
                      "event_seq": int(event.id)}
    payload["format"] = EXPORT_FORMAT
    return payload


def _existing_keys(path: Path) -> tuple[set[tuple[str, int]], int]:
    keys: set[tuple[str, int]] = set()
    malformed = 0
    if not path.exists():
        return keys, malformed
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
                key = (str(value["watch_name"]), int(value["event_seq"]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                malformed += 1
                continue
            keys.add(key)
    return keys, malformed


def _atomic_write(path: Path, text: str, *, append: bool) -> tuple[bool, int]:
    """Write JSONL atomically.  Returns (created, skipped_duplicates)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_keys: set[tuple[str, int]] = set()
    existing_text = ""
    if path.exists() and append:
        existing_text = path.read_text(encoding="utf-8")
        existing_keys, malformed = _existing_keys(path)
        if malformed:
            raise ExportError(
                f"refusing to append to {path}: {malformed} line(s) are not "
                "valid pagewatch records", detail={"path": str(path),
                                                   "malformed": malformed})
    elif path.exists():
        existing_text = path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    kept: list[str] = []
    skipped = 0
    for line in lines:
        try:
            value = json.loads(line)
            key = (str(value["watch_name"]), int(value["event_seq"]))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ExportError("internal error: generated a malformed record",
                              detail={"reason": str(exc)}) from exc
        if key in existing_keys:
            skipped += 1
            continue
        existing_keys.add(key)
        kept.append(line)
    body = existing_text
    if body and not body.endswith("\n"):
        body += "\n"
    body += "".join(line + "\n" for line in kept)
    # Write via a sibling temp file so a crashed process never leaves a
    # half-written ledger that a later pull would treat as authoritative.
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=str(path.parent),
        prefix=f".{path.name}.", suffix=".tmp", delete=False)
    try:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(handle.name, path)
    except BaseException:
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise
    return (not existing_text, skipped)


def export_jsonl(connection: sqlite3.Connection, query: HistoryQuery,
                 out_path: str | Path, *,
                 cursor: str | None = None,
                 page_size: int = DEFAULT_LIMIT,
                 append: bool = False,
                 watches: Iterable[WatchSpec] | None = None,
                 tag_snapshot: TagSnapshot | None = None) -> ExportBatch:
    """Export one page as JSONL; the returned cursor continues the pull."""
    snapshot = tag_snapshot if tag_snapshot is not None else \
        load_tag_snapshot(connection, watches)
    result: HistoryResult = query_events(
        connection, query, cursor=cursor, limit=page_size,
        tag_snapshot=snapshot)
    text = "".join(json.dumps(event_record(event), ensure_ascii=False,
                              sort_keys=True, separators=(",", ":")) + "\n"
                   for event in result.events)
    destination = Path(out_path)
    # An empty page must not clobber the previous pages that were appended to
    # the same ledger.  Still advance/persist the cursor, but leave the file.
    if not result.events:
        return ExportBatch(
            path=destination, records=0,
            next_cursor=result.next_cursor, exhausted=result.exhausted,
            has_more=result.has_more, created=False, skipped_duplicates=0,
            cursor_only=True,
            degraded_watches=tuple(item.name
                                   for item in result.degraded_watches),
        )
    if destination.exists() and not append:
        # Overwrites are only safe when every key in this page is already
        # present in the file (same pull, re-run).  Anything else needs
        # --append so previous rounds are never silently destroyed.
        keys, malformed = _existing_keys(destination)
        new_keys = {record_key(event) for event in result.events}
        if malformed or not new_keys <= keys:
            raise ExportError(
                f"refusing to overwrite {destination}: it already exists and "
                "is not the same page (use --append or a new path)",
                detail={"path": str(destination), "malformed": malformed})
    created, skipped = _atomic_write(destination, text, append=append)
    return ExportBatch(
        path=destination, records=len(result.events),
        next_cursor=result.next_cursor, exhausted=result.exhausted,
        has_more=result.has_more, created=created,
        skipped_duplicates=skipped,
        degraded_watches=tuple(item.name
                               for item in result.degraded_watches),
    )


def write_cursor_state(path: str | Path, cursor: str, query: HistoryQuery, *,
                       exhausted: bool, has_more: bool,
                       records: int) -> None:
    """Persist continuation state next to (or independent of) JSONL output.

    The file is written through a sibling temporary file and ``os.replace`` so
    a puller never observes a partially-written checkpoint.
    """
    payload = {
        "format": EXPORT_FORMAT,
        "cursor": cursor,
        "exhausted": exhausted,
        "has_more": has_more,
        "query": query.as_dict(),
        "query_fingerprint": query.fingerprint(),
        "records_this_page": records,
    }
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(f".{destination.name}.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                               indent=2) + "\n", encoding="utf-8")
    os.replace(temp, destination)


def write_cursor_file(path: str | Path, batch: ExportBatch,
                      query: HistoryQuery) -> None:
    """Persist the continuation state that follows a JSONL export page."""
    write_cursor_state(path, batch.next_cursor, query,
                       exhausted=batch.exhausted, has_more=batch.has_more,
                       records=batch.records)


def read_cursor_file(path: str | Path) -> str:
    """Read the persisted continuation token, validating it is decodable.

    A corrupt checkpoint is reported as ``cursor_malformed`` rather than
    surfacing as an empty page, so an audit puller can distinguish "nothing
    new" from "my checkpoint is broken".
    """
    source = Path(path)
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
        cursor = value["cursor"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise CursorMalformed(
            f"cannot read cursor file {path}: {exc}",
            detail={"path": str(path)}) from exc
    if not isinstance(cursor, str):
        raise CursorMalformed(f"cursor file {path} has no string cursor",
                              detail={"path": str(path)})
    Cursor.decode(cursor)  # normalise malformed tokens into one taxonomy
    return cursor
