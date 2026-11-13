"""Opaque, checksummed continuation tokens for history pagination.

Design contract
---------------

The cursor is **not** an offset.  It encodes:

* a filter fingerprint, so a token cannot silently be reused with a different
  watch/severity/tag/time selection;
* ``after`` - the highest event sequence that has been fully examined at the
  same time as a fixed snapshot watermark.  It is also the exclusive lower
  bound carried from one round to the next;
* ``watermark`` - the inclusive high-water event sequence of the current
  export round; events written concurrently after the watermark are deferred
  to the next round instead of shifting already-read pages;
* ``position`` - the ``(created_at, watch_name, id)`` of the last emitted
  event inside that frozen watermark, used as a keyset for the deterministic
  ``(created_at, watch_name, id)`` ordering (so several watches changing at
  the same instant always sort the same way);
* ``resolved_names`` - for tag filters only, the watch-name set resolved when
  the round started, frozen for the duration of the round so a metadata edit
  cannot make already-unread rows vanish;
* a SHA-256 checksum so truncation/corruption is reported as a distinct
  ``cursor_malformed`` error rather than silently returning empty results.

Once a round is drained the cursor intentionally drops ``watermark`` and
``position`` and only keeps ``after``.  The next pull re-reads metadata and
snapshots a fresh watermark, which is what makes incremental export continue
from exactly where the previous round ended while new events keep arriving.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
from dataclasses import dataclass

from .errors import CursorMalformed, CursorOutOfRange

CURSOR_VERSION = 1
_CHECKSUM_DOMAIN = b"pagewatch-history-cursor-v1"
_FIELDS = {"v", "q", "a", "w", "p", "r", "c"}


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _checksum(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical(payload) + b"\n" + _CHECKSUM_DOMAIN).hexdigest()


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(float(value)))


@dataclass(frozen=True)
class Cursor:
    """Decoded cursor state.  Use :meth:`encode` to hand it to a caller."""

    query_fingerprint: str
    after: int = 0
    watermark: int | None = None
    position: tuple[float, str, int] | None = None
    resolved_names: tuple[str, ...] | None = None

    @property
    def finished(self) -> bool:
        """True for an end-of-round cursor (next pull starts a new round)."""
        return self.watermark is None and self.position is None

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "v": CURSOR_VERSION,
            "q": self.query_fingerprint,
            "a": int(self.after),
            "w": None if self.watermark is None else int(self.watermark),
            "p": None if self.position is None else [self.position[0],
                                                     self.position[1],
                                                     int(self.position[2])],
        }
        if self.resolved_names is not None:
            payload["r"] = list(self.resolved_names)
        return payload

    def encode(self) -> str:
        payload = self.payload()
        envelope = {**payload, "c": _checksum(payload)}
        blob = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
        return base64.urlsafe_b64encode(blob.encode("utf-8")).decode("ascii")

    @classmethod
    def decode(cls, token: str) -> "Cursor":
        if not isinstance(token, str) or not token.strip():
            raise CursorMalformed("cursor is empty")
        try:
            raw = base64.urlsafe_b64decode(token.strip().encode("ascii"))
        except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
            raise CursorMalformed("cursor is not valid base64url", detail={
                "reason": str(exc)}) from exc
        try:
            envelope = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CursorMalformed("cursor payload is not valid JSON", detail={
                "reason": str(exc)}) from exc
        if not isinstance(envelope, dict):
            raise CursorMalformed("cursor payload must be a JSON object")
        unknown = set(envelope) - _FIELDS
        if unknown:
            raise CursorMalformed("cursor has unknown field(s)",
                                  detail={"fields": sorted(unknown)})
        missing = _FIELDS - {"r"} - set(envelope)
        if missing:
            raise CursorMalformed("cursor is missing required field(s)",
                                  detail={"fields": sorted(missing)})
        if not _is_int(envelope["v"]) or envelope["v"] != CURSOR_VERSION:
            raise CursorMalformed("unsupported cursor version",
                                  detail={"version": envelope.get("v")})
        payload = {key: value for key, value in envelope.items() if key != "c"}
        checksum = envelope.get("c")
        if not isinstance(checksum, str) or not hmac.compare_digest(
                checksum, _checksum(payload)):
            raise CursorMalformed("cursor checksum mismatch (token was "
                                  "truncated or modified)")
        fingerprint = envelope.get("q")
        if not isinstance(fingerprint, str) or not fingerprint:
            raise CursorMalformed("cursor query fingerprint is invalid")
        after = envelope.get("a")
        if not _is_int(after) or after < 0:
            raise CursorMalformed("cursor 'after' must be a non-negative integer")
        watermark = envelope.get("w")
        if watermark is not None and (not _is_int(watermark) or watermark < 0):
            raise CursorMalformed("cursor watermark must be a non-negative "
                                  "integer or null")
        position = envelope.get("p")
        if position is not None:
            if (not isinstance(position, list) or len(position) != 3
                    or not _is_number(position[0])
                    or not isinstance(position[1], str) or not position[1]
                    or not _is_int(position[2])):
                raise CursorMalformed("cursor position must be "
                                      "[created_at, watch_name, id]")
            position = (float(position[0]), position[1], int(position[2]))
        resolved = envelope.get("r")
        if resolved is not None:
            if (not isinstance(resolved, list)
                    or any(not isinstance(name, str) or not name
                           for name in resolved)):
                raise CursorMalformed("cursor resolved names must be a list of "
                                      "non-empty strings")
            resolved = tuple(resolved)
        if position is not None and watermark is None:
            raise CursorMalformed("cursor has a position without a snapshot "
                                  "watermark")
        if watermark is not None and watermark < after:
            raise CursorMalformed("cursor watermark precedes its 'after' bound")
        if position is not None:
            if watermark is None or position[2] > watermark:
                raise CursorMalformed("cursor position lies outside its snapshot")
            if position[2] <= after:
                raise CursorMalformed("cursor position must follow 'after'")
        if resolved is not None and position is None:
            raise CursorMalformed("resolved tag names are only valid inside "
                                  "an active snapshot")
        return cls(query_fingerprint=fingerprint, after=int(after),
                   watermark=None if watermark is None else int(watermark),
                   position=position, resolved_names=resolved)


# Aliases used by the public package surface; they mirror CursorMalformed's
# role but keep the error taxonomy readable in caller code.
CursorExpired = CursorOutOfRange
