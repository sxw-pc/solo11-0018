"""Per-event content fingerprints rebuilt from stored normalized blocks.

This module only *reads* the snapshot chain (``blocks_json`` produced by the
normalizer); it never re-fetches or re-normalizes anything.
"""

from __future__ import annotations

from ..models import ChangeEvent, Snapshot
from ..normalize.pipeline import blocks_from_json, content_hash
from .model import EventFingerprint


def _eligible_hashes(blocks_json: str, *, min_block_chars: int) -> dict[str, str]:
    """Map content hash -> block text for fingerprint-eligible blocks.

    Noise blocks are excluded: masking rewrites volatile fragments to the
    same ``[NOISE]`` placeholder on every page, so their hashes would collide
    across unrelated watches.  Very short blocks are excluded for the same
    reason (generic labels appear everywhere).
    """
    hashes: dict[str, str] = {}
    for block in blocks_from_json(blocks_json):
        if block.is_noise:
            continue
        text = block.text.strip()
        if len(text) < min_block_chars:
            continue
        hashes.setdefault(content_hash(text), text)
    return hashes


def build_fingerprint(event: ChangeEvent, from_snapshot: Snapshot | None,
                      to_snapshot: Snapshot | None, *,
                      min_block_chars: int) -> EventFingerprint | None:
    """Fingerprint one event; ``None`` when its snapshots are gone.

    A retention pass may have pruned the snapshots an event refers to.  That
    is a normal degraded condition, not an error, so callers get ``None``
    instead of an exception.
    """
    if from_snapshot is None or to_snapshot is None:
        return None
    old = _eligible_hashes(from_snapshot.blocks_json,
                           min_block_chars=min_block_chars)
    new = _eligible_hashes(to_snapshot.blocks_json,
                           min_block_chars=min_block_chars)
    added = {key: new[key] for key in sorted(set(new) - set(old))}
    removed = {key: old[key] for key in sorted(set(old) - set(new))}
    url = to_snapshot.url or from_snapshot.url or None
    return EventFingerprint(
        event_id=event.id, watch_name=event.watch_name,
        created_at=float(event.created_at), url=url,
        added=added, removed=removed,
    )
