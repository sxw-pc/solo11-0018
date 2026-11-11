"""Deterministic HTML -> normalized text + ordered block pipeline."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

from .. import NORMALIZER_VERSION
from ..models import NormalizedBlock, NormalizedPage
from .noise import mask_noise
from .selectors import extract_blocks, included_roots, normalize_whitespace, prepare_soup


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def blocks_to_json(blocks: Sequence[NormalizedBlock]) -> str:
    return json.dumps([block.as_dict() for block in blocks],
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def blocks_from_json(value: str) -> tuple[NormalizedBlock, ...]:
    if not value:
        return ()
    try:
        raw = json.loads(value)
    except (TypeError, ValueError):
        return ()
    return tuple(NormalizedBlock.from_dict(item) for item in raw)


def normalize_html(
    html: str,
    *,
    include_selectors: Sequence[str] = (),
    exclude_selectors: Sequence[str] = (),
    noise_patterns: Sequence[str] = (),
    focus_selectors: Sequence[str] = (),
) -> NormalizedPage:
    """Strip unsafe/irrelevant markup, select regions, mask noise, hash text.

    The output text consists of one line per selected block.  This makes the
    line-oriented diff correspond directly to semantically meaningful blocks.
    """
    soup = prepare_soup(html, exclude_selectors)
    roots = included_roots(soup, include_selectors)
    raw_blocks = extract_blocks(soup, roots, focus_selectors)
    normalized: list[NormalizedBlock] = []
    for index, raw in enumerate(raw_blocks):
        masked = mask_noise(str(raw["text"]), noise_patterns)
        text = normalize_whitespace(masked)
        if not text:
            continue
        normalized.append(NormalizedBlock(
            index=len(normalized),
            text=text,
            tag=str(raw["tag"]),
            region=str(raw["region"]),
            is_focus=bool(raw["is_focus"]),
            is_noise=bool(raw["is_noise"]),
        ))
    full_text = "\n".join(block.text for block in normalized)
    return NormalizedPage(
        text=full_text,
        blocks=tuple(normalized),
        content_hash=content_hash(full_text),
        normalizer_version=NORMALIZER_VERSION,
    )
