"""HTML cleanup and CSS-selector based region selection."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence

from bs4 import BeautifulSoup, Comment, Tag

REMOVE_TAGS = ("script", "style", "noscript", "template", "iframe", "svg")
BLOCK_TAGS = (
    "p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "tr",
    "pre", "blockquote", "dt", "dd", "figcaption", "summary",
)
NOISE_CLASSES = frozenset({"noise", "dynamic", "counter", "timestamp",
                           "analytics", "session", "random"})
_SPACE = re.compile(r"\s+")


def normalize_whitespace(text: str) -> str:
    return _SPACE.sub(" ", text).strip()


def prepare_soup(html: str, exclude_selectors: Sequence[str] = ()) -> BeautifulSoup:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(REMOVE_TAGS):
        tag.decompose()
    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()
    for selector in exclude_selectors:
        for element in soup.select(selector):
            element.decompose()
    return soup


def included_roots(soup: BeautifulSoup,
                   include_selectors: Sequence[str]) -> list[Tag]:
    if not include_selectors:
        return [soup]
    chosen: list[Tag] = []
    for selector in include_selectors:
        for element in soup.select(selector):
            if any(ancestor is element or element in ancestor.descendants
                   for ancestor in chosen):
                continue
            if any(root is element or root in element.descendants for root in chosen):
                continue
            chosen.append(element)
    return chosen


def _is_focus(element: Tag, focus_ids: set[int]) -> bool:
    current: Tag | None = element
    while current is not None and isinstance(current, Tag):
        if id(current) in focus_ids:
            return True
        current = current.parent if isinstance(current.parent, Tag) else None
    return False


def _is_noise(element: Tag) -> bool:
    current: Tag | None = element
    while current is not None and isinstance(current, Tag):
        classes = {str(value).lower() for value in current.get("class", [])}
        if classes & NOISE_CLASSES or current.has_attr("data-noise"):
            return True
        current = current.parent if isinstance(current.parent, Tag) else None
    return False


def _region(element: Tag) -> str:
    current: Tag | None = element
    while current is not None and isinstance(current, Tag):
        if current.get("id"):
            return f"#{current['id']}"
        classes = [str(value) for value in current.get("class", [])]
        if classes:
            return f".{classes[0]}"
        current = current.parent if isinstance(current.parent, Tag) else None
    return element.name or "document"


def _has_block_descendant(element: Tag) -> bool:
    return any(isinstance(child, Tag) and child.name in BLOCK_TAGS
               for child in element.find_all(True))


def _candidate_elements(root: Tag) -> list[Tag]:
    if root.name in BLOCK_TAGS:
        return [root]
    return root.find_all(BLOCK_TAGS)


def extract_blocks(soup: BeautifulSoup, roots: Iterable[Tag],
                   focus_selectors: Sequence[str] = (),
                   *, document_scope: bool = True) -> list[dict[str, object]]:
    """Extract ordered text blocks from *roots* only.

    ``document_scope`` is true only when no include selector was configured.
    With an explicit selection scope we never widen extraction to the whole
    document: a selected root without block descendants contributes its own
    text, and a selection that matches nothing yields no text at all.  This
    keeps extraction (and therefore the content hash and diff) inside the
    configured region, so excluded markup such as page footers cannot leak
    back in through a fallback.
    """
    focus_ids = {id(element) for selector in focus_selectors
                 for element in soup.select(selector)}
    values: list[dict[str, object]] = []
    for root in roots:
        candidates = _candidate_elements(root)
        if not candidates and not document_scope:
            text = normalize_whitespace(root.get_text(" ", strip=True))
            if not text:
                continue
            values.append({
                "text": text,
                "tag": root.name or "document",
                "region": _region(root),
                "is_focus": _is_focus(root, focus_ids),
                "is_noise": _is_noise(root),
            })
            continue
        for element in candidates:
            if _has_block_descendant(element):
                continue
            text = normalize_whitespace(element.get_text(" ", strip=True))
            if not text:
                continue
            values.append({
                "text": text,
                "tag": element.name or "document",
                "region": _region(element),
                "is_focus": _is_focus(element, focus_ids),
                "is_noise": _is_noise(element),
            })
    if not values and document_scope and soup.body is not None:
        text = normalize_whitespace(soup.body.get_text(" ", strip=True))
        if text:
            values.append({
                "text": text, "tag": "body", "region": "body",
                "is_focus": False, "is_noise": False,
            })
    return values
