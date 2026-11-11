"""Strict local-target allowlist, checked before any socket is opened."""

from __future__ import annotations

from urllib.parse import urlsplit

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


class AllowlistError(PermissionError):
    """Raised when a target is outside the built-in local allowlist."""


def is_host_allowed(host: str | None) -> bool:
    if not host:
        return False
    return host.strip().lower() in LOOPBACK_HOSTS


def host_from_url(url: str) -> str | None:
    try:
        return urlsplit(url).hostname
    except ValueError:
        return None


def ensure_url_allowed(url: str) -> str:
    """Validate URL shape and return its host; raise before network activity."""
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise AllowlistError(f"invalid URL: {exc}") from exc
    if parts.scheme not in {"http", "https"}:
        raise AllowlistError("only http and https targets are supported")
    if parts.username or parts.password:
        raise AllowlistError("URLs containing credentials are not allowed")
    host = parts.hostname
    if not is_host_allowed(host):
        rendered = host or "<missing host>"
        raise AllowlistError(
            f"target host {rendered!r} is blocked by the local allowlist; "
            f"allowed hosts: {', '.join(sorted(LOOPBACK_HOSTS))}"
        )
    return host or ""


def guard_redirect_url(url: str) -> None:
    """Reject a redirect unless its target is also locally allowlisted."""
    ensure_url_allowed(url)
