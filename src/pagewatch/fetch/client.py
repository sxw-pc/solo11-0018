"""Small, conservative HTTP client with conditional requests and hard limits."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

import requests

from .. import USER_AGENT
from .allowlist import AllowlistError, ensure_url_allowed

REDIRECT_CODES = {301, 302, 303, 307, 308}


class FetchError(RuntimeError):
    """A fetch failed without producing usable content."""


@dataclass(frozen=True)
class FetchResult:
    status_code: int
    url: str
    body: bytes
    text: str
    etag: str | None
    last_modified: str | None
    elapsed_ms: int
    not_modified: bool

    @property
    def raw_sha256(self) -> str | None:
        if self.not_modified:
            return None
        return hashlib.sha256(self.body).hexdigest()


def decode_body(response: requests.Response, body: bytes) -> str:
    encoding = response.encoding or "utf-8"
    try:
        return body.decode(encoding, errors="strict")
    except (LookupError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")


class FetchClient:
    def __init__(self, *, timeout_seconds: float = 5.0,
                 max_bytes: int = 2_097_152, retries: int = 1) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.retries = max(0, retries)

    def fetch(self, url: str, *, etag: str | None = None,
              last_modified: str | None = None) -> FetchResult:
        ensure_url_allowed(url)
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
            "Accept-Encoding": "gzip, deflate",
        }
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        last_error: Exception | None = None
        session = requests.Session()
        session.trust_env = False
        try:
            for attempt in range(self.retries + 1):
                started = time.monotonic()
                response = None
                try:
                    response = session.get(
                        url,
                        headers=headers,
                        timeout=(self.timeout_seconds, self.timeout_seconds),
                        stream=True,
                        allow_redirects=False,
                    )
                    elapsed_ms = int((time.monotonic() - started) * 1000)
                    return self._consume(response, url, elapsed_ms)
                except AllowlistError:
                    raise
                except (requests.RequestException, FetchError, ConnectionError) as exc:
                    last_error = exc
                    if isinstance(exc, FetchError) and "too large" in str(exc):
                        raise
                    if attempt >= self.retries:
                        break
                    time.sleep(min(0.2 * (2 ** attempt), 2.0))
                finally:
                    if response is not None:
                        response.close()
        finally:
            session.close()
        raise FetchError(f"fetch failed for {url}: {last_error}") from last_error

    def _consume(self, response: requests.Response, url: str,
                 elapsed_ms: int) -> FetchResult:
        if response.status_code == 304:
            return FetchResult(
                status_code=304, url=url, body=b"", text="",
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
                elapsed_ms=elapsed_ms, not_modified=True,
            )
        if response.status_code in REDIRECT_CODES:
            raise FetchError(
                f"redirect response {response.status_code} was not followed; "
                "update the watch URL to the final local target"
            )
        if not 200 <= response.status_code < 300:
            raise FetchError(f"HTTP {response.status_code} for {url}")
        length = response.headers.get("Content-Length")
        if length:
            try:
                if int(length) > self.max_bytes:
                    raise FetchError(f"response body too large (limit {self.max_bytes})")
            except ValueError:
                pass
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=65_536):
            if not chunk:
                continue
            total += len(chunk)
            if total > self.max_bytes:
                raise FetchError(f"response body too large (limit {self.max_bytes})")
            chunks.append(chunk)
        body = b"".join(chunks)
        return FetchResult(
            status_code=response.status_code,
            url=url,
            body=body,
            text=decode_body(response, body),
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            elapsed_ms=elapsed_ms,
            not_modified=False,
        )
