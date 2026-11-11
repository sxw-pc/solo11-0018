from __future__ import annotations

import pytest

from pagewatch.fetch.client import FetchClient, FetchError, decode_body


def test_conditional_fetch_304_and_size_limit(mock_server):
    base_url, _ = mock_server
    first = FetchClient(timeout_seconds=2, retries=0).fetch(f"{base_url}/docs")
    assert first.status_code == 200
    assert first.etag and first.last_modified
    assert "本地部署手册" in first.text
    assert first.raw_sha256

    second = FetchClient(timeout_seconds=2, retries=0).fetch(
        f"{base_url}/docs", etag=first.etag, last_modified=first.last_modified)
    assert second.status_code == 304
    assert second.not_modified is True
    assert second.body == b""
    assert second.raw_sha256 is None

    with pytest.raises(FetchError, match="too large"):
        FetchClient(timeout_seconds=2, max_bytes=10, retries=0).fetch(
            f"{base_url}/docs")


def test_decode_body_falls_back_for_unknown_or_bad_encoding():
    class Response:
        encoding = "definitely-not-an-encoding"

    assert decode_body(Response(), b"plain ascii") == "plain ascii"
    assert "�" in decode_body(Response(), b"\xff\xfe")
