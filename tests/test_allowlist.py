from __future__ import annotations

import hashlib

import pytest
import requests

from pagewatch.fetch.allowlist import (
    AllowlistError, ensure_url_allowed, host_from_url, is_host_allowed,
)
from pagewatch.fetch.archive import read_archive, sha256_bytes, write_archive
from pagewatch.fetch.client import FetchClient


@pytest.mark.parametrize("url, host", [
    ("http://127.0.0.1:8765/a", "127.0.0.1"),
    ("https://localhost/path", "localhost"),
    ("http://[::1]:8080/", "::1"),
])
def test_local_targets_are_allowed(url, host):
    assert ensure_url_allowed(url) == host
    assert host_from_url(url) == host


def test_host_matcher_is_strict():
    assert is_host_allowed("127.0.0.1")
    assert is_host_allowed("LOCALHOST")
    assert not is_host_allowed("127.0.0.2")
    assert not is_host_allowed("localhost.example")
    assert not is_host_allowed(None)


@pytest.mark.parametrize("url, fragment", [
    ("https://outside.invalid/page", "blocked by the local allowlist"),
    ("http://user:pass@127.0.0.1/x", "credentials"),
    ("ftp://127.0.0.1/x", "only http"),
    ("http://[bad", "invalid URL"),
])
def test_nonlocal_or_malformed_targets_are_rejected(url, fragment):
    with pytest.raises(AllowlistError, match=fragment):
        ensure_url_allowed(url)
    with pytest.raises(AllowlistError):
        FetchClient().fetch(url)


def test_allowlist_rejection_happens_before_http(monkeypatch):
    called = {"value": False}

    def fail_if_called(*args, **kwargs):
        called["value"] = True
        raise AssertionError("network layer must not be reached")

    monkeypatch.setattr(requests.Session, "get", fail_if_called)
    with pytest.raises(AllowlistError):
        FetchClient(retries=0).fetch("https://outside.invalid/")
    assert called["value"] is False


def test_archive_is_content_addressed_deduplicated_and_roundtrips(tmp_path):
    body = "<html><body>same bytes</body></html>".encode("utf-8")
    first_path, digest, first_created = write_archive(body, tmp_path)
    second_path, second_digest, second_created = write_archive(body, tmp_path)
    assert first_created is True
    assert second_created is False
    assert first_path == second_path
    assert digest == second_digest == sha256_bytes(body)
    assert digest == hashlib.sha256(body).hexdigest()
    assert first_path.name == f"{digest}.gz"
    assert read_archive(first_path) == body
