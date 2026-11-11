"""Session-wide offline assertion plus a small local-only smoke check."""

from __future__ import annotations

import socket


def test_no_nonlocal_connection_attempts_were_recorded(offline_guard):
    assert offline_guard.attempts == []


def test_guard_host_classification(offline_guard):
    assert offline_guard.is_local(("127.0.0.1", 80))
    assert offline_guard.is_local(("::1", 80, 0, 0))
    assert offline_guard.is_local(("/tmp/local.sock",))
    assert not offline_guard.is_local(("203.0.113.10", 443))
    assert not offline_guard.is_local(("outside.invalid", 443))


def test_local_socket_connect_is_allowed(offline_guard):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        # Port 1 is expected to refuse; the point is that the guard permits
        # loopback and records no external attempt.
        try:
            sock.connect(("127.0.0.1", 1))
        except OSError:
            pass
    finally:
        sock.close()
    assert offline_guard.attempts == []
