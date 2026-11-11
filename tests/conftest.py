"""Shared fixtures: offline socket guard, frozen clock, local mock server."""

from __future__ import annotations

import logging
import socket
import threading
from pathlib import Path

import pytest
from werkzeug.serving import make_server

from mocksite.app import create_app
from mocksite.scenarios import ScenarioState
from pagewatch.clock import FrozenClock
from pagewatch.models import Rules

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", ""}


class OfflineGuard:
    def __init__(self) -> None:
        self.attempts: list[tuple[str, int | str]] = []

    @staticmethod
    def address_host(address: object) -> str:
        if isinstance(address, tuple) and address:
            return str(address[0]).lower()
        if isinstance(address, (str, bytes)):
            return str(address)
        return str(address)

    @classmethod
    def is_local(cls, address: object) -> bool:
        host = cls.address_host(address)
        return host in LOCAL_HOSTS or host.startswith(("/", ".", "\\"))

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_connect = socket.socket.connect
        original_connect_ex = socket.socket.connect_ex
        guard = self

        def guarded_connect(sock, address):
            if not guard.is_local(address):
                guard.attempts.append((guard.address_host(address), "connect"))
                raise AssertionError(f"offline guard blocked non-local connect: {address}")
            return original_connect(sock, address)

        def guarded_connect_ex(sock, address):
            if not guard.is_local(address):
                guard.attempts.append((guard.address_host(address), "connect_ex"))
                raise AssertionError(f"offline guard blocked non-local connect_ex: {address}")
            return original_connect_ex(sock, address)

        monkeypatch.setattr(socket.socket, "connect", guarded_connect)
        monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)


@pytest.fixture(scope="session", autouse=True)
def offline_guard():
    guard = OfflineGuard()
    monkeypatch = pytest.MonkeyPatch()
    guard.install(monkeypatch)
    yield guard
    monkeypatch.undo()
    assert guard.attempts == [], f"non-local socket attempts: {guard.attempts}"


@pytest.fixture
def frozen_clock() -> FrozenClock:
    return FrozenClock(1_767_225_600.0)


@pytest.fixture
def rules() -> Rules:
    return Rules()


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_server():
    scenario = ScenarioState()
    app = create_app(scenario)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    logger = logging.getLogger("werkzeug")
    previous_level = logger.level
    logger.setLevel(logging.WARNING)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", scenario
    finally:
        server.shutdown()
        thread.join(timeout=5)
        logger.setLevel(previous_level)
