"""Cross-process locking so concurrent sync commands cannot interleave.

SQLite's own locking serializes database writes, but the replication protocol
also mutates the spool directory and appends to a JSONL outbox, which are not
transactional resources.  A single advisory lock per role therefore guards a
whole push or pull run; a second command fails fast with a clear message
instead of racing.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Type


class LockBusy(RuntimeError):
    """Raised when another sync process already holds the requested lock."""


class FileLock:
    def __init__(self, locks_dir: str | Path, name: str) -> None:
        self.path = Path(locks_dir) / f"{name}.lock"
        self._handle: int | None = None

    def acquire(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                os.close(handle)
                raise LockBusy(
                    f"another sync process holds lock {self.path}"
                ) from exc
        except ImportError:  # pragma: no cover - platform fallback
            pass
        self._handle = handle
        return self

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            import fcntl

            fcntl.flock(self._handle, fcntl.LOCK_UN)
        except ImportError:  # pragma: no cover
            pass
        os.close(self._handle)
        self._handle = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type: Type[BaseException] | None,
                 exc: BaseException | None,
                 traceback: TracebackType | None) -> None:
        self.release()


@contextmanager
def held_lock(locks_dir: str | Path, name: str):
    lock = FileLock(locks_dir, name).acquire()
    try:
        yield lock
    finally:
        lock.release()
