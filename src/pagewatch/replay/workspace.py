"""Private temporary workspace for replay-time archive extraction.

Every replay run extracts the gzip archives it verifies into its own
private directory created with :func:`tempfile.mkdtemp`, so concurrent
replays of the same watch never share temp files.  The directory is
removed on normal exit, on ``KeyboardInterrupt``/``SIGTERM`` (via signal
handlers installed for the lifetime of the context) and as a last resort
from ``atexit``.  Cleanup is idempotent.
"""

from __future__ import annotations

import atexit
import hashlib
import shutil
import signal
import tempfile
from pathlib import Path
from types import FrameType

_CHUNK = 65536


class ReplayWorkspace:
    """A self-cleaning temporary directory for one replay run."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base_dir = Path(base_dir) if base_dir is not None else None
        self.path: Path | None = None
        self._cleaned = False
        self._previous_handlers: dict[int, object] = {}
        self._handlers_installed = False

    def __enter__(self) -> "ReplayWorkspace":
        if self._base_dir is not None:
            self._base_dir.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(
            prefix="pagewatch-replay-",
            dir=str(self._base_dir) if self._base_dir is not None else None,
        ))
        self._install_signal_handlers()
        atexit.register(self.cleanup)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.cleanup()
        return False

    # -- temp file production -------------------------------------------

    def extract_archive(self, archive_path: str | Path, digest: str) -> tuple[str, int]:
        """Decompress ``archive_path`` into the workspace and hash the bytes.

        Returns ``(actual_sha256, size_bytes)``.  The extracted file lives
        inside this workspace only and is removed by :meth:`cleanup`.
        """
        if self.path is None:
            raise RuntimeError("workspace is not open")
        if self._cleaned:
            raise RuntimeError("workspace has been cleaned up")
        destination = self.path / f"{digest}.raw"
        hasher = hashlib.sha256()
        size = 0
        import gzip  # local import keeps module import cheap

        with gzip.open(archive_path, "rb") as source:
            with destination.open("wb") as target:
                while True:
                    chunk = source.read(_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    target.write(chunk)
                    size += len(chunk)
        return hasher.hexdigest(), size

    # -- cleanup ---------------------------------------------------------

    def cleanup(self) -> None:
        """Remove the workspace directory; safe to call multiple times."""
        if self._cleaned:
            return
        self._cleaned = True
        self._restore_signal_handlers()
        try:
            atexit.unregister(self.cleanup)
        except Exception:
            pass
        if self.path is not None and self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)

    # -- signal handling -------------------------------------------------

    def _install_signal_handlers(self) -> None:
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                self._previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, self._handle_signal)
            self._handlers_installed = True
        except ValueError:
            # Not running in the main thread: signal handlers cannot be
            # installed.  try/finally and atexit still cover cleanup.
            self._previous_handlers = {}
            self._handlers_installed = False

    def _restore_signal_handlers(self) -> None:
        if not self._handlers_installed:
            return
        for signum, previous in self._previous_handlers.items():
            try:
                signal.signal(signum, previous)
            except (ValueError, OSError):
                pass
        self._handlers_installed = False

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        self.cleanup()
        previous = self._previous_handlers.get(signum)
        if previous is signal.SIG_IGN:
            return
        if callable(previous):
            # Chains to e.g. Python's default_int_handler, which raises
            # KeyboardInterrupt for SIGINT.
            previous(signum, frame)
            return
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + signum)
