"""Content-addressed, deterministic gzip archive of raw responses."""

from __future__ import annotations

import gzip
import hashlib
import os
import tempfile
from pathlib import Path


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def archive_path(archive_dir: str | Path, digest: str) -> Path:
    return Path(archive_dir) / f"{digest}.gz"


def write_archive(data: bytes, archive_dir: str | Path) -> tuple[Path, str, bool]:
    """Store raw bytes once per content hash.

    Returns (path, sha256, created).  Compression uses mtime=0 so archives and
    their hashes are reproducible.  Writes are atomic and duplicate-safe.
    """
    directory = Path(archive_dir)
    digest = sha256_bytes(data)
    destination = archive_path(directory, digest)
    if destination.exists():
        return destination, digest, False
    directory.mkdir(parents=True, exist_ok=True)
    compressed = gzip.compress(data, compresslevel=6, mtime=0)
    fd, temp_name = tempfile.mkstemp(prefix=f".{digest}.", suffix=".tmp",
                                     dir=str(directory))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, destination)
        created = True
    except FileExistsError:
        created = False
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return destination, digest, created


def read_archive(path: str | Path) -> bytes:
    with gzip.open(path, "rb") as handle:
        return handle.read()


def archive_size(path: str | Path) -> int:
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0
