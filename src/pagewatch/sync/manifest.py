"""Content-addressed batch manifests and the on-disk spool layout.

Why content addressing (the central trade-off of the sync link)
---------------------------------------------------------------
Three transport units exist — raw gzip archives, normalized snapshot rows and
event/outbox rows — and they carry hard cross-references (an event points at
two snapshot ids; an outbox line points at an event id).  Sending them as
loose files would make "what belongs together" unknowable on the receiving
side, so each transfer window is instead one *batch*:

* every payload is stored once under ``blobs/<sha256>`` (content addressing —
  identical bytes, including retried batches, share one file and never
  collide);
* the manifest enumerates each item with its own hash, so a corrupt or
  truncated transfer is detected per item before any database write;
* ``batch_id`` is the SHA-256 of the canonical manifest itself, hence a pure
  function of the batch's content.  Re-staging after an interrupted transfer
  produces the same id, which makes the batch ledger an idempotency key with
  no extra sequence negotiation and no duplicate event rows on retry.

A batch is the atomic unit of ingestion: the receiving side verifies hashes
and referential integrity first and only then applies everything inside one
SQLite transaction plus one atomic outbox append.  Any failure rolls the
whole batch back — a peer can never end up holding events whose snapshots are
missing.

Spool layout (a plain local directory; works over mounted disks / rsync)::

    <spool>/
      locks/                 # flock-based coordination files
      batches/<batch_id>/
        manifest.json
        blobs/<sha256>
      applied/               # manifests moved here after durable application
      failed/                # manifests moved here with an .error sidecar
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from ..storage.db import json_dump

MANIFEST_FORMAT_VERSION = 1
MANIFEST_NAME = "manifest.json"
BLOBS_DIRNAME = "blobs"

ITEM_GZIP = "gzip"
ITEM_SNAPSHOT = "snapshot"
ITEM_EVENT = "event"
ITEM_OUTBOX = "outbox"

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class ManifestError(ValueError):
    """Raised when a manifest or one of its payloads fails verification."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json_bytes(value: object) -> bytes:
    return json_dump(value).encode("utf-8")


@dataclass(frozen=True)
class BatchItem:
    item_id: str
    item_type: str
    payload: bytes
    meta: dict[str, object]

    @property
    def digest(self) -> str:
        return sha256_hex(self.payload)

    def as_manifest_entry(self) -> dict[str, object]:
        return {
            "item_id": self.item_id,
            "type": self.item_type,
            "sha256": self.digest,
            "size": len(self.payload),
            "meta": dict(self.meta),
        }


def gzip_item(digest: str, gzip_bytes: bytes) -> BatchItem:
    return BatchItem(
        item_id=f"{ITEM_GZIP}:{digest}",
        item_type=ITEM_GZIP,
        payload=gzip_bytes,
        meta={"digest": digest},
    )


def snapshot_item(row: dict[str, object]) -> BatchItem:
    payload = canonical_json_bytes(row)
    return BatchItem(
        item_id=f"{ITEM_SNAPSHOT}:{int(row['id'])}",
        item_type=ITEM_SNAPSHOT,
        payload=payload,
        meta={"watch_name": str(row["watch_name"]),
              "version": int(row["version"])},
    )


def event_item(row: dict[str, object]) -> BatchItem:
    payload = canonical_json_bytes(row)
    return BatchItem(
        item_id=f"{ITEM_EVENT}:{int(row['id'])}",
        item_type=ITEM_EVENT,
        payload=payload,
        meta={"watch_name": str(row["watch_name"])},
    )


def outbox_item(seq: int, line_bytes: bytes) -> BatchItem:
    return BatchItem(
        item_id=f"{ITEM_OUTBOX}:{seq}",
        item_type=ITEM_OUTBOX,
        payload=line_bytes,
        meta={"seq": seq},
    )


@dataclass(frozen=True)
class BatchManifest:
    batch_id: str
    batch_seq: int
    watermarks: dict[str, object]
    items: tuple[dict[str, object], ...]
    manifest_sha256: str

    def to_json(self) -> dict[str, object]:
        return {
            "format_version": MANIFEST_FORMAT_VERSION,
            "batch_id": self.batch_id,
            "batch_seq": self.batch_seq,
            "watermarks": self.watermarks,
            "items": list(self.items),
            "manifest_sha256": self.manifest_sha256,
        }


def _hash_body(manifest: BatchManifest) -> dict[str, object]:
    """Content covered by the batch id.

    ``batch_seq`` is deliberately excluded: it is a monotonic allocation hint
    for ordering/debugging, and including it would make a retried batch get a
    different id (the source ledger had already consumed a sequence number),
    breaking the content-addressed idempotency guarantee.
    """
    return {
        "format_version": MANIFEST_FORMAT_VERSION,
        "watermarks": manifest.watermarks,
        "items": list(manifest.items),
    }


def build_manifest(batch_seq: int, watermarks: dict[str, object],
                   items: list[BatchItem]) -> BatchManifest:
    entries = tuple(item.as_manifest_entry() for item in items)
    manifest = BatchManifest(
        batch_id="",
        batch_seq=int(batch_seq),
        watermarks=watermarks,
        items=entries,
        manifest_sha256="",
    )
    digest = sha256_hex(canonical_json_bytes(_hash_body(manifest)))
    return BatchManifest(
        batch_id=digest,
        batch_seq=int(batch_seq),
        watermarks=watermarks,
        items=entries,
        manifest_sha256=digest,
    )


def manifest_from_json(data: dict[str, object]) -> BatchManifest:
    try:
        entries = tuple(dict(entry) for entry in data["items"])
        return BatchManifest(
            batch_id=str(data["batch_id"]),
            batch_seq=int(data["batch_seq"]),
            watermarks=dict(data["watermarks"]),
            items=entries,
            manifest_sha256=str(data["manifest_sha256"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ManifestError(f"malformed manifest: {exc}") from exc


class Spool:
    """Filesystem layout for staged/applied/failed batches."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.batches_dir = self.root / "batches"
        self.applied_dir = self.root / "applied"
        self.failed_dir = self.root / "failed"
        self.locks_dir = self.root / "locks"

    def ensure(self) -> None:
        for directory in (self.batches_dir, self.applied_dir,
                          self.failed_dir, self.locks_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def batch_dir(self, batch_id: str) -> Path:
        return self.batches_dir / batch_id

    def applied_manifest(self, batch_id: str) -> Path:
        return self.applied_dir / f"{batch_id}.json"

    def failed_manifest(self, batch_id: str) -> Path:
        return self.failed_dir / f"{batch_id}.json"

    def failed_error(self, batch_id: str) -> Path:
        return self.failed_dir / f"{batch_id}.error"

    def list_staged(self) -> list[Path]:
        if not self.batches_dir.exists():
            return []
        return sorted(
            path for path in self.batches_dir.iterdir()
            if path.is_dir() and _HEX64.fullmatch(path.name)
        )

    def stage_batch(self, manifest: BatchManifest,
                    items: list[BatchItem]) -> Path:
        """Write blobs then the manifest; fully idempotent on retry."""
        batch_dir = self.batch_dir(manifest.batch_id)
        blobs_dir = batch_dir / BLOBS_DIRNAME
        blobs_dir.mkdir(parents=True, exist_ok=True)
        for item in items:
            blob_path = blobs_dir / item.digest
            if blob_path.exists():
                continue
            self._atomic_write(blob_path, item.payload)
        manifest_path = batch_dir / MANIFEST_NAME
        self._atomic_write(
            manifest_path,
            canonical_json_bytes(manifest.to_json()) + b"\n",
        )
        return batch_dir

    def load_manifest(self, batch_dir: Path) -> BatchManifest:
        manifest_path = batch_dir / MANIFEST_NAME
        try:
            raw = manifest_path.read_bytes()
        except OSError as exc:
            raise ManifestError(f"cannot read manifest {manifest_path}: {exc}") from exc
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
        return manifest_from_json(data)

    def read_blob(self, batch_dir: Path, digest: str) -> bytes:
        if not _HEX64.fullmatch(digest):
            raise ManifestError(f"refuse non-hash blob name: {digest!r}")
        path = batch_dir / BLOBS_DIRNAME / digest
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ManifestError(f"missing blob {digest}: {exc}") from exc

    def mark_applied(self, batch_id: str) -> None:
        source = self.batch_dir(batch_id) / MANIFEST_NAME
        if source.exists():
            self._atomic_write(self.applied_manifest(batch_id), source.read_bytes())
        self._remove_batch_dir(batch_id)

    def mark_failed(self, batch_id: str, error: str) -> None:
        source = self.batch_dir(batch_id) / MANIFEST_NAME
        if source.exists():
            self._atomic_write(self.failed_manifest(batch_id), source.read_bytes())
        self._atomic_write(self.failed_error(batch_id),
                           error.encode("utf-8", "replace"))
        self._remove_batch_dir(batch_id)

    def _remove_batch_dir(self, batch_id: str) -> None:
        batch_dir = self.batch_dir(batch_id)
        if not batch_dir.exists():
            return
        for path in sorted(batch_dir.rglob("*"), reverse=True):
            if path.is_file() or path.is_symlink():
                path.unlink()
        for path in sorted(batch_dir.rglob("*"), reverse=True):
            if path.is_dir():
                path.rmdir()
        batch_dir.rmdir()

    @staticmethod
    def _atomic_write(destination: Path, data: bytes) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.",
                                         suffix=".tmp",
                                         dir=str(destination.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, destination)
        except OSError:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
            raise


def verify_manifest_hash(manifest: BatchManifest) -> None:
    """Recompute the manifest hash from its content (defence against tampering)."""
    if not _HEX64.fullmatch(manifest.batch_id) or \
            not _HEX64.fullmatch(manifest.manifest_sha256):
        raise ManifestError("batch id / manifest hash must be sha256 hex")
    if manifest.batch_id != manifest.manifest_sha256:
        raise ManifestError("batch id does not match manifest hash")
    actual = sha256_hex(canonical_json_bytes(_hash_body(manifest)))
    if actual != manifest.manifest_sha256:
        raise ManifestError(
            f"manifest hash mismatch: declared {manifest.manifest_sha256}, "
            f"computed {actual}"
        )


def verify_item_payload(entry: dict[str, object], payload: bytes) -> None:
    """Per-item hash/size check plus cheap intrinsic sanity checks."""
    digest = str(entry.get("sha256", ""))
    if not _HEX64.fullmatch(digest) or sha256_hex(payload) != digest:
        raise ManifestError(f"payload hash mismatch for {entry.get('item_id')}")
    if int(entry.get("size", -1)) != len(payload):
        raise ManifestError(f"payload size mismatch for {entry.get('item_id')}")
    item_type = str(entry.get("type"))
    if item_type == ITEM_GZIP:
        declared = str(entry.get("meta", {}).get("digest", ""))
        if not _HEX64.fullmatch(declared):
            raise ManifestError("gzip item lacks valid content digest")
        try:
            raw = gzip.decompress(payload)
        except (OSError, EOFError) as exc:
            raise ManifestError(f"gzip payload unreadable: {exc}") from exc
        if sha256_hex(raw) != declared:
            raise ManifestError(
                "gzip content does not match its content-addressed digest"
            )
    elif item_type in (ITEM_SNAPSHOT, ITEM_EVENT, ITEM_OUTBOX):
        try:
            parsed = json.loads(payload)
        except ValueError as exc:
            raise ManifestError(f"{item_type} payload is not JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ManifestError(f"{item_type} payload must be a JSON object")
    else:
        raise ManifestError(f"unknown item type: {item_type!r}")
