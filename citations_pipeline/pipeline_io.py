"""Snapshot staging, compression, and file-inventory helpers shared by the
legacy arXiv pipeline (`pipeline.py`) and the full DataCite pipeline
(`full_pipeline.py`).

Staging hashes the Crossref archive while it is copied off the mount, so the
digest that enters a run identity is the digest of the bytes the extractor
reads. Fetching a durable file back from the bucket uses the same single-pass
copy-and-hash and refuses any size or digest mismatch by name.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import zstandard

from .full_state import FileMeta
from .mirror import ResolvedSnapshot
from .paths import PipelinePaths
from .state import sha256_file
from .telemetry import ProgressTracker

STAGE_CHUNK_BYTES = 16 * 1024 * 1024


class PipelineError(RuntimeError):
    pass


def count_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _line in handle)


def copy_with_digest(
    source: Path,
    target: Path,
    *,
    advance: Callable[[int], None] | None = None,
) -> tuple[int, str]:
    """Copy `source` to `target` in one pass; return `(size, sha256)`."""
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with source.open("rb") as src, target.open("wb") as dst:
        while chunk := src.read(STAGE_CHUNK_BYTES):
            dst.write(chunk)
            digest.update(chunk)
            size += len(chunk)
            if advance is not None:
                advance(len(chunk))
    return size, digest.hexdigest()


def stage_snapshot(
    paths: PipelinePaths,
    resolved: ResolvedSnapshot,
    events: Any,
    *,
    verify_digest: bool = True,
) -> Path:
    """Copy the resolved archive into the local work tree, hashing as it goes.

    A fallback download already lives locally and was hashed as it arrived,
    so it is reused as is. Otherwise the mirror archive is copied to
    `paths.snapshot_local`; with `verify_digest` the digest computed during
    the copy must equal the one the mirror or transfer marker recorded.
    """
    if resolved.archive_is_local:
        return resolved.archive_path
    source = resolved.archive_path
    current_size = source.stat().st_size if source.is_file() else -1
    if current_size != resolved.archive_size:
        raise PipelineError(
            "mirror archive changed before staging; rerun, or set "
            f"CROSSREF_API_TOKEN for the fallback download: {source}"
        )
    target = paths.snapshot_local
    target.parent.mkdir(parents=True, exist_ok=True)

    def report(payload: dict[str, object]) -> None:
        events.emit("progress", **payload)

    with events.phase("stage_snapshot"):
        with ProgressTracker(
            phase="stage_snapshot",
            total_bytes=resolved.archive_size,
            progress=report,
        ) as tracker:
            staged, digest = copy_with_digest(source, target, advance=tracker.advance)
    if staged != resolved.archive_size:
        raise PipelineError(
            f"staged archive size mismatch: {staged} != {resolved.archive_size}"
        )
    if verify_digest and digest != resolved.archive_sha256:
        raise PipelineError(
            f"staged archive digest mismatch: {digest} != {resolved.archive_sha256}"
        )
    events.emit(
        "snapshot_staged",
        archive_sha256=digest,
        archive_size=staged,
        verified=verify_digest,
    )
    return target


def compress_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    compressor = zstandard.ZstdCompressor()
    with source.open("rb") as src, target.open("wb") as dst:
        compressor.copy_stream(src, dst)


def tree_file_metadata(root: Path) -> dict[str, dict[str, Any]]:
    """`{relative_path: {"size", "sha256"}}` for every file under `root`."""
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            files[path.relative_to(root).as_posix()] = {
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
    return files


def fetch_verified_file(
    source: Path, target: Path, expected: FileMeta, *, name: str
) -> Path:
    """Copy a durable file locally, hashing it on the way; refuse any size or
    digest mismatch, naming the file."""
    if not source.is_file():
        raise PipelineError(f"derived file {name} is missing: {source}")
    size, digest = copy_with_digest(source, target)
    if size != expected.size:
        target.unlink(missing_ok=True)
        raise PipelineError(
            f"derived file {name} is {size} bytes, expected {expected.size}"
        )
    if digest != expected.sha256:
        target.unlink(missing_ok=True)
        raise PipelineError(
            f"derived file {name} digest {digest} does not match expected "
            f"{expected.sha256}"
        )
    return target


def verify_file_metadata(
    root: Path, files: dict[str, FileMeta], *, label: str
) -> None:
    """Read every listed file back from `root` (the bucket mount) and hash it;
    fail on the first missing file or size/digest mismatch, naming it."""
    for name, expected in sorted(files.items()):
        candidate = root / name
        if not candidate.is_file():
            raise PipelineError(f"{label}: {name} is missing after upload")
        size = candidate.stat().st_size
        if size != expected.size:
            raise PipelineError(
                f"{label}: {name} is {size} bytes after upload, expected {expected.size}"
            )
        digest = sha256_file(candidate)
        if digest != expected.sha256:
            raise PipelineError(
                f"{label}: {name} digest {digest} after upload does not match "
                f"{expected.sha256}"
            )
