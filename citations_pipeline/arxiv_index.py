from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .bucket import BucketStore
from .paths import (
    DATACITE_PREFIX_RE,
    FST_SCHEMA_VERSION,
    MARKER_SCHEMA_VERSION,
    PipelinePaths,
)
from .state import FstMarker, StateError, read_json, sha256_file, utc_now


class ArxivIndexError(RuntimeError):
    pass


@dataclass(frozen=True)
class DataciteRelease:
    release: str
    dois_path: Path


@dataclass(frozen=True)
class FstResult:
    local_path: Path
    sha256: str
    size: int
    release: str
    reused: bool


def discover_datacite_release(
    bucket_root: Path, pinned: str | None = None
) -> DataciteRelease:
    candidates: dict[str, Path] = {}
    if bucket_root.is_dir():
        for entry in bucket_root.iterdir():
            match = DATACITE_PREFIX_RE.fullmatch(entry.name)
            if match is None or not entry.is_dir():
                continue
            if not (entry / "_TRANSFER_COMPLETE.json").is_file():
                continue
            if not (entry / "dois").is_dir():
                continue
            candidates[match.group(1)] = entry / "dois"
    if pinned is not None:
        if pinned not in candidates:
            raise ArxivIndexError(
                f"pinned DataCite release {pinned} is not complete in the bucket"
            )
        return DataciteRelease(release=pinned, dois_path=candidates[pinned])
    if not candidates:
        raise ArxivIndexError(
            "no complete DataCite monthly data file found in the bucket"
        )
    latest = max(candidates)
    return DataciteRelease(release=latest, dois_path=candidates[latest])


def _cached_marker(paths: PipelinePaths, release: str) -> FstMarker | None:
    cache = paths.fst_prefix(release)
    if not cache.marker.is_file() or not cache.fst.is_file():
        return None
    try:
        marker = FstMarker.from_dict(read_json(cache.marker))
    except (StateError, ValueError, OSError):
        return None
    if marker.datacite_release != release:
        return None
    if cache.fst.stat().st_size != marker.fst_size:
        return None
    return marker


def ensure_fst(
    *,
    paths: PipelinePaths,
    release: DataciteRelease,
    build_index: Callable[[Path, Path], None],
    extractor_version: str,
    store: BucketStore | None,
    events: Any,
    force: bool = False,
) -> FstResult:
    local = paths.fst_local
    local.parent.mkdir(parents=True, exist_ok=True)

    if not force:
        marker = _cached_marker(paths, release.release)
        if marker is not None:
            cache = paths.fst_prefix(release.release)
            with events.phase("fetch_cached_fst"):
                shutil.copyfile(cache.fst, local)
                digest = sha256_file(local)
            if digest == marker.fst_sha256:
                events.emit("fst_cache_reused", release=release.release)
                return FstResult(
                    local_path=local,
                    sha256=digest,
                    size=marker.fst_size,
                    release=release.release,
                    reused=True,
                )
            events.emit("fst_cache_checksum_mismatch", release=release.release)

    with events.phase("build_fst"):
        build_index(release.dois_path, local)
        digest = sha256_file(local)
        size = local.stat().st_size

    marker = FstMarker(
        schema_version=MARKER_SCHEMA_VERSION,
        fst_schema_version=FST_SCHEMA_VERSION,
        datacite_release=release.release,
        fst_sha256=digest,
        fst_size=size,
        extractor_version=extractor_version,
        completed_at=utc_now(),
    )
    if store is not None:
        cache = paths.fst_prefix(release.release)
        with events.phase("persist_fst"):
            store.put_file(local, cache.fst)
            store.put_json(marker.to_dict(), cache.marker)
    return FstResult(
        local_path=local,
        sha256=digest,
        size=size,
        release=release.release,
        reused=False,
    )
