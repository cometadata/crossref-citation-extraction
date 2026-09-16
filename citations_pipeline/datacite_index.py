"""DataCite release discovery and the verified index-bundle cache.

Discovery selects a complete mirrored monthly data file and computes its
source inventory digest exactly as the Rust reader does (see
`src/input/datacite_release.rs`), so both sides agree on the bundle cache key.

Bundles are cached beneath the owned prefix at
`indexes/<release>-v<format>-<inventory>/generations/<uuid>/` and never
modified once written. `_BUNDLE_CURRENT.json` in the cache-key directory
points at the generation to reuse. Every reuse copies the bundle into a local
staging directory and verifies the marker digest plus each file's size and
SHA-256 before the copy is promoted; every fresh build is verified the same
way before it is persisted as a new generation and the pointer is replaced.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .bucket import BucketStore
from .paths import (
    DATACITE_INDEX_VERSION,
    DATACITE_MARKER_VERSION,
    DATACITE_PREFIX_RE,
    NORMALIZATION_VERSION,
    PipelinePaths,
    bundle_prefix,
)
from .state import (
    StateError,
    _require_keys,
    _sha256,
    _version,
    read_json,
    sha256_file,
    write_atomic_json,
)

# Bundle layout written by `build-datacite-index` (src/index/datacite.rs).
BUNDLE_KIND = "datacite-index"
BUNDLE_MARKER_NAME = "_INDEX_COMPLETE.json"
BUNDLE_FILES = ("dois.fst", "hosts.fst", "urls.fst", "url-targets.bin")
# Cache pointer in the cache-key directory.
BUNDLE_POINTER_NAME = "_BUNDLE_CURRENT.json"
BUNDLE_CACHE_KIND = "datacite-bundle-cache"
GENERATIONS_DIR = "generations"
# Mirror entry contents.
TRANSFER_MARKER_NAME = "_TRANSFER_COMPLETE.json"
STATUS_NAME = "STATUS.json"
MANIFEST_NAME = "MANIFEST.json"
STATUS_COMPLETE = "Complete"

_DOIS_DIR = "dois"
_MONTH_DIR_PREFIX = "updated_"
_METADATA_SUFFIX = ".jsonl.gz"
_STATE_REPORT_SUFFIX = ".csv.gz"
_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
_RELATIVE_GENERATION_RE = re.compile(rf"^{GENERATIONS_DIR}/[0-9a-f]{{32}}$")
_POINTER_KEYS = {
    "kind",
    "schema_version",
    "release",
    "source_inventory_sha256",
    "relative_generation",
    "generation_manifest_sha256",
}
_MANIFEST_KEYS = {
    "kind",
    "format_version",
    "normalization_version",
    "release",
    "source_inventory_sha256",
    "files",
    "counts",
}


class DataciteIndexError(RuntimeError):
    pass


@dataclass(frozen=True)
class DataciteRelease:
    release: str
    root: Path
    inventory_sha256: str


@dataclass(frozen=True)
class BundleResult:
    directory: Path
    release: str
    inventory_sha256: str
    manifest_sha256: str
    reused: bool


# --- release discovery --------------------------------------------------------


def _parse_month(text: Any) -> tuple[int, int] | None:
    if not isinstance(text, str):
        return None
    match = _MONTH_RE.fullmatch(text)
    if match is None:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    if not 1 <= month <= 12:
        return None
    return year, month


def _read_status(root: Path) -> str:
    path = root / STATUS_NAME
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DataciteIndexError(f"cannot read {path}") from error
    if not isinstance(payload, dict):
        raise DataciteIndexError(f"{path} is not an object")
    status = payload.get("status")
    month = payload.get("month")
    if status != STATUS_COMPLETE:
        raise DataciteIndexError(
            f"{path}: release {month!r} status is {status!r}, not {STATUS_COMPLETE!r}"
        )
    if _parse_month(month) is None:
        raise DataciteIndexError(f"{path}: month {month!r} is not YYYY-MM")
    return month


def _source_kind(filename: str) -> str | None:
    if filename.endswith(_METADATA_SUFFIX):
        return "metadata"
    if filename.endswith(_STATE_REPORT_SUFFIX):
        return "state_report"
    return None


def _listed_month(filename: str) -> str:
    """Validate `dois/updated_YYYY-MM/<file>` and return the folder month.

    The month is never compared with the release month: a release generated
    on the 1st lists the next month's folder for the DOIs updated before the
    snapshot, and that folder is an input like any other."""
    parts = filename.split("/")
    if len(parts) != 3:
        raise DataciteIndexError(
            f"expected {_DOIS_DIR}/{_MONTH_DIR_PREFIX}YYYY-MM/<file>"
        )
    top, folder, basename = parts
    if top != _DOIS_DIR:
        raise DataciteIndexError(
            f"expected the first path component to be {_DOIS_DIR!r}, found {top!r}"
        )
    month = folder.removeprefix(_MONTH_DIR_PREFIX)
    if not folder.startswith(_MONTH_DIR_PREFIX) or _parse_month(month) is None:
        raise DataciteIndexError(f"folder {folder!r} is not {_MONTH_DIR_PREFIX}YYYY-MM")
    if basename in {"", ".", ".."} or "\\" in basename:
        raise DataciteIndexError(f"invalid file name {basename!r}")
    return month


def _decode_manifest_digest(encoded: Any) -> str:
    """Decode a manifest's base64 SHA-256 into lowercase hex."""
    if not isinstance(encoded, str):
        raise DataciteIndexError("sha256 must be a string")
    try:
        raw = base64.b64decode(encoded.strip(), validate=True)
    except (binascii.Error, ValueError) as error:
        raise DataciteIndexError(f"sha256 {encoded!r} is not valid base64") from error
    if len(raw) != 32:
        raise DataciteIndexError(
            f"sha256 {encoded!r} decodes to {len(raw)} bytes, expected 32 bytes"
        )
    return raw.hex()


def _read_manifest(root: Path) -> tuple[Path, list[Any]]:
    path = root / MANIFEST_NAME
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DataciteIndexError(f"cannot read {path}") from error
    if not isinstance(entries, list):
        raise DataciteIndexError(f"{path} must be a JSON list of manifest entries")
    return path, entries


def _inventory_digest(rows: list[list[Any]]) -> str:
    # Byte-for-byte the Rust `inventory_digest`: compact JSON over
    # `[relative_path, size, sha256_hex]` rows sorted by path, UTF-8, no
    # trailing newline.
    serialized = json.dumps(rows, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def release_inventory_sha256(root: Path, release: str) -> str:
    """Digest of the release's listed inputs, computed from `MANIFEST.json`.

    Inputs are the manifest entries ending in `.jsonl.gz` or `.csv.gz` at
    exactly `dois/updated_YYYY-MM/<file>`, whatever the folder month relative
    to `release`. A month folder listing JSONL metadata without a CSV state
    report is invalid: there is no JSONL-only fallback. File bytes are not
    read here; the Rust builder verifies them against the manifest.
    """
    if _parse_month(release) is None:
        raise DataciteIndexError(f"release {release!r} is not YYYY-MM")
    manifest_path, entries = _read_manifest(root)
    listed: dict[str, tuple[int, str]] = {}
    folders: dict[str, set[str]] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("filename"), str):
            raise DataciteIndexError(
                f"{manifest_path}: entry is not an object with a filename: {entry!r}"
            )
        filename = entry["filename"]
        kind = _source_kind(filename)
        if kind is None:
            continue
        context = f"{manifest_path}: entry {filename!r}"
        try:
            month = _listed_month(filename)
            size = entry.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise DataciteIndexError("size must be a non-negative integer")
            digest = _decode_manifest_digest(entry.get("sha256"))
        except DataciteIndexError as error:
            raise DataciteIndexError(f"{context}: {error}") from error
        if filename in listed:
            raise DataciteIndexError(f"{context}: duplicate entry")
        listed[filename] = (size, digest)
        folders.setdefault(month, set()).add(kind)
    if not listed:
        raise DataciteIndexError(
            f"{manifest_path}: lists no {_DOIS_DIR}/ JSONL or CSV inputs"
        )
    for month in sorted(folders):
        if "metadata" in folders[month] and "state_report" not in folders[month]:
            raise DataciteIndexError(
                f"{manifest_path}: {_DOIS_DIR}/{_MONTH_DIR_PREFIX}{month} lists JSONL "
                "metadata but no .csv.gz state report; refusing JSONL-only coverage"
            )
    rows = [[path, size, digest] for path, (size, digest) in sorted(listed.items())]
    return _inventory_digest(rows)


def discover_release(bucket_root: Path, pinned: str | None = None) -> DataciteRelease:
    """Select the pinned or newest complete `datacite-monthly-data-file-YYYY-MM`
    entry directly under `bucket_root` and validate its release."""
    candidates: dict[str, Path] = {}
    if bucket_root.is_dir():
        for entry in bucket_root.iterdir():
            match = DATACITE_PREFIX_RE.fullmatch(entry.name)
            if match is None or not entry.is_dir():
                continue
            if not (entry / TRANSFER_MARKER_NAME).is_file():
                continue
            candidates[match.group(1)] = entry
    if pinned is not None:
        if pinned not in candidates:
            raise DataciteIndexError(
                f"pinned DataCite release {pinned} is not complete in the bucket"
            )
        release = pinned
    elif candidates:
        release = max(candidates)
    else:
        raise DataciteIndexError(
            "no complete DataCite monthly data file found in the bucket"
        )
    root = candidates[release]
    status_month = _read_status(root)
    if status_month != release:
        raise DataciteIndexError(
            f"{root / STATUS_NAME}: month {status_month} does not match the "
            f"mirror entry {release}"
        )
    return DataciteRelease(
        release=release,
        root=root,
        inventory_sha256=release_inventory_sha256(root, release),
    )


# --- bundle verification ------------------------------------------------------


def _plain_file_name(name: Any) -> bool:
    return (
        isinstance(name, str)
        and name not in {"", ".", ".."}
        and not any(character in name for character in "/\\\0")
    )


def _validate_manifest(manifest: Any, release: DataciteRelease) -> None:
    _require_keys(manifest, _MANIFEST_KEYS, "bundle manifest")
    if manifest["kind"] != BUNDLE_KIND:
        raise StateError(f"kind must be {BUNDLE_KIND!r}")
    _version(manifest["format_version"], DATACITE_INDEX_VERSION, "format_version")
    _version(
        manifest["normalization_version"],
        NORMALIZATION_VERSION,
        "normalization_version",
    )
    if manifest["release"] != release.release:
        raise StateError(f"release must be {release.release}")
    if manifest["source_inventory_sha256"] != release.inventory_sha256:
        raise StateError("source_inventory_sha256 does not match the release inventory")
    files = manifest["files"]
    if not isinstance(files, dict):
        raise StateError("files must be an object")
    for name, entry in files.items():
        if not _plain_file_name(name) or name == BUNDLE_MARKER_NAME:
            raise StateError(f"invalid bundle file name {name!r}")
        _require_keys(entry, {"size", "sha256"}, f"files[{name}]")
        size = entry["size"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise StateError(f"files[{name}].size must be a non-negative integer")
        _sha256(entry["sha256"], f"files[{name}].sha256")
    for name in BUNDLE_FILES:
        if name not in files:
            raise StateError(f"files does not list {name}")
    counts = manifest["counts"]
    if not isinstance(counts, dict) or any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in counts.values()
    ):
        raise StateError("counts must map names to non-negative integers")


def _bundle_manifest(
    marker_path: Path, release: DataciteRelease
) -> tuple[dict[str, Any], str]:
    """Read and validate a bundle marker; return it with the SHA-256 of its
    exact bytes (the bundle digest)."""
    try:
        raw = marker_path.read_bytes()
    except OSError as error:
        raise DataciteIndexError(f"cannot read bundle marker {marker_path}") from error
    digest = hashlib.sha256(raw).hexdigest()
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DataciteIndexError(f"bundle marker is not JSON: {marker_path}") from error
    try:
        _validate_manifest(manifest, release)
    except StateError as error:
        raise DataciteIndexError(
            f"invalid bundle marker {marker_path.name}: {error}"
        ) from error
    return manifest, digest


def _verify_files(directory: Path, manifest: dict[str, Any]) -> None:
    """Check every listed file's size and digest in `directory`."""
    for name, metadata in manifest["files"].items():
        candidate = directory / name
        try:
            size = candidate.stat().st_size
        except OSError as error:
            raise DataciteIndexError(f"bundle file missing: {name}") from error
        if size != metadata["size"]:
            raise DataciteIndexError(f"bundle size mismatch: {name}")
        if sha256_file(candidate) != metadata["sha256"]:
            raise DataciteIndexError(f"bundle checksum mismatch: {name}")


def _read_pointer(path: Path, release: DataciteRelease) -> dict[str, Any]:
    payload = read_json(path)
    _require_keys(payload, _POINTER_KEYS, "bundle pointer")
    if payload["kind"] != BUNDLE_CACHE_KIND:
        raise StateError(f"bundle pointer kind must be {BUNDLE_CACHE_KIND!r}")
    _version(payload["schema_version"], DATACITE_MARKER_VERSION, "schema_version")
    if payload["release"] != release.release:
        raise StateError(
            f"bundle pointer release {payload['release']!r} does not match "
            f"{release.release}"
        )
    if payload["source_inventory_sha256"] != release.inventory_sha256:
        raise StateError(
            "bundle pointer source_inventory_sha256 does not match the release inventory"
        )
    relative = payload["relative_generation"]
    if not isinstance(relative, str) or not _RELATIVE_GENERATION_RE.fullmatch(relative):
        raise StateError(f"bundle pointer relative_generation is invalid: {relative!r}")
    _sha256(payload["generation_manifest_sha256"], "generation_manifest_sha256")
    return payload


def _promote(staging: Path, destination: Path) -> Path:
    """Atomically rename a verified staging directory into place."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    os.replace(staging, destination)
    return destination


def _reuse_cached_bundle(
    cache_root: Path,
    local_root: Path,
    release: DataciteRelease,
    events: Any,
) -> BundleResult | None:
    pointer_path = cache_root / BUNDLE_POINTER_NAME
    if not pointer_path.is_file():
        events.emit("bundle_cache_missing", release=release.release, cache=str(cache_root))
        return None
    staging = local_root / f".stage-{uuid.uuid4().hex}"
    try:
        with events.phase("fetch_cached_bundle"):
            pointer = _read_pointer(pointer_path, release)
            generation = cache_root / pointer["relative_generation"]
            manifest, digest = _bundle_manifest(generation / BUNDLE_MARKER_NAME, release)
            if digest != pointer["generation_manifest_sha256"]:
                raise DataciteIndexError(
                    f"cached bundle marker digest mismatch: {BUNDLE_MARKER_NAME}"
                )
            staging.mkdir(parents=True)
            for name in [*manifest["files"], BUNDLE_MARKER_NAME]:
                try:
                    shutil.copyfile(generation / name, staging / name)
                except OSError as error:
                    raise DataciteIndexError(
                        f"cannot copy cached bundle file {name}: {error}"
                    ) from error
            _verify_files(staging, manifest)
            if sha256_file(staging / BUNDLE_MARKER_NAME) != digest:
                raise DataciteIndexError(
                    f"bundle checksum mismatch: {BUNDLE_MARKER_NAME}"
                )
            generation_name = pointer["relative_generation"].split("/")[1]
            local = _promote(staging, local_root / GENERATIONS_DIR / generation_name)
    except (DataciteIndexError, StateError, OSError) as error:
        shutil.rmtree(staging, ignore_errors=True)
        events.emit(
            "bundle_cache_invalid",
            release=release.release,
            cache=str(cache_root),
            reason=str(error),
        )
        return None
    write_atomic_json(local_root / BUNDLE_POINTER_NAME, pointer)
    events.emit(
        "bundle_cache_reused",
        release=release.release,
        relative_generation=pointer["relative_generation"],
        manifest_sha256=digest,
    )
    return BundleResult(
        directory=local,
        release=release.release,
        inventory_sha256=release.inventory_sha256,
        manifest_sha256=digest,
        reused=True,
    )


def _build_bundle(
    *,
    cache_root: Path,
    local_root: Path,
    release: DataciteRelease,
    build_index: Callable[[Path, Path], None],
    store: BucketStore | None,
    events: Any,
) -> BundleResult:
    local_root.mkdir(parents=True, exist_ok=True)
    build_dir = local_root / f".build-{uuid.uuid4().hex}"
    generation_name = uuid.uuid4().hex
    try:
        with events.phase("build_datacite_index"):
            build_index(release.root, build_dir)
            manifest, digest = _bundle_manifest(build_dir / BUNDLE_MARKER_NAME, release)
            _verify_files(build_dir, manifest)
        local = _promote(build_dir, local_root / GENERATIONS_DIR / generation_name)
    except BaseException:
        shutil.rmtree(build_dir, ignore_errors=True)
        raise
    relative_generation = f"{GENERATIONS_DIR}/{generation_name}"
    pointer = {
        "kind": BUNDLE_CACHE_KIND,
        "schema_version": DATACITE_MARKER_VERSION,
        "release": release.release,
        "source_inventory_sha256": release.inventory_sha256,
        "relative_generation": relative_generation,
        "generation_manifest_sha256": digest,
    }
    events.emit(
        "bundle_built",
        release=release.release,
        manifest_sha256=digest,
        counts=manifest["counts"],
    )
    if store is not None:
        remote = cache_root / relative_generation
        with events.phase("persist_bundle"):
            # A new immutable generation: every file, then the marker; the
            # pointer is replaced only once the generation is fully uploaded.
            for name in sorted(manifest["files"]):
                store.put_file(local / name, remote / name)
            store.put_file(local / BUNDLE_MARKER_NAME, remote / BUNDLE_MARKER_NAME)
            write_atomic_json(local_root / BUNDLE_POINTER_NAME, pointer)
            store.put_json(pointer, cache_root / BUNDLE_POINTER_NAME)
        events.emit(
            "bundle_persisted",
            release=release.release,
            relative_generation=relative_generation,
        )
    else:
        write_atomic_json(local_root / BUNDLE_POINTER_NAME, pointer)
    return BundleResult(
        directory=local,
        release=release.release,
        inventory_sha256=release.inventory_sha256,
        manifest_sha256=digest,
        reused=False,
    )


def ensure_bundle(
    *,
    paths: PipelinePaths,
    release: DataciteRelease,
    build_index: Callable[[Path, Path], None],
    store: BucketStore | None,
    events: Any,
    force: bool = False,
) -> BundleResult:
    """Return a verified local copy of the release's index bundle.

    Without `force`, a cached generation that verifies completely is copied
    and reused. Otherwise (or on any verification failure) the bundle is
    rebuilt with `build_index(release_root, output_dir)`, verified, persisted
    as a new generation through `store` (skipped when `store` is None), and
    the cache pointer is replaced. Existing generations are never modified.
    `BundleResult.directory` always names the verified local copy.
    """
    relative = bundle_prefix(release.release, release.inventory_sha256)
    cache_root = paths.prefix / relative
    local_root = paths.bundle_local / relative.name
    if force:
        events.emit("bundle_cache_forced", release=release.release, cache=str(cache_root))
    else:
        reused = _reuse_cached_bundle(cache_root, local_root, release, events)
        if reused is not None:
            return reused
    return _build_bundle(
        cache_root=cache_root,
        local_root=local_root,
        release=release,
        build_index=build_index,
        store=store,
        events=events,
    )
