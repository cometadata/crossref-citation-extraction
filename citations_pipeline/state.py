from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .paths import (
    DOWNLOAD_PROTOCOL_VERSION,
    EXTRACTION_SCHEMA_VERSION,
    FST_SCHEMA_VERSION,
    MARKER_SCHEMA_VERSION,
    validate_generation_prefix,
)
from .telemetry import ProgressTracker


class StateError(RuntimeError):
    pass


def _require_keys(payload: Any, expected: set[str], marker: str) -> None:
    if not isinstance(payload, dict):
        raise StateError(f"{marker} must be an object")
    missing = expected - payload.keys()
    extra = payload.keys() - expected
    if missing or extra:
        raise StateError(
            f"Invalid {marker} keys: missing={sorted(missing)}, extra={sorted(extra)}"
        )


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise StateError(f"{name} must be a positive integer")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise StateError(f"{name} must be a nonempty string")
    return value


def _sha256(value: Any, name: str) -> str:
    digest = _string(value, name)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise StateError(f"{name} must be a lowercase SHA-256 digest")
    return digest


def _version(value: Any, expected: int, name: str) -> int:
    if value != expected or isinstance(value, bool):
        raise StateError(f"{name} must equal {expected}")
    return expected


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def write_atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StateError(f"Cannot read JSON marker {path}") from error
    if not isinstance(payload, dict):
        raise StateError(f"JSON marker is not an object: {path}")
    return payload


def sha256_file(
    path: Path,
    chunk_size: int = 8 * 1024 * 1024,
    *,
    progress: Callable[[dict[str, object]], None] | None = None,
    phase: str = "checksum_file",
    monotonic: Callable[[], float] = time.monotonic,
    progress_interval: float = 60.0,
) -> str:
    digest = hashlib.sha256()
    with ProgressTracker(
        phase=phase,
        total_bytes=path.stat().st_size,
        progress=progress,
        interval=progress_interval,
        monotonic=monotonic,
    ) as tracker, path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
            tracker.advance(len(chunk))
    return digest.hexdigest()


_SNAPSHOT_ID_RE = re.compile(r"^\d{4}-\d{2}$")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
_PROVENANCES = {"mirror", "download"}


def _release(value: Any, name: str) -> str:
    text = _string(value, name)
    if not _SNAPSHOT_ID_RE.fullmatch(text):
        raise StateError(f"{name} must have YYYY-MM form")
    return text


def _generation(value: Any, root: str, name: str) -> str:
    text = _string(value, name)
    try:
        return validate_generation_prefix(text, root)
    except ValueError as error:
        raise StateError(f"{name} is invalid: {error}") from error


def _provenance(value: Any, name: str) -> str:
    text = _string(value, name)
    if text not in _PROVENANCES:
        raise StateError(f"{name} must be one of {sorted(_PROVENANCES)}")
    return text


def _commit(value: Any, name: str) -> str:
    text = _string(value, name)
    if not _COMMIT_RE.fullmatch(text):
        raise StateError(f"{name} must be a 40-character lowercase commit hash")
    return text


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StateError(f"{name} must be an object")
    return value


def _files(value: Any, name: str) -> dict[str, dict[str, Any]]:
    entries = _mapping(value, name)
    validated: dict[str, dict[str, Any]] = {}
    for key, entry in entries.items():
        relative = _string(key, f"{name} key")
        _require_keys(entry, {"size", "sha256"}, f"{name} entry {relative}")
        validated[relative] = {
            "size": _positive_int(entry["size"], f"{name}[{relative}].size"),
            "sha256": _sha256(entry["sha256"], f"{name}[{relative}].sha256"),
        }
    return validated


@dataclass(frozen=True)
class TransferMarker:
    schema_version: int
    download_protocol_version: int
    snapshot: dict[str, Any]
    snapshot_id: str
    archive_sha256: str
    archive_size: int
    raw_prefix: str
    source_tree_sha256: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> TransferMarker:
        _require_keys(
            payload,
            {
                "schema_version",
                "download_protocol_version",
                "snapshot",
                "snapshot_id",
                "archive_sha256",
                "archive_size",
                "raw_prefix",
                "source_tree_sha256",
                "completed_at",
            },
            "transfer marker",
        )
        if not isinstance(payload["snapshot"], dict):
            raise StateError("transfer marker snapshot must be an object")
        return cls(
            schema_version=_version(
                payload["schema_version"], MARKER_SCHEMA_VERSION, "schema_version"
            ),
            download_protocol_version=_version(
                payload["download_protocol_version"],
                DOWNLOAD_PROTOCOL_VERSION,
                "download_protocol_version",
            ),
            snapshot=payload["snapshot"],
            snapshot_id=_release(payload["snapshot_id"], "snapshot_id"),
            archive_sha256=_sha256(payload["archive_sha256"], "archive_sha256"),
            archive_size=_positive_int(payload["archive_size"], "archive_size"),
            raw_prefix=_generation(
                payload["raw_prefix"], "raw-generations", "raw_prefix"
            ),
            source_tree_sha256=_sha256(
                payload["source_tree_sha256"], "source_tree_sha256"
            ),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


@dataclass(frozen=True)
class FstMarker:
    schema_version: int
    fst_schema_version: int
    datacite_release: str
    fst_sha256: str
    fst_size: int
    extractor_version: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FstMarker:
        _require_keys(
            payload,
            {
                "schema_version",
                "fst_schema_version",
                "datacite_release",
                "fst_sha256",
                "fst_size",
                "extractor_version",
                "completed_at",
            },
            "fst marker",
        )
        return cls(
            schema_version=_version(
                payload["schema_version"], MARKER_SCHEMA_VERSION, "schema_version"
            ),
            fst_schema_version=_version(
                payload["fst_schema_version"],
                FST_SCHEMA_VERSION,
                "fst_schema_version",
            ),
            datacite_release=_release(
                payload["datacite_release"], "datacite_release"
            ),
            fst_sha256=_sha256(payload["fst_sha256"], "fst_sha256"),
            fst_size=_positive_int(payload["fst_size"], "fst_size"),
            extractor_version=_string(
                payload["extractor_version"], "extractor_version"
            ),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


@dataclass(frozen=True)
class ExtractionMarker:
    schema_version: int
    extraction_schema_version: int
    snapshot_id: str
    snapshot_provenance: str
    archive_sha256: str
    datacite_release: str
    fst_sha256: str
    source_tree_sha256: str
    extractor_version: str
    derived_prefix: str
    files: dict[str, dict[str, Any]]
    counts: dict[str, Any]
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ExtractionMarker:
        _require_keys(
            payload,
            {
                "schema_version",
                "extraction_schema_version",
                "snapshot_id",
                "snapshot_provenance",
                "archive_sha256",
                "datacite_release",
                "fst_sha256",
                "source_tree_sha256",
                "extractor_version",
                "derived_prefix",
                "files",
                "counts",
                "completed_at",
            },
            "extraction marker",
        )
        return cls(
            schema_version=_version(
                payload["schema_version"], MARKER_SCHEMA_VERSION, "schema_version"
            ),
            extraction_schema_version=_version(
                payload["extraction_schema_version"],
                EXTRACTION_SCHEMA_VERSION,
                "extraction_schema_version",
            ),
            snapshot_id=_release(payload["snapshot_id"], "snapshot_id"),
            snapshot_provenance=_provenance(
                payload["snapshot_provenance"], "snapshot_provenance"
            ),
            archive_sha256=_sha256(payload["archive_sha256"], "archive_sha256"),
            datacite_release=_release(
                payload["datacite_release"], "datacite_release"
            ),
            fst_sha256=_sha256(payload["fst_sha256"], "fst_sha256"),
            source_tree_sha256=_sha256(
                payload["source_tree_sha256"], "source_tree_sha256"
            ),
            extractor_version=_string(
                payload["extractor_version"], "extractor_version"
            ),
            derived_prefix=_generation(
                payload["derived_prefix"], "derived-generations", "derived_prefix"
            ),
            files=_files(payload["files"], "files"),
            counts=_mapping(payload["counts"], "counts"),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


@dataclass(frozen=True)
class PipelineMarker:
    schema_version: int
    snapshot_id: str
    snapshot_provenance: str
    archive_sha256: str
    datacite_release: str
    fst_sha256: str
    source_tree_sha256: str
    derived_prefix: str
    raw_prefix: str | None
    dataset_repo: str
    dataset_commit: str
    dataset_commit_url: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> PipelineMarker:
        _require_keys(
            payload,
            {
                "schema_version",
                "snapshot_id",
                "snapshot_provenance",
                "archive_sha256",
                "datacite_release",
                "fst_sha256",
                "source_tree_sha256",
                "derived_prefix",
                "raw_prefix",
                "dataset_repo",
                "dataset_commit",
                "dataset_commit_url",
                "completed_at",
            },
            "pipeline marker",
        )
        raw_prefix = payload["raw_prefix"]
        return cls(
            schema_version=_version(
                payload["schema_version"], MARKER_SCHEMA_VERSION, "schema_version"
            ),
            snapshot_id=_release(payload["snapshot_id"], "snapshot_id"),
            snapshot_provenance=_provenance(
                payload["snapshot_provenance"], "snapshot_provenance"
            ),
            archive_sha256=_sha256(payload["archive_sha256"], "archive_sha256"),
            datacite_release=_release(
                payload["datacite_release"], "datacite_release"
            ),
            fst_sha256=_sha256(payload["fst_sha256"], "fst_sha256"),
            source_tree_sha256=_sha256(
                payload["source_tree_sha256"], "source_tree_sha256"
            ),
            derived_prefix=_generation(
                payload["derived_prefix"], "derived-generations", "derived_prefix"
            ),
            raw_prefix=(
                None
                if raw_prefix is None
                else _generation(raw_prefix, "raw-generations", "raw_prefix")
            ),
            dataset_repo=_string(payload["dataset_repo"], "dataset_repo"),
            dataset_commit=_commit(payload["dataset_commit"], "dataset_commit"),
            dataset_commit_url=_string(
                payload["dataset_commit_url"], "dataset_commit_url"
            ),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


def can_reuse_extraction(
    marker: ExtractionMarker,
    *,
    archive_sha256: str,
    datacite_release: str,
    fst_sha256: str,
    source_tree_sha256: str,
) -> bool:
    return (
        marker.archive_sha256 == archive_sha256
        and marker.datacite_release == datacite_release
        and marker.fst_sha256 == fst_sha256
        and marker.source_tree_sha256 == source_tree_sha256
    )
