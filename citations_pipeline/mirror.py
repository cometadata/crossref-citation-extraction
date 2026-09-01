from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import (
    AFFILIATIONS_PREFIX_NAME,
    ARCHIVE_NAME,
    MIRROR_MARKER_SCHEMA_VERSION,
    GenerationPathError,
    validate_generation_prefix,
)
from .state import read_json

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SNAPSHOT_ID_RE = re.compile(r"^\d{4}-\d{2}$")


class MirrorError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResolvedSnapshot:
    snapshot_id: str
    provenance: str
    archive_sha256: str
    archive_size: int
    archive_path: Path
    archive_is_local: bool
    source: dict[str, Any]


def _read_marker(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise MirrorError(f"affiliations {label} not found: {path}")
    try:
        payload = read_json(path)
    except (OSError, ValueError) as error:
        raise MirrorError(f"affiliations {label} is unreadable: {error}") from error
    if not isinstance(payload, dict):
        raise MirrorError(f"affiliations {label} is not a JSON object")
    return payload


def resolve_mirror_snapshot(bucket_root: Path) -> ResolvedSnapshot:
    prefix = bucket_root / AFFILIATIONS_PREFIX_NAME
    pipeline = _read_marker(prefix / "_PIPELINE_COMPLETE.json", "pipeline marker")

    if pipeline.get("schema_version") != MIRROR_MARKER_SCHEMA_VERSION:
        raise MirrorError(
            "affiliations marker schema_version is not "
            f"{MIRROR_MARKER_SCHEMA_VERSION}: {pipeline.get('schema_version')!r}"
        )
    snapshot_id = pipeline.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not _SNAPSHOT_ID_RE.fullmatch(snapshot_id):
        raise MirrorError(f"affiliations marker snapshot_id is invalid: {snapshot_id!r}")
    raw_prefix_value = pipeline.get("raw_prefix")
    if not isinstance(raw_prefix_value, str):
        raise MirrorError("affiliations marker raw_prefix is missing")
    try:
        raw_prefix = validate_generation_prefix(raw_prefix_value, "raw-generations")
    except GenerationPathError as error:
        raise MirrorError(f"affiliations marker raw_prefix is invalid: {error}") from error
    archive_sha256 = pipeline.get("archive_sha256")
    if not isinstance(archive_sha256, str) or not _SHA256_RE.fullmatch(archive_sha256):
        raise MirrorError("affiliations marker archive_sha256 is invalid")

    transfer = _read_marker(
        prefix / raw_prefix / "_TRANSFER_COMPLETE.json", "transfer marker"
    )
    if transfer.get("archive_sha256") != archive_sha256:
        raise MirrorError("affiliations markers disagree on archive_sha256")
    archive_size = transfer.get("archive_size")
    if not isinstance(archive_size, int) or isinstance(archive_size, bool):
        raise MirrorError("affiliations transfer marker archive_size is invalid")
    if archive_size <= 0:
        raise MirrorError("affiliations transfer marker archive_size is invalid")

    archive_path = prefix / raw_prefix / ARCHIVE_NAME
    if not archive_path.is_file():
        raise MirrorError(f"mirror archive is missing: {archive_path}")
    actual_size = archive_path.stat().st_size
    if actual_size != archive_size:
        raise MirrorError(
            f"mirror archive size mismatch: {actual_size} != {archive_size}"
        )

    return ResolvedSnapshot(
        snapshot_id=snapshot_id,
        provenance="mirror",
        archive_sha256=archive_sha256,
        archive_size=archive_size,
        archive_path=archive_path,
        archive_is_local=False,
        source={
            "kind": "affiliations-mirror",
            "prefix": AFFILIATIONS_PREFIX_NAME,
            "raw_prefix": raw_prefix,
            "pipeline_marker_completed_at": pipeline.get("completed_at"),
        },
    )
