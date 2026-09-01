from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable

import requests

from .bucket import BucketStore
from .mirror import ResolvedSnapshot
from .paths import DOWNLOAD_PROTOCOL_VERSION, MARKER_SCHEMA_VERSION, PipelinePaths
from .snapshot import (
    download_snapshot,
    preflight_crossref_access,
    resolve_snapshot,
)
from .state import StateError, TransferMarker, read_json, utc_now


class TransferError(RuntimeError):
    pass


@dataclass(frozen=True)
class TransferServices:
    session_factory: Callable[[], Any]
    preflight: Callable[..., Any]
    resolve: Callable[..., Any]
    download: Callable[..., Any]


def default_transfer_services() -> TransferServices:
    return TransferServices(
        session_factory=requests.Session,
        preflight=preflight_crossref_access,
        resolve=resolve_snapshot,
        download=download_snapshot,
    )


def _reusable_own_transfer(
    paths: PipelinePaths, identity: Any
) -> ResolvedSnapshot | None:
    if not paths.transfer_marker.is_file():
        return None
    try:
        marker = TransferMarker.from_dict(read_json(paths.transfer_marker))
    except (StateError, ValueError, OSError):
        return None
    if marker.snapshot_id != identity.snapshot_id:
        return None
    if marker.archive_size != identity.content_length:
        return None
    generation = paths.raw_generation(marker.raw_prefix)
    if not generation.archive.is_file():
        return None
    if generation.archive.stat().st_size != marker.archive_size:
        return None
    return ResolvedSnapshot(
        snapshot_id=marker.snapshot_id,
        provenance="download",
        archive_sha256=marker.archive_sha256,
        archive_size=marker.archive_size,
        archive_path=generation.archive,
        archive_is_local=False,
        source={
            "kind": "own-transfer",
            "raw_prefix": marker.raw_prefix,
            "reused": True,
        },
    )


def fallback_download_snapshot(
    *,
    paths: PipelinePaths,
    store: BucketStore,
    credential: str,
    source_tree_sha256: str,
    events: Any,
    services: TransferServices | None = None,
    run_id: str | None = None,
) -> ResolvedSnapshot:
    services = services or default_transfer_services()
    run_id = run_id or uuid.uuid4().hex
    session = services.session_factory()

    with events.phase("crossref_preflight"):
        services.preflight(session, credential)
    with events.phase("resolve_crossref_snapshot"):
        identity = services.resolve(session, credential)

    reusable = _reusable_own_transfer(paths, identity)
    if reusable is not None:
        events.emit("transfer_reused", raw_prefix=reusable.source["raw_prefix"])
        return reusable

    staging = paths.download_staging
    staging.mkdir(parents=True, exist_ok=True)
    part_path = staging / "all.json.tar.gz.part"
    resume_metadata = staging / "resume.json"

    def report(payload: dict[str, object]) -> None:
        events.emit("progress", **payload)

    with events.phase("download_snapshot"):
        result = services.download(
            session,
            identity,
            credential,
            part_path,
            resume_metadata,
            progress=report,
        )

    generation = paths.new_raw_generation(identity.snapshot_id, run_id)
    marker = TransferMarker(
        schema_version=MARKER_SCHEMA_VERSION,
        download_protocol_version=DOWNLOAD_PROTOCOL_VERSION,
        snapshot=identity.to_dict(),
        snapshot_id=identity.snapshot_id,
        archive_sha256=result.sha256,
        archive_size=result.size,
        raw_prefix=generation.relative_prefix,
        source_tree_sha256=source_tree_sha256,
        completed_at=utc_now(),
    )
    paths.snapshot_local.parent.mkdir(parents=True, exist_ok=True)
    result.path.replace(paths.snapshot_local)
    with events.phase("persist_raw_generation"):
        store.put_file(paths.snapshot_local, generation.archive, progress=report)
        store.put_json(marker.to_dict(), generation.transfer_marker)
        store.put_json(marker.to_dict(), paths.transfer_marker)

    return ResolvedSnapshot(
        snapshot_id=identity.snapshot_id,
        provenance="download",
        archive_sha256=result.sha256,
        archive_size=result.size,
        archive_path=paths.snapshot_local,
        archive_is_local=True,
        source={
            "kind": "own-transfer",
            "raw_prefix": generation.relative_prefix,
            "reused": False,
        },
    )
