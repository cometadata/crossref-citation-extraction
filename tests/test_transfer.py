import json
from pathlib import Path

from citations_pipeline.paths import PipelinePaths
from citations_pipeline.snapshot import DownloadResult, SnapshotIdentity
from citations_pipeline.state import utc_now
from citations_pipeline.telemetry import EventLogger
from citations_pipeline.transfer import (
    TransferServices,
    fallback_download_snapshot,
)

RUN = "f" * 32
IDENTITY = SnapshotIdentity(
    snapshot_id="2026-07",
    alias_url="https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz",
    effective_url="https://example.org/signed",
    content_length=7,
    etag='"etag"',
    last_modified=None,
    content_disposition=None,
)


class RecordingStore:
    def __init__(self) -> None:
        self.files: list[tuple[Path, Path]] = []
        self.jsons: list[tuple[dict, Path]] = []

    def put_file(self, source, destination, **kwargs) -> None:
        self.files.append((source, destination))

    def put_json(self, payload, destination) -> None:
        self.jsons.append((payload, destination))


def make_services(downloaded: list | None = None) -> TransferServices:
    def download(session, identity, credential, part_path, resume_path, **kwargs):
        part_path.parent.mkdir(parents=True, exist_ok=True)
        part_path.write_bytes(b"tarball")
        if downloaded is not None:
            downloaded.append(part_path)
        return DownloadResult(path=part_path, size=7, sha256="d" * 64)

    return TransferServices(
        session_factory=lambda: object(),
        preflight=lambda session, credential: None,
        resolve=lambda session, credential: IDENTITY,
        download=download,
    )


def test_fresh_download_persists_generation_and_markers(tmp_path: Path) -> None:
    paths = PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    store = RecordingStore()
    resolved = fallback_download_snapshot(
        paths=paths,
        store=store,
        credential="token",
        source_tree_sha256="c" * 64,
        events=EventLogger(),
        services=make_services(),
        run_id=RUN,
    )
    assert resolved.provenance == "download"
    assert resolved.snapshot_id == "2026-07"
    assert resolved.archive_sha256 == "d" * 64
    assert resolved.archive_is_local is True
    assert resolved.archive_path == paths.snapshot_local
    assert resolved.archive_path.read_bytes() == b"tarball"

    generation = paths.raw_generation(f"raw-generations/2026-07-{RUN}")
    assert store.files == [(resolved.archive_path, generation.archive)]
    marker_destinations = [destination for _payload, destination in store.jsons]
    assert generation.transfer_marker in marker_destinations
    assert paths.transfer_marker in marker_destinations
    payload = store.jsons[0][0]
    assert payload["archive_sha256"] == "d" * 64
    assert payload["raw_prefix"] == f"raw-generations/2026-07-{RUN}"
    assert payload["snapshot"]["snapshot_id"] == "2026-07"


def test_reuses_prior_own_transfer(tmp_path: Path) -> None:
    paths = PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    generation = paths.raw_generation(f"raw-generations/2026-07-{RUN}")
    generation.root.mkdir(parents=True)
    generation.archive.write_bytes(b"tarball")
    paths.prefix.mkdir(parents=True, exist_ok=True)
    paths.transfer_marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "download_protocol_version": 1,
                "snapshot": IDENTITY.to_dict(),
                "snapshot_id": "2026-07",
                "archive_sha256": "d" * 64,
                "archive_size": 7,
                "raw_prefix": f"raw-generations/2026-07-{RUN}",
                "source_tree_sha256": "c" * 64,
                "completed_at": utc_now(),
            }
        )
    )
    downloaded: list = []
    resolved = fallback_download_snapshot(
        paths=paths,
        store=RecordingStore(),
        credential="token",
        source_tree_sha256="c" * 64,
        events=EventLogger(),
        services=make_services(downloaded),
        run_id="0" * 32,
    )
    assert downloaded == []  # no new download
    assert resolved.archive_is_local is False
    assert resolved.archive_path == generation.archive
    assert resolved.source["reused"] is True


def test_stale_marker_triggers_fresh_download(tmp_path: Path) -> None:
    paths = PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    paths.prefix.mkdir(parents=True, exist_ok=True)
    paths.transfer_marker.write_text("{not json")
    downloaded: list = []
    resolved = fallback_download_snapshot(
        paths=paths,
        store=RecordingStore(),
        credential="token",
        source_tree_sha256="c" * 64,
        events=EventLogger(),
        services=make_services(downloaded),
        run_id=RUN,
    )
    assert len(downloaded) == 1
    assert resolved.archive_is_local is True
