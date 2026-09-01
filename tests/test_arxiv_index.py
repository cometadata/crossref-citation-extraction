import hashlib
import json
from pathlib import Path

import pytest

from citations_pipeline.arxiv_index import (
    ArxivIndexError,
    DataciteRelease,
    discover_datacite_release,
    ensure_fst,
)
from citations_pipeline.paths import PipelinePaths
from citations_pipeline.state import utc_now
from citations_pipeline.telemetry import EventLogger


def make_datacite(root: Path, release: str, complete: bool = True) -> None:
    prefix = root / f"datacite-monthly-data-file-{release}"
    (prefix / "dois" / f"updated_{release}").mkdir(parents=True)
    if complete:
        (prefix / "_TRANSFER_COMPLETE.json").write_text("{}")


def test_discovery_picks_latest_complete(tmp_path: Path) -> None:
    make_datacite(tmp_path, "2026-06")
    make_datacite(tmp_path, "2026-07")
    make_datacite(tmp_path, "2026-08", complete=False)
    found = discover_datacite_release(tmp_path)
    assert found.release == "2026-07"
    assert found.dois_path == (
        tmp_path / "datacite-monthly-data-file-2026-07" / "dois"
    )


def test_discovery_pinned(tmp_path: Path) -> None:
    make_datacite(tmp_path, "2026-06")
    make_datacite(tmp_path, "2026-07")
    assert discover_datacite_release(tmp_path, pinned="2026-06").release == "2026-06"
    with pytest.raises(ArxivIndexError, match="2026-05"):
        discover_datacite_release(tmp_path, pinned="2026-05")


def test_discovery_empty(tmp_path: Path) -> None:
    with pytest.raises(ArxivIndexError, match="no complete"):
        discover_datacite_release(tmp_path)


class RecordingStore:
    def __init__(self) -> None:
        self.files: list[tuple[Path, Path]] = []
        self.jsons: list[tuple[dict, Path]] = []

    def put_file(self, source, destination, **kwargs) -> None:
        self.files.append((source, destination))

    def put_json(self, payload, destination) -> None:
        self.jsons.append((payload, destination))


def _setup(tmp_path: Path) -> tuple[PipelinePaths, DataciteRelease]:
    mount = tmp_path / "mnt"
    work = tmp_path / "work"
    make_datacite(mount, "2026-07")
    paths = PipelinePaths.from_roots(mount, work)
    return paths, discover_datacite_release(mount)


def _write_cache(paths: PipelinePaths, release: str, content: bytes) -> None:
    cache = paths.fst_prefix(release)
    cache.root.mkdir(parents=True)
    cache.fst.write_bytes(content)
    cache.marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "fst_schema_version": 1,
                "datacite_release": release,
                "fst_sha256": hashlib.sha256(content).hexdigest(),
                "fst_size": len(content),
                "extractor_version": "2.1.0",
                "completed_at": utc_now(),
            }
        )
    )


def test_ensure_fst_reuses_valid_cache(tmp_path: Path) -> None:
    paths, release = _setup(tmp_path)
    _write_cache(paths, "2026-07", b"fst-bytes")

    def fail_build(records: Path, output: Path) -> None:
        raise AssertionError("must not rebuild when cache is valid")

    result = ensure_fst(
        paths=paths,
        release=release,
        build_index=fail_build,
        extractor_version="2.1.0",
        store=None,
        events=EventLogger(),
    )
    assert result.reused is True
    assert result.local_path.read_bytes() == b"fst-bytes"


def test_ensure_fst_rebuilds_on_checksum_mismatch(tmp_path: Path) -> None:
    paths, release = _setup(tmp_path)
    _write_cache(paths, "2026-07", b"fst-bytes")
    cache = paths.fst_prefix("2026-07")
    cache.fst.write_bytes(b"corrupted")  # same length, different content

    def build(records: Path, output: Path) -> None:
        output.write_bytes(b"rebuilt")

    store = RecordingStore()
    result = ensure_fst(
        paths=paths,
        release=release,
        build_index=build,
        extractor_version="2.1.0",
        store=store,
        events=EventLogger(),
    )
    assert result.reused is False
    assert result.local_path.read_bytes() == b"rebuilt"
    assert store.files == [(paths.fst_local, cache.fst)]
    assert store.jsons[0][1] == cache.marker
    assert store.jsons[0][0]["fst_sha256"] == hashlib.sha256(b"rebuilt").hexdigest()


def test_ensure_fst_builds_and_persists_without_cache(tmp_path: Path) -> None:
    paths, release = _setup(tmp_path)
    built: list[Path] = []

    def build(records: Path, output: Path) -> None:
        built.append(records)
        output.write_bytes(b"fresh")

    store = RecordingStore()
    result = ensure_fst(
        paths=paths,
        release=release,
        build_index=build,
        extractor_version="2.1.0",
        store=store,
        events=EventLogger(),
    )
    assert built == [release.dois_path]
    assert result.reused is False
    assert len(store.files) == 1 and len(store.jsons) == 1


def test_ensure_fst_force_ignores_cache(tmp_path: Path) -> None:
    paths, release = _setup(tmp_path)
    _write_cache(paths, "2026-07", b"fst-bytes")

    def build(records: Path, output: Path) -> None:
        output.write_bytes(b"forced")

    result = ensure_fst(
        paths=paths,
        release=release,
        build_index=build,
        extractor_version="2.1.0",
        store=None,
        events=EventLogger(),
        force=True,
    )
    assert result.reused is False
    assert result.local_path.read_bytes() == b"forced"
