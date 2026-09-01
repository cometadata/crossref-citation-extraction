import json
from pathlib import Path

import pytest

from citations_pipeline.mirror import MirrorError, resolve_mirror_snapshot

RUN = "0472389f9cbc4f8a9eb4d1ba8278b955"


def make_mirror(
    root: Path,
    *,
    schema_version: int = 2,
    pipeline_sha: str = "a" * 64,
    transfer_sha: str = "a" * 64,
    archive_bytes: bytes = b"tarball",
    archive_size: int | None = None,
    write_pipeline_marker: bool = True,
    write_transfer_marker: bool = True,
    write_archive: bool = True,
) -> Path:
    prefix = root / "crossref-affiliations-latest"
    raw_prefix = f"raw-generations/2026-07-{RUN}"
    generation = prefix / raw_prefix
    generation.mkdir(parents=True)
    if write_pipeline_marker:
        (prefix / "_PIPELINE_COMPLETE.json").write_text(
            json.dumps(
                {
                    "schema_version": schema_version,
                    "snapshot_id": "2026-07",
                    "raw_prefix": raw_prefix,
                    "archive_sha256": pipeline_sha,
                    "completed_at": "2026-08-27T03:08:48+00:00",
                }
            )
        )
    if write_transfer_marker:
        (generation / "_TRANSFER_COMPLETE.json").write_text(
            json.dumps(
                {
                    "archive_sha256": transfer_sha,
                    "archive_size": (
                        archive_size if archive_size is not None else len(archive_bytes)
                    ),
                }
            )
        )
    if write_archive:
        (generation / "all.json.tar.gz").write_bytes(archive_bytes)
    return root


def test_resolves_valid_mirror(tmp_path: Path) -> None:
    make_mirror(tmp_path)
    resolved = resolve_mirror_snapshot(tmp_path)
    assert resolved.snapshot_id == "2026-07"
    assert resolved.provenance == "mirror"
    assert resolved.archive_sha256 == "a" * 64
    assert resolved.archive_size == len(b"tarball")
    assert resolved.archive_path.name == "all.json.tar.gz"
    assert resolved.archive_is_local is False
    assert resolved.source["raw_prefix"] == f"raw-generations/2026-07-{RUN}"


def test_missing_pipeline_marker(tmp_path: Path) -> None:
    make_mirror(tmp_path, write_pipeline_marker=False)
    with pytest.raises(MirrorError, match="pipeline marker"):
        resolve_mirror_snapshot(tmp_path)


def test_wrong_schema_version(tmp_path: Path) -> None:
    make_mirror(tmp_path, schema_version=3)
    with pytest.raises(MirrorError, match="schema_version"):
        resolve_mirror_snapshot(tmp_path)


def test_marker_sha_disagreement(tmp_path: Path) -> None:
    make_mirror(tmp_path, transfer_sha="b" * 64)
    with pytest.raises(MirrorError, match="disagree"):
        resolve_mirror_snapshot(tmp_path)


def test_archive_missing(tmp_path: Path) -> None:
    make_mirror(tmp_path, write_archive=False)
    with pytest.raises(MirrorError, match="archive"):
        resolve_mirror_snapshot(tmp_path)


def test_archive_size_mismatch(tmp_path: Path) -> None:
    make_mirror(tmp_path, archive_size=999)
    with pytest.raises(MirrorError, match="size"):
        resolve_mirror_snapshot(tmp_path)


def test_corrupt_pipeline_marker(tmp_path: Path) -> None:
    make_mirror(tmp_path)
    marker = tmp_path / "crossref-affiliations-latest" / "_PIPELINE_COMPLETE.json"
    marker.write_text("{not json")
    with pytest.raises(MirrorError, match="unreadable"):
        resolve_mirror_snapshot(tmp_path)


def test_traversal_prefix_rejected(tmp_path: Path) -> None:
    make_mirror(tmp_path)
    marker = tmp_path / "crossref-affiliations-latest" / "_PIPELINE_COMPLETE.json"
    payload = json.loads(marker.read_text())
    payload["raw_prefix"] = "raw-generations/../../etc"
    marker.write_text(json.dumps(payload))
    with pytest.raises(MirrorError, match="raw_prefix"):
        resolve_mirror_snapshot(tmp_path)
