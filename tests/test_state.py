import pytest

from citations_pipeline.state import (
    ExtractionMarker,
    FstMarker,
    PipelineMarker,
    StateError,
    TransferMarker,
    can_reuse_extraction,
    read_json,
    utc_now,
    write_atomic_json,
)


def _extraction_payload() -> dict:
    return {
        "schema_version": 1,
        "extraction_schema_version": 1,
        "snapshot_id": "2026-07",
        "snapshot_provenance": "mirror",
        "archive_sha256": "a" * 64,
        "datacite_release": "2026-07",
        "fst_sha256": "b" * 64,
        "source_tree_sha256": "c" * 64,
        "extractor_version": "2.1.0",
        "derived_prefix": "derived-generations/2026-07-" + "0" * 32,
        "files": {"data/all.parquet": {"size": 10, "sha256": "d" * 64}},
        "counts": {"all": {"works": 1, "citations": 1, "references": 1}},
        "completed_at": "2026-08-31T00:00:00+00:00",
    }


def test_extraction_marker_round_trip() -> None:
    marker = ExtractionMarker.from_dict(_extraction_payload())
    assert ExtractionMarker.from_dict(marker.to_dict()) == marker


def test_extraction_marker_rejects_bad_prefix() -> None:
    payload = _extraction_payload()
    payload["derived_prefix"] = "raw-generations/wrong-root"
    with pytest.raises(StateError):
        ExtractionMarker.from_dict(payload)


def test_extraction_marker_rejects_wrong_version() -> None:
    payload = _extraction_payload()
    payload["extraction_schema_version"] = 99
    with pytest.raises(StateError):
        ExtractionMarker.from_dict(payload)


def test_extraction_marker_rejects_unknown_provenance() -> None:
    payload = _extraction_payload()
    payload["snapshot_provenance"] = "guess"
    with pytest.raises(StateError):
        ExtractionMarker.from_dict(payload)


def test_fst_marker_round_trip() -> None:
    marker = FstMarker.from_dict(
        {
            "schema_version": 1,
            "fst_schema_version": 1,
            "datacite_release": "2026-07",
            "fst_sha256": "a" * 64,
            "fst_size": 123,
            "extractor_version": "2.1.0",
            "completed_at": utc_now(),
        }
    )
    assert FstMarker.from_dict(marker.to_dict()) == marker


def test_transfer_marker_round_trip() -> None:
    marker = TransferMarker.from_dict(
        {
            "schema_version": 1,
            "download_protocol_version": 1,
            "snapshot": {"snapshot_id": "2026-07"},
            "snapshot_id": "2026-07",
            "archive_sha256": "a" * 64,
            "archive_size": 5,
            "raw_prefix": "raw-generations/2026-07-" + "1" * 32,
            "source_tree_sha256": "b" * 64,
            "completed_at": utc_now(),
        }
    )
    assert TransferMarker.from_dict(marker.to_dict()) == marker


def test_pipeline_marker_round_trip() -> None:
    marker = PipelineMarker.from_dict(
        {
            "schema_version": 1,
            "snapshot_id": "2026-07",
            "snapshot_provenance": "download",
            "archive_sha256": "a" * 64,
            "datacite_release": "2026-07",
            "fst_sha256": "b" * 64,
            "source_tree_sha256": "c" * 64,
            "derived_prefix": "derived-generations/2026-07-" + "0" * 32,
            "raw_prefix": "raw-generations/2026-07-" + "1" * 32,
            "dataset_repo": "cometadata/crossref-arxiv-citations",
            "dataset_commit": "e" * 40,
            "dataset_commit_url": "https://huggingface.co/datasets/x/commit/e",
            "completed_at": utc_now(),
        }
    )
    assert PipelineMarker.from_dict(marker.to_dict()) == marker
    # raw_prefix is optional (mirror provenance has none)
    payload = marker.to_dict()
    payload["raw_prefix"] = None
    payload["snapshot_provenance"] = "mirror"
    assert PipelineMarker.from_dict(payload).raw_prefix is None


def test_can_reuse_extraction() -> None:
    marker = ExtractionMarker.from_dict(_extraction_payload())
    kwargs = {
        "archive_sha256": "a" * 64,
        "datacite_release": "2026-07",
        "fst_sha256": "b" * 64,
        "source_tree_sha256": "c" * 64,
    }
    assert can_reuse_extraction(marker, **kwargs)
    assert not can_reuse_extraction(marker, **{**kwargs, "archive_sha256": "f" * 64})
    assert not can_reuse_extraction(marker, **{**kwargs, "fst_sha256": "f" * 64})
    assert not can_reuse_extraction(
        marker, **{**kwargs, "source_tree_sha256": "f" * 64}
    )
    assert not can_reuse_extraction(
        marker, **{**kwargs, "datacite_release": "2026-06"}
    )


def test_write_atomic_json_round_trip(tmp_path) -> None:
    target = tmp_path / "m.json"
    write_atomic_json(target, {"b": 2, "a": 1})
    assert read_json(target) == {"a": 1, "b": 2}
    text = target.read_text()
    assert text.index('"a"') < text.index('"b"')  # sort_keys
