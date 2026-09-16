import hashlib
import json

import pytest

from citations_pipeline import paths
from citations_pipeline.full_state import (
    FULL_EXTRACTION_KIND,
    FileMeta,
    FullExtractionMarker,
    ResourceSettings,
    RunIdentity,
)
from citations_pipeline.state import (
    ExtractionMarker,
    FstMarker,
    PipelineMarker,
    StateError,
    TransferMarker,
)


def test_resources_reject_non_power_of_two_partitions():
    with pytest.raises(ValueError, match="power of two"):
        ResourceSettings(partitions=3).validate()


def test_identity_changes_when_registry_changes():
    from dataclasses import replace
    identity = RunIdentity("a"*64, "2026-07", "b"*64, "c"*64, "d"*64,
                           "datacite", 2, 1, 2, "fnv1a64-v1", 256, 2, 1)
    assert identity.fingerprint() != replace(identity, bundle_manifest_sha256="e"*64).fingerprint()


def test_full_mode_constants_are_distinct_from_legacy() -> None:
    assert paths.DATACITE_PREFIX_NAME == "crossref-datacite-citations-latest"
    assert paths.DATACITE_DATASET_REPO == "cometadata/crossref-datacite-citations"
    assert paths.DATACITE_INDEX_VERSION == 2
    assert paths.NORMALIZATION_VERSION == 1
    assert paths.DATACITE_EXTRACTION_VERSION == 2
    assert paths.DATACITE_MARKER_VERSION == 2
    assert paths.DATACITE_PUBLISH_SCHEMA_VERSION == 2
    assert paths.ARXIV_PUBLISH_SCHEMA_VERSION == paths.PUBLISH_SCHEMA_VERSION == 1
    assert paths.PARTITION_ALGORITHM == "fnv1a64-v1"
    # Legacy constants keep their values.
    assert paths.BUCKET_PREFIX_NAME == "crossref-arxiv-citations-latest"
    assert paths.DATASET_REPO == "cometadata/crossref-arxiv-citations"
    assert (
        paths.FST_SCHEMA_VERSION,
        paths.EXTRACTION_SCHEMA_VERSION,
        paths.MARKER_SCHEMA_VERSION,
        paths.MIRROR_MARKER_SCHEMA_VERSION,
    ) == (1, 1, 1, 2)


# --- ResourceSettings ---------------------------------------------------------


def test_resource_defaults_match_the_resource_table() -> None:
    settings = ResourceSettings()
    assert settings == ResourceSettings(
        partitions=256,
        writer_memory_mb=4096,
        result_batch_mb=16,
        queue_batches=8,
        aggregation_memory_mb=16384,
        merge_fan_in=32,
        sort_workers=4,
        lookup_memory_mb=64,
        max_entry_mb=512,
        index_sort_memory_mb=4096,
        duckdb_memory_mb=32768,
    )
    settings.validate()
    # The largest MiB value whose byte count still fits in an i64 is accepted.
    ResourceSettings(aggregation_memory_mb=2**43 - 1).validate()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"writer_memory_mb": 0}, "writer_memory_mb must be positive"),
        ({"queue_batches": -1}, "queue_batches must be positive"),
        ({"sort_workers": True}, "sort_workers must be an integer"),
        ({"lookup_memory_mb": "64"}, "lookup_memory_mb must be an integer"),
        ({"partitions": 6}, "partitions must be a positive power of two"),
        ({"merge_fan_in": 1}, "merge_fan_in must be at least 2"),
        ({"max_entry_mb": 2**43}, "max_entry_mb exceeds"),
        ({"duckdb_memory_mb": 2**63}, "duckdb_memory_mb exceeds"),
        ({"index_sort_memory_mb": 2**50}, "index_sort_memory_mb exceeds"),
        (
            {"aggregation_memory_mb": 1024},
            "aggregation_memory_mb must be at least writer_memory_mb",
        ),
    ],
)
def test_resources_reject_invalid_values(overrides: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ResourceSettings(**overrides).validate()


def test_rust_args_forwards_exactly_the_extraction_flags() -> None:
    settings = ResourceSettings(
        partitions=512,
        writer_memory_mb=2048,
        result_batch_mb=32,
        queue_batches=4,
        aggregation_memory_mb=8192,
        merge_fan_in=16,
        sort_workers=2,
        lookup_memory_mb=128,
        max_entry_mb=256,
        index_sort_memory_mb=1234,
        duckdb_memory_mb=5678,
    )
    assert settings.rust_args() == [
        "--partitions",
        "512",
        "--writer-memory-mb",
        "2048",
        "--result-batch-mb",
        "32",
        "--queue-batches",
        "4",
        "--aggregation-memory-mb",
        "8192",
        "--merge-fan-in",
        "16",
        "--sort-workers",
        "2",
        "--lookup-memory-mb",
        "128",
        "--max-entry-mb",
        "256",
    ]


def test_rust_args_validates_first() -> None:
    with pytest.raises(ValueError, match="power of two"):
        ResourceSettings(partitions=6).rust_args()


# --- RunIdentity --------------------------------------------------------------


def _identity_payload() -> dict:
    return {
        "archive_sha256": "a" * 64,
        "datacite_release": "2026-07",
        "source_inventory_sha256": "b" * 64,
        "bundle_manifest_sha256": "c" * 64,
        "source_tree_sha256": "d" * 64,
        "extraction_mode": "datacite",
        "index_version": 2,
        "normalization_version": 1,
        "extraction_version": 2,
        "partition_algorithm": "fnv1a64-v1",
        "partitions": 256,
        "datacite_publish_version": 2,
        "arxiv_publish_version": 1,
    }


def test_identity_round_trip() -> None:
    identity = RunIdentity.from_dict(_identity_payload())
    assert identity.to_dict() == _identity_payload()
    assert RunIdentity.from_dict(identity.to_dict()) == identity
    assert identity == RunIdentity(**_identity_payload())


def test_identity_fingerprint_is_the_rust_canonical_digest() -> None:
    payload = _identity_payload()
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    assert RunIdentity.from_dict(payload).fingerprint() == hashlib.sha256(
        canonical
    ).hexdigest()


@pytest.mark.parametrize(
    "mutation",
    [
        {"extraction_mode": "arxiv"},
        {"extraction_mode": "legacy"},
        {"index_version": 1},  # legacy FST_SCHEMA_VERSION
        {"index_version": "2"},
        {"normalization_version": 0},
        {"extraction_version": 1},  # legacy EXTRACTION_SCHEMA_VERSION
        {"datacite_publish_version": 1},  # legacy PUBLISH_SCHEMA_VERSION
        {"arxiv_publish_version": 2},
        {"partition_algorithm": "fnv1a64-v0"},
        {"partitions": 0},
        {"partitions": 3},
        {"partitions": True},
        {"partitions": "256"},
        {"datacite_release": "2026-7"},
        {"archive_sha256": "A" * 64},
        {"bundle_manifest_sha256": "c" * 63},
        {"source_tree_sha256": ""},
    ],
)
def test_identity_rejects_legacy_or_malformed_values(mutation: dict) -> None:
    with pytest.raises(StateError):
        RunIdentity.from_dict({**_identity_payload(), **mutation})


def test_identity_requires_exact_keys() -> None:
    payload = _identity_payload()
    payload["fst_sha256"] = "e" * 64
    with pytest.raises(StateError, match="extra"):
        RunIdentity.from_dict(payload)
    del payload["fst_sha256"]
    del payload["source_tree_sha256"]
    with pytest.raises(StateError, match="missing"):
        RunIdentity.from_dict(payload)
    with pytest.raises(StateError, match="object"):
        RunIdentity.from_dict(["not", "an", "object"])  # type: ignore[arg-type]


# --- FullExtractionMarker -----------------------------------------------------


def _marker_payload() -> dict:
    return {
        "kind": "datacite-extraction",
        "schema_version": 2,
        "identity": _identity_payload(),
        "snapshot_id": "2026-07",
        "snapshot_provenance": "mirror",
        "derived_prefix": "derived-generations/2026-07-" + "0" * 32,
        "files": {
            "outputs/all.json_valid.jsonl": {"size": 10, "sha256": "e" * 64},
            "outputs/all.json_failed.jsonl": {
                "size": 0,
                "sha256": hashlib.sha256(b"").hexdigest(),
            },
        },
        "counts": {"datacite": {"all": {"works": 1}}, "ambiguous_urls": 0},
        "completed_at": "2026-09-15T00:00:00+00:00",
    }


def test_full_marker_round_trip() -> None:
    marker = FullExtractionMarker.from_dict(_marker_payload())
    assert marker.kind == FULL_EXTRACTION_KIND == "datacite-extraction"
    assert marker.schema_version == paths.DATACITE_MARKER_VERSION
    assert marker.identity == RunIdentity.from_dict(_identity_payload())
    assert marker.files["outputs/all.json_failed.jsonl"] == FileMeta(
        size=0, sha256=hashlib.sha256(b"").hexdigest()
    )
    assert marker.to_dict() == _marker_payload()
    assert FullExtractionMarker.from_dict(marker.to_dict()) == marker


def test_full_marker_allows_zero_byte_diagnostics_and_checks_required_files() -> None:
    marker = FullExtractionMarker.from_dict(_marker_payload())
    marker.require_files(["outputs/all.json_valid.jsonl", "outputs/all.json_failed.jsonl"])
    with pytest.raises(StateError, match="report.json"):
        marker.require_files(["outputs/all.json_valid.jsonl", "report.json"])


@pytest.mark.parametrize(
    "mutation",
    [
        {"kind": "extraction"},
        {"kind": "arxiv-extraction"},
        {"kind": "datacite-index"},
        {"kind": "datacite-pipeline"},
        {"kind": "datacite-bundle-cache"},
        {"kind": ""},
        {"schema_version": 1},  # legacy MARKER_SCHEMA_VERSION
        {"schema_version": 3},
        {"schema_version": "2"},
        {"snapshot_id": "2026-7"},
        {"snapshot_provenance": "guess"},
        {"derived_prefix": "raw-generations/2026-07-" + "0" * 32},
        {"files": {"outputs/x.jsonl": {"size": -1, "sha256": "e" * 64}}},
        {"files": {"outputs/x.jsonl": {"size": True, "sha256": "e" * 64}}},
        {"files": {"outputs/x.jsonl": {"size": 1, "sha256": "E" * 64}}},
        {"files": {"outputs/x.jsonl": {"size": 1}}},
        {"files": {"": {"size": 1, "sha256": "e" * 64}}},
        {"files": []},
        {"identity": {**_identity_payload(), "extraction_mode": "arxiv"}},
        {"identity": "not-an-object"},
        {"counts": []},
        {"completed_at": ""},
    ],
)
def test_full_marker_rejects_legacy_kinds_versions_and_malformed_fields(
    mutation: dict,
) -> None:
    with pytest.raises(StateError):
        FullExtractionMarker.from_dict({**_marker_payload(), **mutation})


def test_full_marker_requires_exact_keys() -> None:
    payload = _marker_payload()
    payload["fst_sha256"] = "b" * 64
    with pytest.raises(StateError, match="extra"):
        FullExtractionMarker.from_dict(payload)
    del payload["fst_sha256"]
    del payload["identity"]
    with pytest.raises(StateError, match="missing"):
        FullExtractionMarker.from_dict(payload)


def _legacy_payloads() -> dict[str, tuple[type, dict]]:
    generation = "2026-07-" + "0" * 32
    return {
        "transfer": (
            TransferMarker,
            {
                "schema_version": 1,
                "download_protocol_version": 1,
                "snapshot": {"id": "2026-07"},
                "snapshot_id": "2026-07",
                "archive_sha256": "a" * 64,
                "archive_size": 10,
                "raw_prefix": f"raw-generations/{generation}",
                "source_tree_sha256": "c" * 64,
                "completed_at": "2026-08-31T00:00:00+00:00",
            },
        ),
        "fst": (
            FstMarker,
            {
                "schema_version": 1,
                "fst_schema_version": 1,
                "datacite_release": "2026-07",
                "fst_sha256": "b" * 64,
                "fst_size": 10,
                "extractor_version": "2.1.0",
                "completed_at": "2026-08-31T00:00:00+00:00",
            },
        ),
        "extraction": (
            ExtractionMarker,
            {
                "schema_version": 1,
                "extraction_schema_version": 1,
                "snapshot_id": "2026-07",
                "snapshot_provenance": "mirror",
                "archive_sha256": "a" * 64,
                "datacite_release": "2026-07",
                "fst_sha256": "b" * 64,
                "source_tree_sha256": "c" * 64,
                "extractor_version": "2.1.0",
                "derived_prefix": f"derived-generations/{generation}",
                "files": {"data/all.parquet": {"size": 10, "sha256": "d" * 64}},
                "counts": {"all": {"works": 1}},
                "completed_at": "2026-08-31T00:00:00+00:00",
            },
        ),
        "pipeline": (
            PipelineMarker,
            {
                "schema_version": 1,
                "snapshot_id": "2026-07",
                "snapshot_provenance": "mirror",
                "archive_sha256": "a" * 64,
                "datacite_release": "2026-07",
                "fst_sha256": "b" * 64,
                "source_tree_sha256": "c" * 64,
                "derived_prefix": f"derived-generations/{generation}",
                "raw_prefix": None,
                "dataset_repo": "cometadata/crossref-arxiv-citations",
                "dataset_commit": "0" * 40,
                "dataset_commit_url": "https://huggingface.co/x",
                "completed_at": "2026-08-31T00:00:00+00:00",
            },
        ),
    }


@pytest.mark.parametrize("name", sorted(_legacy_payloads()))
def test_every_legacy_marker_still_parses_but_never_satisfies_full_mode(
    name: str,
) -> None:
    legacy_type, payload = _legacy_payloads()[name]
    legacy_type.from_dict(payload)  # legacy classes stay operational
    with pytest.raises(StateError):
        FullExtractionMarker.from_dict(payload)
    with pytest.raises(StateError):
        RunIdentity.from_dict(payload)
    # A legacy marker relabelled with the full-mode kind and version is still
    # rejected: the full marker's key set is different.
    with pytest.raises(StateError):
        FullExtractionMarker.from_dict(
            {**payload, "kind": "datacite-extraction", "schema_version": 2}
        )
