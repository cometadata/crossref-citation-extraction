"""Full-mode (DataCite) run identity, resource settings, and extraction marker.

These types are deliberately separate from the legacy arXiv markers in
`state.py`: a full-mode identity carries the DataCite release and bundle
digests, the partitioning scheme, and distinct version numbers, so no legacy
cache or checkpoint can ever satisfy it. Markers contain digests and
identifiers only; never credentials or signed URLs.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, fields
from typing import Any

from .paths import (
    ARXIV_PUBLISH_SCHEMA_VERSION,
    DATACITE_DATASET_REPO,
    DATACITE_EXTRACTION_VERSION,
    DATACITE_INDEX_VERSION,
    DATACITE_MARKER_VERSION,
    DATACITE_PUBLISH_SCHEMA_VERSION,
    DATASET_REPO,
    NORMALIZATION_VERSION,
    PARTITION_ALGORITHM,
)
from .state import (
    StateError,
    _commit,
    _generation,
    _mapping,
    _provenance,
    _release,
    _require_keys,
    _sha256,
    _string,
    _version,
)

FULL_EXTRACTION_KIND = "datacite-extraction"
FULL_EXTRACTION_MODE = "datacite"
FULL_PIPELINE_KIND = "datacite-pipeline"
# The two destinations of a paired publication, in publication order, and the
# repository each one owns. A pipeline marker must carry exactly these.
PUBLICATION_KINDS = ("datacite", "arxiv")
PUBLICATION_REPOS = {"datacite": DATACITE_DATASET_REPO, "arxiv": DATASET_REPO}
# Per-generation receipt journal: `{"datacite": receipt, "arxiv": receipt}`,
# any subset, one entry appended after each verified publication.
PUBLICATIONS_JOURNAL_NAME = "_PUBLICATIONS.json"

_MIB = 1024 * 1024
_MAX_BYTES = 2**63 - 1
# Fields expressed in MiB; their byte conversion must fit a signed 64-bit int.
_MEBIBYTE_FIELDS = (
    "writer_memory_mb",
    "result_batch_mb",
    "aggregation_memory_mb",
    "lookup_memory_mb",
    "max_entry_mb",
    "index_sort_memory_mb",
    "duckdb_memory_mb",
)
# The extraction flags forwarded to `pipeline --datacite-index`, in order.
# `index_sort_memory_mb` goes only to `build-datacite-index --sort-memory-mb`
# and `duckdb_memory_mb` stays in the Python transform.
_EXTRACTION_FLAGS = (
    ("partitions", "--partitions"),
    ("writer_memory_mb", "--writer-memory-mb"),
    ("result_batch_mb", "--result-batch-mb"),
    ("queue_batches", "--queue-batches"),
    ("aggregation_memory_mb", "--aggregation-memory-mb"),
    ("merge_fan_in", "--merge-fan-in"),
    ("sort_workers", "--sort-workers"),
    ("lookup_memory_mb", "--lookup-memory-mb"),
    ("max_entry_mb", "--max-entry-mb"),
)


def _is_power_of_two(value: int) -> bool:
    return value > 0 and value & (value - 1) == 0


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class ResourceSettings:
    partitions: int = 256
    writer_memory_mb: int = 4096
    result_batch_mb: int = 16
    queue_batches: int = 8
    aggregation_memory_mb: int = 16384
    merge_fan_in: int = 32
    sort_workers: int = 4
    lookup_memory_mb: int = 64
    max_entry_mb: int = 512
    index_sort_memory_mb: int = 4096
    duckdb_memory_mb: int = 32768

    def validate(self) -> None:
        for name in (item.name for item in fields(self)):
            value = getattr(self, name)
            if not _is_int(value):
                raise ValueError(f"{name} must be an integer")
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if not _is_power_of_two(self.partitions):
            raise ValueError("partitions must be a positive power of two")
        if self.merge_fan_in < 2:
            raise ValueError("merge_fan_in must be at least 2")
        for name in _MEBIBYTE_FIELDS:
            if getattr(self, name) * _MIB > _MAX_BYTES:
                raise ValueError(f"{name} exceeds the largest representable byte count")
        # The Rust pipeline rejects this combination before touching the
        # filesystem; fail here so it never costs a staged archive.
        if self.aggregation_memory_mb < self.writer_memory_mb:
            raise ValueError("aggregation_memory_mb must be at least writer_memory_mb")

    def rust_args(self) -> list[str]:
        self.validate()
        arguments: list[str] = []
        for name, flag in _EXTRACTION_FLAGS:
            arguments.extend((flag, str(getattr(self, name))))
        return arguments


@dataclass(frozen=True)
class RunIdentity:
    archive_sha256: str
    datacite_release: str
    source_inventory_sha256: str
    bundle_manifest_sha256: str
    source_tree_sha256: str
    extraction_mode: str
    index_version: int
    normalization_version: int
    extraction_version: int
    partition_algorithm: str
    partitions: int
    datacite_publish_version: int
    arxiv_publish_version: int

    def fingerprint(self) -> str:
        """SHA-256 of the canonical JSON form; equals the Rust `identity_sha256`."""
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Any) -> RunIdentity:
        _require_keys(payload, {item.name for item in fields(cls)}, "run identity")
        mode = _string(payload["extraction_mode"], "extraction_mode")
        if mode != FULL_EXTRACTION_MODE:
            raise StateError(f"extraction_mode must be {FULL_EXTRACTION_MODE!r}")
        algorithm = _string(payload["partition_algorithm"], "partition_algorithm")
        if algorithm != PARTITION_ALGORITHM:
            raise StateError(f"partition_algorithm must be {PARTITION_ALGORITHM!r}")
        partitions = payload["partitions"]
        if not _is_int(partitions) or not _is_power_of_two(partitions):
            raise StateError("partitions must be a positive power of two")
        return cls(
            archive_sha256=_sha256(payload["archive_sha256"], "archive_sha256"),
            datacite_release=_release(payload["datacite_release"], "datacite_release"),
            source_inventory_sha256=_sha256(
                payload["source_inventory_sha256"], "source_inventory_sha256"
            ),
            bundle_manifest_sha256=_sha256(
                payload["bundle_manifest_sha256"], "bundle_manifest_sha256"
            ),
            source_tree_sha256=_sha256(
                payload["source_tree_sha256"], "source_tree_sha256"
            ),
            extraction_mode=mode,
            index_version=_version(
                payload["index_version"], DATACITE_INDEX_VERSION, "index_version"
            ),
            normalization_version=_version(
                payload["normalization_version"],
                NORMALIZATION_VERSION,
                "normalization_version",
            ),
            extraction_version=_version(
                payload["extraction_version"],
                DATACITE_EXTRACTION_VERSION,
                "extraction_version",
            ),
            partition_algorithm=algorithm,
            partitions=partitions,
            datacite_publish_version=_version(
                payload["datacite_publish_version"],
                DATACITE_PUBLISH_SCHEMA_VERSION,
                "datacite_publish_version",
            ),
            arxiv_publish_version=_version(
                payload["arxiv_publish_version"],
                ARXIV_PUBLISH_SCHEMA_VERSION,
                "arxiv_publish_version",
            ),
        )


@dataclass(frozen=True)
class FileMeta:
    size: int
    sha256: str


def _file_meta(value: Any, name: str) -> FileMeta:
    _require_keys(value, {"size", "sha256"}, name)
    size = value["size"]
    # Zero-byte diagnostic JSONL (failed, ambiguous) is valid output.
    if not _is_int(size) or size < 0:
        raise StateError(f"{name}.size must be a non-negative integer")
    return FileMeta(size=size, sha256=_sha256(value["sha256"], f"{name}.sha256"))


def _file_metas(value: Any, name: str) -> dict[str, FileMeta]:
    entries = _mapping(value, name)
    return {
        _string(key, f"{name} key"): _file_meta(entry, f"{name}[{key}]")
        for key, entry in entries.items()
    }


@dataclass(frozen=True)
class FullExtractionMarker:
    kind: str
    schema_version: int
    identity: RunIdentity
    snapshot_id: str
    snapshot_provenance: str
    derived_prefix: str
    files: dict[str, FileMeta]
    counts: dict[str, Any]
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "identity": self.identity.to_dict(),
            "snapshot_id": self.snapshot_id,
            "snapshot_provenance": self.snapshot_provenance,
            "derived_prefix": self.derived_prefix,
            "files": {name: asdict(meta) for name, meta in self.files.items()},
            "counts": dict(self.counts),
            "completed_at": self.completed_at,
        }

    def require_files(self, names: Iterable[str]) -> None:
        """Fail unless every required output is listed (any size, including 0)."""
        missing = sorted(set(names) - self.files.keys())
        if missing:
            raise StateError(f"extraction marker is missing required files: {missing}")

    @classmethod
    def from_dict(cls, payload: Any) -> FullExtractionMarker:
        _require_keys(
            payload,
            {
                "kind",
                "schema_version",
                "identity",
                "snapshot_id",
                "snapshot_provenance",
                "derived_prefix",
                "files",
                "counts",
                "completed_at",
            },
            "extraction marker",
        )
        kind = _string(payload["kind"], "kind")
        if kind != FULL_EXTRACTION_KIND:
            raise StateError(f"kind must be {FULL_EXTRACTION_KIND!r}")
        return cls(
            kind=kind,
            schema_version=_version(
                payload["schema_version"], DATACITE_MARKER_VERSION, "schema_version"
            ),
            identity=RunIdentity.from_dict(payload["identity"]),
            snapshot_id=_release(payload["snapshot_id"], "snapshot_id"),
            snapshot_provenance=_provenance(
                payload["snapshot_provenance"], "snapshot_provenance"
            ),
            derived_prefix=_generation(
                payload["derived_prefix"], "derived-generations", "derived_prefix"
            ),
            files=_file_metas(payload["files"], "files"),
            counts=dict(_mapping(payload["counts"], "counts")),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


@dataclass(frozen=True)
class PublicationReceipt:
    """A verified dataset commit: the repository, the commit that was verified,
    its URL, and the digest of the artifact manifest that commit publishes."""

    repo_id: str
    commit_sha: str
    commit_url: str
    artifact_digest: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Any) -> PublicationReceipt:
        _require_keys(
            payload,
            {"repo_id", "commit_sha", "commit_url", "artifact_digest"},
            "publication receipt",
        )
        return cls(
            repo_id=_string(payload["repo_id"], "repo_id"),
            commit_sha=_commit(payload["commit_sha"], "commit_sha"),
            commit_url=_string(payload["commit_url"], "commit_url"),
            artifact_digest=_sha256(payload["artifact_digest"], "artifact_digest"),
        )


def _publications(value: Any, name: str) -> dict[str, PublicationReceipt]:
    _require_keys(value, set(PUBLICATION_KINDS), name)
    publications: dict[str, PublicationReceipt] = {}
    for kind in PUBLICATION_KINDS:
        receipt = PublicationReceipt.from_dict(value[kind])
        if receipt.repo_id != PUBLICATION_REPOS[kind]:
            raise StateError(
                f"{name}[{kind}].repo_id must be {PUBLICATION_REPOS[kind]!r}"
            )
        publications[kind] = receipt
    return publications


@dataclass(frozen=True)
class FullPipelineMarker:
    """`_PIPELINE_COMPLETE.json` for a full run: written only after both
    publications were verified, and the authority for the paired publication."""

    kind: str
    schema_version: int
    identity: RunIdentity
    snapshot_id: str
    derived_prefix: str
    publications: dict[str, PublicationReceipt]
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "identity": self.identity.to_dict(),
            "snapshot_id": self.snapshot_id,
            "derived_prefix": self.derived_prefix,
            "publications": {
                kind: receipt.to_dict() for kind, receipt in self.publications.items()
            },
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, payload: Any) -> FullPipelineMarker:
        _require_keys(
            payload,
            {
                "kind",
                "schema_version",
                "identity",
                "snapshot_id",
                "derived_prefix",
                "publications",
                "completed_at",
            },
            "pipeline marker",
        )
        kind = _string(payload["kind"], "kind")
        if kind != FULL_PIPELINE_KIND:
            raise StateError(f"kind must be {FULL_PIPELINE_KIND!r}")
        return cls(
            kind=kind,
            schema_version=_version(
                payload["schema_version"], DATACITE_MARKER_VERSION, "schema_version"
            ),
            identity=RunIdentity.from_dict(payload["identity"]),
            snapshot_id=_release(payload["snapshot_id"], "snapshot_id"),
            derived_prefix=_generation(
                payload["derived_prefix"], "derived-generations", "derived_prefix"
            ),
            publications=_publications(payload["publications"], "publications"),
            completed_at=_string(payload["completed_at"], "completed_at"),
        )


def publications_to_dict(
    receipts: dict[str, PublicationReceipt],
) -> dict[str, Any]:
    """Serialize a (possibly partial) receipt journal in publication order."""
    unknown = sorted(receipts.keys() - set(PUBLICATION_KINDS))
    if unknown:
        raise StateError(f"publication journal has unknown kinds: {unknown}")
    return {
        kind: receipts[kind].to_dict() for kind in PUBLICATION_KINDS if kind in receipts
    }


def publications_from_dict(payload: Any) -> dict[str, PublicationReceipt]:
    """Parse a `_PUBLICATIONS.json` journal: any subset of the publication
    kinds, each receipt naming the repository that kind owns."""
    entries = _mapping(payload, "publication journal")
    unknown = sorted(entries.keys() - set(PUBLICATION_KINDS))
    if unknown:
        raise StateError(f"publication journal has unknown kinds: {unknown}")
    receipts: dict[str, PublicationReceipt] = {}
    for kind in PUBLICATION_KINDS:
        if kind not in entries:
            continue
        receipt = PublicationReceipt.from_dict(entries[kind])
        if receipt.repo_id != PUBLICATION_REPOS[kind]:
            raise StateError(
                f"publication journal[{kind}].repo_id must be "
                f"{PUBLICATION_REPOS[kind]!r}"
            )
        receipts[kind] = receipt
    return receipts
