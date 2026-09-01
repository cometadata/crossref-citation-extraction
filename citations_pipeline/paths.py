from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

DOWNLOAD_PROTOCOL_VERSION = 1
EXTRACTION_SCHEMA_VERSION = 1
# Cache discipline: any change to the extractor's `build-index` semantics — i.e. the
# FST content produced for the same DataCite release — MUST bump FST_SCHEMA_VERSION.
# It is the only guard against silently reusing a stale cached index.
FST_SCHEMA_VERSION = 1
PUBLISH_SCHEMA_VERSION = 1
MARKER_SCHEMA_VERSION = 1
MIRROR_MARKER_SCHEMA_VERSION = 2

BUCKET_ID = "cometadata/shared-storage"
BUCKET_PREFIX_NAME = "crossref-arxiv-citations-latest"
AFFILIATIONS_PREFIX_NAME = "crossref-affiliations-latest"
DATASET_REPO = "cometadata/crossref-arxiv-citations"
ARCHIVE_NAME = "all.json.tar.gz"
INPUT_STEM = "all.json"
OUTPUT_KINDS = ("valid", "failed", "publisher", "crossref", "mined")
CONFIG_PROVENANCES = {
    "all": ("publisher", "crossref", "mined"),
    "asserted": ("publisher", "crossref"),
    "mined": ("mined",),
}
CONFIG_NAMES = tuple(CONFIG_PROVENANCES)
DATACITE_PREFIX_RE = re.compile(r"^datacite-monthly-data-file-(\d{4}-\d{2})$")
RUNTIME_SOURCE_PATTERNS = (
    "jobs/pipeline.py",
    "citations_pipeline/**/*.py",
    "Cargo.toml",
    "Cargo.lock",
    "src/**/*.rs",
    # Cargo.toml declares [[bench]] targets; cargo refuses to parse the
    # manifest if a declared target file is missing from the staged volume.
    "benches/**/*.rs",
)

_GENERATION_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SNAPSHOT_ID = re.compile(r"^\d{4}-\d{2}$")
_RUN_ID = re.compile(r"^[0-9a-f]{32}$")


class GenerationPathError(ValueError):
    pass


def validate_generation_prefix(value: str, expected_root: str) -> str:
    if not isinstance(value, str) or value.startswith("/"):
        raise GenerationPathError("generation prefix must be relative")
    parts = value.split("/")
    if len(parts) != 2 or parts[0] != expected_root:
        raise GenerationPathError(
            f"generation prefix must be directly under {expected_root}"
        )
    name = parts[1]
    if name in {"", ".", ".."} or not _GENERATION_NAME.fullmatch(name):
        raise GenerationPathError("generation name is invalid")
    return f"{expected_root}/{name}"


def _generation_name(snapshot_id: str, run_id: str) -> str:
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise GenerationPathError("snapshot ID must have YYYY-MM form")
    if not _RUN_ID.fullmatch(run_id):
        raise GenerationPathError("run ID must be 32 lowercase hexadecimal digits")
    return f"{snapshot_id}-{run_id}"


@dataclass(frozen=True)
class RawGenerationPaths:
    relative_prefix: str
    root: Path
    archive: Path
    metadata: Path
    transfer_marker: Path


@dataclass(frozen=True)
class DerivedGenerationPaths:
    relative_prefix: str
    root: Path
    data: Path
    outputs: Path
    report: Path
    extraction_marker: Path


@dataclass(frozen=True)
class FstPaths:
    relative_prefix: str
    root: Path
    fst: Path
    marker: Path


@dataclass(frozen=True)
class PipelinePaths:
    bucket_root: Path
    work_root: Path
    prefix: Path
    affiliations_prefix: Path
    transfer_marker: Path
    extraction_marker: Path
    pipeline_marker: Path
    snapshot_local: Path
    fst_local: Path
    outputs_dir: Path
    partitions: Path
    transform_dir: Path
    derived_local: Path
    smoke_dir: Path
    download_staging: Path

    @classmethod
    def from_roots(cls, bucket_root: Path, work_root: Path) -> PipelinePaths:
        prefix = bucket_root / BUCKET_PREFIX_NAME
        return cls(
            bucket_root=bucket_root,
            work_root=work_root,
            prefix=prefix,
            affiliations_prefix=bucket_root / AFFILIATIONS_PREFIX_NAME,
            transfer_marker=prefix / "_TRANSFER_COMPLETE.json",
            extraction_marker=prefix / "_EXTRACTION_COMPLETE.json",
            pipeline_marker=prefix / "_PIPELINE_COMPLETE.json",
            snapshot_local=work_root / "snapshot" / ARCHIVE_NAME,
            fst_local=work_root / "fst" / "arxiv.fst",
            outputs_dir=work_root / "outputs",
            partitions=work_root / "partitions",
            transform_dir=work_root / "transform",
            derived_local=work_root / "derived",
            smoke_dir=work_root / "smoke",
            download_staging=work_root / "download-staging",
        )

    def fst_prefix(self, release: str) -> FstPaths:
        if not _SNAPSHOT_ID.fullmatch(release):
            raise GenerationPathError("DataCite release must have YYYY-MM form")
        name = f"{release}-v{FST_SCHEMA_VERSION}"
        root = self.prefix / "fst" / name
        return FstPaths(
            relative_prefix=f"fst/{name}",
            root=root,
            fst=root / "arxiv.fst",
            marker=root / "_FST_COMPLETE.json",
        )

    def new_raw_generation(self, snapshot_id: str, run_id: str) -> RawGenerationPaths:
        return self.raw_generation(
            f"raw-generations/{_generation_name(snapshot_id, run_id)}"
        )

    def new_derived_generation(
        self, snapshot_id: str, run_id: str
    ) -> DerivedGenerationPaths:
        return self.derived_generation(
            f"derived-generations/{_generation_name(snapshot_id, run_id)}"
        )

    def raw_generation(self, relative_prefix: str) -> RawGenerationPaths:
        validated = validate_generation_prefix(relative_prefix, "raw-generations")
        root = self.prefix / validated
        return RawGenerationPaths(
            relative_prefix=validated,
            root=root,
            archive=root / ARCHIVE_NAME,
            metadata=root / "metadata.json",
            transfer_marker=root / "_TRANSFER_COMPLETE.json",
        )

    def derived_generation(self, relative_prefix: str) -> DerivedGenerationPaths:
        validated = validate_generation_prefix(relative_prefix, "derived-generations")
        root = self.prefix / validated
        return DerivedGenerationPaths(
            relative_prefix=validated,
            root=root,
            data=root / "data",
            outputs=root / "outputs",
            report=root / "report.json",
            extraction_marker=root / "_EXTRACTION_COMPLETE.json",
        )


def runtime_source_files(root: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                path
                for pattern in RUNTIME_SOURCE_PATTERNS
                for path in root.glob(pattern)
                if path.is_file()
            }
        )
    )


def runtime_source_digest(root: Path) -> str:
    paths = runtime_source_files(root)
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
