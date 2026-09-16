"""Full DataCite pipeline: one complete Crossref extraction feeding the paired
publication of `cometadata/crossref-datacite-citations` and
`cometadata/crossref-arxiv-citations`.

The phases are plain functions so the smoke and benchmark modes can run
acquisition, bundle preparation, extraction and transformation without
publishing anything. `run_full_pipeline` strings them together for
`mode == "datacite"`, persists every durable artifact under
`crossref-datacite-citations-latest/`, and activates `_PIPELINE_COMPLETE.json`
only after both destinations have been verified.

Durable layout under the owned prefix (`PipelinePaths.prefix`):

    indexes/<release>-v2-<inventory>/...        verified bundles (datacite_index)
    raw-generations/<snapshot>-<run>/           fallback downloads (transfer)
    derived-generations/<snapshot>-<run>/
        datacite/README.md, datacite/metadata/pipeline-manifest.json,
        datacite/data/<config>/part-NNNNN.parquet
        arxiv/README.md, arxiv/metadata/pipeline-manifest.json,
        arxiv/data/<config>.parquet
        outputs/<kind>.jsonl.zst                six compressed raw extractor outputs
        extraction-report.json
        _EXTRACTION_COMPLETE.json               generation copy of the marker
        _PUBLICATIONS.json                      receipt journal (partial allowed)
    _EXTRACTION_COMPLETE.json                   verified reusable derived data
    _PIPELINE_COMPLETE.json                     both publications verified

Credentials reach only the Hub client, the bucket store and the publication
calls; identities, markers, journals and events carry digests and ids only.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from .bucket import BucketStore, HfBucketStore
from .datacite_index import (
    BundleResult,
    DataciteRelease,
    discover_release,
    ensure_bundle,
)
from .datacite_publish import DatasetTransform, transform_datacite_outputs
from .extractor import (
    FULL_OUTPUT_KINDS,
    REPORT_NAME,
    ExtractionReport,
    ExtractorBinary,
    FullExtractionOutputs,
    build_extractor,
    load_extraction_report,
    run_build_datacite_index,
    run_datacite_extraction,
)
from .full_state import (
    FULL_EXTRACTION_KIND,
    FULL_EXTRACTION_MODE,
    FULL_PIPELINE_KIND,
    PUBLICATION_KINDS,
    PUBLICATION_REPOS,
    PUBLICATIONS_JOURNAL_NAME,
    FileMeta,
    FullExtractionMarker,
    FullPipelineMarker,
    PublicationReceipt,
    RunIdentity,
    publications_from_dict,
    publications_to_dict,
)
from .mirror import MirrorError, ResolvedSnapshot, resolve_mirror_snapshot
from .paired_publish import (
    MANIFEST_PATH,
    PreparedDataset,
    prepare_dataset,
    publish_pair,
    publish_prepared,
    validate_manifest,
    verify_current,
)
from .paths import (
    ARXIV_PUBLISH_SCHEMA_VERSION,
    BUCKET_ID,
    DATACITE_EXTRACTION_VERSION,
    DATACITE_INDEX_VERSION,
    DATACITE_MARKER_VERSION,
    DATACITE_PREFIX_NAME,
    DATACITE_PUBLISH_SCHEMA_VERSION,
    NORMALIZATION_VERSION,
    PARTITION_ALGORITHM,
    DerivedGenerationPaths,
    PipelinePaths,
    runtime_source_digest,
)
from .pipeline import PipelineConfig
from .pipeline_io import (
    PipelineError,
    compress_file,
    fetch_verified_file,
    stage_snapshot,
    tree_file_metadata,
    verify_file_metadata,
    verify_uploaded_files,
)
from .publish import PublishError
from .state import StateError, read_json, utc_now, write_atomic_json
from .telemetry import EventLogger
from .transfer import fallback_download_snapshot

FULL_MODE = "datacite"
EXTRACTION_MARKER_NAME = "_EXTRACTION_COMPLETE.json"
PIPELINE_MARKER_NAME = "_PIPELINE_COMPLETE.json"
RAW_OUTPUTS_DIR = "outputs"
# The six compressed raw extractor outputs inside a derived generation, plus
# the report: the files a reusable extraction marker must list.
RAW_OUTPUT_NAMES = {kind: f"{RAW_OUTPUTS_DIR}/{kind}.jsonl.zst" for kind in FULL_OUTPUT_KINDS}
REQUIRED_RAW_FILES = (*RAW_OUTPUT_NAMES.values(), REPORT_NAME)
DATASET_URLS = {
    kind: f"https://huggingface.co/datasets/{repo}" for kind, repo in PUBLICATION_REPOS.items()
}
GENERATION_ROOTS = ("derived-generations", "raw-generations")
# Backoff between read-back attempts of an uploaded generation whose files
# are not yet visible through the mount (missing, or the wrong size): about
# five minutes in total before the upload is declared failed.
UPLOAD_VERIFY_DELAYS = (5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0, 45.0)


@dataclass(frozen=True)
class FullDependencies:
    events: Any
    api_factory: Callable[..., Any]
    store_factory: Callable[..., Any]
    resolve_mirror: Callable[..., Any]
    fallback_transfer: Callable[..., Any]
    build_extractor: Callable[..., Any]
    discover_release: Callable[..., Any]
    ensure_bundle: Callable[..., Any]
    run_extraction: Callable[..., Any]
    transform: Callable[..., Any]
    prepare_dataset: Callable[..., Any]
    publish_pair: Callable[..., Any]
    verify_current: Callable[..., Any]
    publish_prepared: Callable[..., Any]
    sleep: Callable[[float], None] = time.sleep


def default_full_dependencies() -> FullDependencies:
    from huggingface_hub import HfApi

    def store_factory(*, api: Any, token: str, mount_root: Path) -> BucketStore:
        return HfBucketStore(
            api=api, bucket_id=BUCKET_ID, mount_root=mount_root, token=token
        )

    return FullDependencies(
        events=EventLogger(),
        api_factory=lambda token: HfApi(token=token),
        store_factory=store_factory,
        resolve_mirror=resolve_mirror_snapshot,
        fallback_transfer=fallback_download_snapshot,
        build_extractor=build_extractor,
        discover_release=discover_release,
        ensure_bundle=ensure_bundle,
        run_extraction=run_datacite_extraction,
        transform=transform_datacite_outputs,
        prepare_dataset=prepare_dataset,
        publish_pair=publish_pair,
        verify_current=verify_current,
        publish_prepared=publish_prepared,
    )


@dataclass(frozen=True)
class PreparedGeneration:
    """A complete local derived generation, ready to persist: both prepared
    datasets (their files now inside `local`), every file's metadata, and
    the configuration counts."""

    local: Path
    prepared: dict[str, PreparedDataset]
    files: dict[str, FileMeta]
    counts: dict[str, Any]


@dataclass(frozen=True)
class FetchedDatasets:
    """Prepared datasets rebuilt from a durable generation after every file
    was copied locally and verified by digest."""

    prepared: dict[str, PreparedDataset]
    counts: dict[str, Any]


# --- configuration and setup -------------------------------------------------------


def check_full_config(config: PipelineConfig) -> None:
    if config.mode != FULL_MODE:
        raise PipelineError(
            f"run_full_pipeline handles mode {FULL_MODE!r}, not {config.mode!r}"
        )
    if config.bucket_root is None:
        raise PipelineError(
            "the full pipeline requires --bucket-root (the mounted shared-storage bucket)"
        )
    if not config.hf_token:
        raise PipelineError("the full pipeline requires the HF_TOKEN secret")
    try:
        config.resources.validate()
    except ValueError as error:
        raise PipelineError(f"invalid resource settings: {error}") from error


def full_paths(config: PipelineConfig) -> PipelinePaths:
    if config.bucket_root is None:
        raise PipelineError("the full pipeline requires --bucket-root")
    return PipelinePaths.from_roots(
        config.bucket_root, config.work_root, prefix_name=DATACITE_PREFIX_NAME
    )


def verify_source(config: PipelineConfig, events: Any) -> None:
    with events.phase("verify_source"):
        digest = runtime_source_digest(config.source_root)
        if digest != config.source_digest:
            raise PipelineError(
                "runtime source digest mismatch: "
                f"{digest[:12]} != {config.source_digest[:12]}"
            )


# --- phase 1: Crossref snapshot and DataCite release --------------------------------


def resolve_snapshot(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    store: BucketStore,
) -> ResolvedSnapshot:
    """Resolve the Crossref snapshot from the affiliations mirror, falling
    back to the authenticated download, and pin it to `expected_snapshot_id`
    before anything expensive happens: the mirror result is checked here, and
    the fallback checks the id it resolves before downloading a byte."""
    events = deps.events
    resolved: ResolvedSnapshot | None
    with events.phase("resolve_snapshot"):
        try:
            resolved = deps.resolve_mirror(config.bucket_root)
            events.emit(
                "snapshot_resolved",
                provenance=resolved.provenance,
                snapshot_id=resolved.snapshot_id,
            )
        except MirrorError as error:
            events.emit("mirror_unavailable", reason=str(error))
            if not config.crossref_token:
                raise PipelineError(
                    "affiliations mirror unavailable and CROSSREF_API_TOKEN is "
                    f"not set for the fallback download: {error}"
                ) from error
            resolved = None
    if resolved is None:
        resolved = deps.fallback_transfer(
            paths=paths,
            store=store,
            credential=config.crossref_token,
            source_tree_sha256=config.source_digest,
            events=events,
            run_id=config.run_id,
            expected_snapshot_id=config.expected_snapshot_id,
        )
        events.emit(
            "snapshot_resolved",
            provenance=resolved.provenance,
            snapshot_id=resolved.snapshot_id,
        )
    # Belt and braces for both sources; the fallback has already refused a
    # mismatch before downloading.
    expected = config.expected_snapshot_id
    if expected is not None and resolved.snapshot_id != expected:
        raise PipelineError(
            f"resolved Crossref snapshot {resolved.snapshot_id} does not match "
            f"the expected snapshot {expected}"
        )
    return resolved


def select_release(config: PipelineConfig, deps: FullDependencies) -> DataciteRelease:
    """Select the complete DataCite release; a pinned release must be the one
    actually selected."""
    events = deps.events
    with events.phase("select_release"):
        release = deps.discover_release(config.bucket_root, config.datacite_release)
        pinned = config.datacite_release
        if pinned is not None and release.release != pinned:
            raise PipelineError(
                f"selected DataCite release {release.release} is not the pinned "
                f"release {pinned}"
            )
        events.emit(
            "release_selected",
            release=release.release,
            source_inventory_sha256=release.inventory_sha256,
            pinned=pinned,
        )
    return release


# --- phase 2: index bundle and run identity ------------------------------------------


def prepare_bundle(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    binary: ExtractorBinary,
    release: DataciteRelease,
    store: BucketStore | None,
) -> BundleResult:
    def build_index(records: Path, output: Path) -> None:
        run_build_datacite_index(
            binary, records, output, config.resources.index_sort_memory_mb
        )

    return deps.ensure_bundle(
        paths=paths,
        release=release,
        build_index=build_index,
        store=store,
        events=deps.events,
        force=config.force,
    )


def build_run_identity(
    config: PipelineConfig,
    *,
    archive_sha256: str,
    release: DataciteRelease,
    bundle: BundleResult,
) -> RunIdentity:
    if (
        bundle.release != release.release
        or bundle.inventory_sha256 != release.inventory_sha256
    ):
        raise PipelineError(
            f"bundle {bundle.release}/{bundle.inventory_sha256[:12]} does not "
            f"belong to the selected release {release.release}/"
            f"{release.inventory_sha256[:12]}"
        )
    return RunIdentity(
        archive_sha256=archive_sha256,
        datacite_release=release.release,
        source_inventory_sha256=release.inventory_sha256,
        bundle_manifest_sha256=bundle.manifest_sha256,
        source_tree_sha256=config.source_digest,
        extraction_mode=FULL_EXTRACTION_MODE,
        index_version=DATACITE_INDEX_VERSION,
        normalization_version=NORMALIZATION_VERSION,
        extraction_version=DATACITE_EXTRACTION_VERSION,
        partition_algorithm=PARTITION_ALGORITHM,
        partitions=config.resources.partitions,
        datacite_publish_version=DATACITE_PUBLISH_SCHEMA_VERSION,
        arxiv_publish_version=ARXIV_PUBLISH_SCHEMA_VERSION,
    )


# --- phases 3-4: staging and extraction ------------------------------------------------


def stage_archive(paths: PipelinePaths, resolved: ResolvedSnapshot, events: Any) -> Path:
    """Copy the archive locally, hashing it during the copy; the digest must
    equal the one the mirror or transfer marker recorded."""
    return stage_snapshot(paths, resolved, events, verify_digest=True)


def release_archive(archive: Path, config: PipelineConfig, events: Any) -> None:
    """Free the staged local archive once extraction succeeded. Durable copies
    (the mirror's, or the bucket raw generation) are never touched."""
    if archive.is_file() and archive.is_relative_to(config.work_root):
        archive.unlink()
        events.emit("archive_released", path=str(archive))


def extract(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    binary: ExtractorBinary,
    *,
    archive: Path,
    bundle: BundleResult,
    identity: RunIdentity,
) -> tuple[FullExtractionOutputs, ExtractionReport]:
    """Run the complete extraction once under `identity` and validate its
    seven outputs and report against that identity."""
    events = deps.events
    with events.phase("extraction"):
        write_atomic_json(paths.identity_local, identity.to_dict())
        outputs = deps.run_extraction(
            binary,
            archive=archive,
            bundle=bundle.directory,
            identity_path=paths.identity_local,
            output_dir=paths.outputs_dir,
            temp_dir=paths.partitions,
            resources=config.resources,
            events=events,
        )
        missing = [path.name for path in outputs.paths() if not path.is_file()]
        if missing:
            raise PipelineError(
                f"extraction outputs missing from {paths.outputs_dir}: {missing}"
            )
        report = load_extraction_report(
            outputs.report, identity_sha256=identity.fingerprint()
        )
        events.emit(
            "extraction_completed",
            files_processed=report.files_processed,
            stats=report.stats,
            aggregation=report.aggregation,
        )
    return outputs, report


# --- phase 5: transform, validate, prepare --------------------------------------------


def transform_extraction(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    outputs: FullExtractionOutputs,
) -> dict[str, DatasetTransform]:
    if paths.transform_dir.exists():
        shutil.rmtree(paths.transform_dir)
    return deps.transform(
        outputs.valid,
        outputs.ambiguous_urls,
        paths.transform_dir,
        deps.events,
        config.resources,
    )


def require_nonempty_all(transforms: dict[str, DatasetTransform]) -> None:
    """Production requires nonempty `all` results for both datasets; smoke
    and benchmark runs do not apply this rule."""
    for kind in PUBLICATION_KINDS:
        stats = transforms[kind].stats.get("all")
        if stats is None or stats.works == 0:
            raise PipelineError(
                f"the {kind} 'all' configuration is empty; production requires "
                "nonempty all results for both datasets"
            )


def counts_from_transforms(transforms: dict[str, DatasetTransform]) -> dict[str, Any]:
    counts: dict[str, Any] = {
        kind: {name: asdict(stats) for name, stats in transforms[kind].stats.items()}
        for kind in PUBLICATION_KINDS
    }
    counts["ambiguous_urls"] = transforms["datacite"].ambiguity_rows
    return counts


def prepare_generation(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    *,
    outputs: FullExtractionOutputs,
    transforms: dict[str, DatasetTransform],
    identity: RunIdentity,
    snapshot_id: str,
    extractor_version: str,
) -> PreparedGeneration:
    """Build the local derived generation: both prepared datasets under
    `datacite/` and `arxiv/`, the compressed raw outputs, and the report."""
    events = deps.events
    local = paths.derived_local
    if local.exists():
        shutil.rmtree(local)
    local.mkdir(parents=True)
    prepared: dict[str, PreparedDataset] = {}
    with events.phase("prepare_datasets"):
        for kind in PUBLICATION_KINDS:
            out_dir = local / kind
            dataset = deps.prepare_dataset(
                kind, transforms[kind], identity, snapshot_id, extractor_version, out_dir
            )
            files: dict[str, Path] = {}
            for name, path in dataset.files.items():
                target = out_dir / name
                if path != target:
                    # Data shards still live in the transform scratch; the
                    # card and manifest were written into place.
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(path), str(target))
                files[name] = target
            prepared[kind] = replace(dataset, files=files)
    with events.phase("compress_outputs"):
        for kind in FULL_OUTPUT_KINDS:
            compress_file(getattr(outputs, kind), local / RAW_OUTPUT_NAMES[kind])
    shutil.copyfile(outputs.report, local / REPORT_NAME)
    with events.phase("hash_derived"):
        files_meta = {
            name: FileMeta(**meta) for name, meta in tree_file_metadata(local).items()
        }
    return PreparedGeneration(
        local=local,
        prepared=prepared,
        files=files_meta,
        counts=counts_from_transforms(transforms),
    )


# --- phase 6: persist the derived generation -------------------------------------------


def persist_generation(
    paths: PipelinePaths,
    store: BucketStore,
    *,
    generation: PreparedGeneration,
    derived: DerivedGenerationPaths,
    identity: RunIdentity,
    resolved: ResolvedSnapshot,
    events: Any,
    sleep: Callable[[float], None] = time.sleep,
) -> FullExtractionMarker:
    """Upload the generation, read every listed file back through the mount
    and hash it (waiting up to `UPLOAD_VERIFY_DELAYS` for files the mount
    does not show yet), and only then publish the root extraction marker.

    The generation carries its own copy of the marker, so a job that dies
    between the upload and the root marker leaves a generation the next run
    with the same identity can verify and adopt."""
    marker = FullExtractionMarker(
        kind=FULL_EXTRACTION_KIND,
        schema_version=DATACITE_MARKER_VERSION,
        identity=identity,
        snapshot_id=resolved.snapshot_id,
        snapshot_provenance=resolved.provenance,
        derived_prefix=derived.relative_prefix,
        files=generation.files,
        counts=generation.counts,
        completed_at=utc_now(),
    )
    write_atomic_json(generation.local / EXTRACTION_MARKER_NAME, marker.to_dict())

    def report(payload: dict[str, object]) -> None:
        events.emit("progress", **payload)

    with events.phase("persist_derived"):
        store.put_tree(generation.local, derived.root, progress=report)
    with events.phase("verify_derived"):
        verify_uploaded_files(
            derived.root,
            generation.files,
            label=derived.relative_prefix,
            events=events,
            delays=UPLOAD_VERIFY_DELAYS,
            sleep=sleep,
        )
    with events.phase("persist_extraction_marker"):
        store.put_json(marker.to_dict(), paths.extraction_marker)
    events.emit(
        "extraction_persisted",
        derived_prefix=derived.relative_prefix,
        files=len(generation.files),
        identity_sha256=identity.fingerprint(),
    )
    return marker


def release_scratch(paths: PipelinePaths, events: Any) -> None:
    """Drop the raw outputs and transform scratch once the generation is
    durable; the local generation stays for publication."""
    for scratch in (paths.outputs_dir, paths.transform_dir):
        if scratch.exists():
            shutil.rmtree(scratch)
    events.emit("scratch_released", paths=[str(paths.outputs_dir), str(paths.transform_dir)])


# --- reuse of a persisted generation ------------------------------------------------------


def reusable_extraction(
    paths: PipelinePaths, identity: RunIdentity, events: Any, *, force: bool
) -> FullExtractionMarker | None:
    """The root extraction marker, when it names exactly this run identity."""
    if force:
        events.emit("extraction_reuse_skipped", reason="force")
        return None
    if not paths.extraction_marker.is_file():
        return None
    try:
        marker = FullExtractionMarker.from_dict(read_json(paths.extraction_marker))
    except StateError as error:
        events.emit("extraction_marker_invalid", reason=str(error))
        return None
    if marker.identity != identity:
        events.emit(
            "extraction_marker_stale",
            marker_identity_sha256=marker.identity.fingerprint(),
            identity_sha256=identity.fingerprint(),
        )
        return None
    return marker


def find_uploaded_generation(
    paths: PipelinePaths,
    identity: RunIdentity,
    resolved: ResolvedSnapshot,
    events: Any,
) -> FullExtractionMarker | None:
    """A derived generation uploaded for exactly this run identity whose root
    marker was never written (the job died, or the marker upload failed,
    after `put_tree`).

    Each `derived-generations/*` entry's own `_EXTRACTION_COMPLETE.json` is
    read; generations of other identities are ignored. A candidate must name
    its own prefix and the resolved snapshot, list every required raw output,
    and have every listed file present with its recorded size and SHA-256.
    Anything partial, corrupt or mismatched is reported as
    `extraction_generation_rejected` and never adopted. The newest candidate
    that verifies is returned; nothing is written here."""
    root = paths.prefix / "derived-generations"
    if not root.is_dir():
        return None
    candidates: list[FullExtractionMarker] = []
    for entry in sorted(root.iterdir()):
        marker_path = entry / EXTRACTION_MARKER_NAME
        if not entry.is_dir() or not marker_path.is_file():
            continue
        relative = f"derived-generations/{entry.name}"
        try:
            marker = FullExtractionMarker.from_dict(read_json(marker_path))
        except StateError as error:
            events.emit(
                "extraction_generation_rejected", derived_prefix=relative, reason=str(error)
            )
            continue
        if marker.identity != identity:
            continue
        reason: str | None = None
        if marker.derived_prefix != relative:
            reason = f"marker names {marker.derived_prefix}"
        elif marker.snapshot_id != resolved.snapshot_id:
            reason = (
                f"marker snapshot {marker.snapshot_id} differs from the resolved "
                f"snapshot {resolved.snapshot_id}"
            )
        else:
            try:
                marker.require_files(REQUIRED_RAW_FILES)
            except StateError as error:
                reason = str(error)
        if reason is not None:
            events.emit("extraction_generation_rejected", derived_prefix=relative, reason=reason)
            continue
        candidates.append(marker)
    for marker in sorted(candidates, key=lambda item: item.completed_at, reverse=True):
        derived = paths.derived_generation(marker.derived_prefix)
        try:
            verify_file_metadata(derived.root, marker.files, label=marker.derived_prefix)
        except PipelineError as error:
            events.emit(
                "extraction_generation_rejected",
                derived_prefix=marker.derived_prefix,
                reason=str(error),
            )
            continue
        events.emit(
            "extraction_generation_verified",
            derived_prefix=marker.derived_prefix,
            files=len(marker.files),
        )
        return marker
    return None


def fetch_prepared_datasets(
    derived_root: Path,
    local: Path,
    *,
    identity: RunIdentity,
    events: Any,
    files: dict[str, FileMeta] | None = None,
    artifact_digests: dict[str, str] | None = None,
) -> FetchedDatasets:
    """Rebuild both prepared datasets from a durable generation.

    Every file is copied locally and hashed on the way; the manifests must
    match the extraction marker's metadata (`files`) and/or the activated
    receipts' artifact digests (`artifact_digests`). Any mismatch is an
    error naming the file, never a silent recomputation.
    """
    if local.exists():
        shutil.rmtree(local)
    prepared: dict[str, PreparedDataset] = {}
    counts: dict[str, Any] = {}
    manifests: dict[str, dict[str, Any]] = {}
    with events.phase("fetch_derived_datasets"):
        for kind in PUBLICATION_KINDS:
            manifest_name = f"{kind}/{MANIFEST_PATH}"
            source = derived_root / manifest_name
            if not source.is_file():
                raise PipelineError(f"derived file {manifest_name} is missing: {source}")
            payload = source.read_bytes()
            digest = hashlib.sha256(payload).hexdigest()
            if files is not None:
                expected = files.get(manifest_name)
                if expected is None:
                    raise PipelineError(
                        f"extraction marker does not list {manifest_name}"
                    )
                if FileMeta(size=len(payload), sha256=digest) != expected:
                    raise PipelineError(
                        f"derived file {manifest_name} digest {digest} "
                        f"({len(payload)} bytes) does not match the extraction "
                        f"marker ({expected.sha256}, {expected.size} bytes)"
                    )
            if artifact_digests is not None and digest != artifact_digests[kind]:
                raise PipelineError(
                    f"derived file {manifest_name} digest {digest} does not match "
                    f"the activated {kind} artifact {artifact_digests[kind]}"
                )
            try:
                manifest = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as error:
                raise PipelineError(
                    f"derived file {manifest_name} is not valid JSON"
                ) from error
            try:
                metas = validate_manifest(manifest)
            except PublishError as error:
                raise PipelineError(
                    f"derived file {manifest_name} is invalid: {error}"
                ) from error
            if manifest["dataset_kind"] != kind:
                raise PipelineError(
                    f"derived file {manifest_name} describes the "
                    f"{manifest['dataset_kind']} dataset"
                )
            if manifest["identity"] != identity.to_dict():
                raise PipelineError(
                    f"derived file {manifest_name} was prepared for another run identity"
                )
            local_files: dict[str, Path] = {}
            for name, meta in sorted(metas.items()):
                full_name = f"{kind}/{name}"
                if files is not None:
                    recorded = files.get(full_name)
                    if recorded is None:
                        raise PipelineError(
                            f"extraction marker does not list {full_name}"
                        )
                    if recorded != meta:
                        raise PipelineError(
                            f"derived file {full_name}: the extraction marker and "
                            "the manifest disagree on its size or digest"
                        )
                local_files[name] = fetch_verified_file(
                    derived_root / full_name, local / full_name, meta, name=full_name
                )
            manifest_local = local / manifest_name
            manifest_local.parent.mkdir(parents=True, exist_ok=True)
            manifest_local.write_bytes(payload)
            local_files[MANIFEST_PATH] = manifest_local
            prepared[kind] = PreparedDataset(
                repo_id=PUBLICATION_REPOS[kind],
                files=local_files,
                artifact_digest=digest,
            )
            manifests[kind] = manifest
            counts[kind] = manifest["counts"]
    counts["ambiguous_urls"] = manifests["datacite"]["ambiguity_rows"]
    events.emit(
        "derived_datasets_fetched",
        files=sum(len(item.files) for item in prepared.values()),
        artifact_digests={kind: item.artifact_digest for kind, item in prepared.items()},
    )
    return FetchedDatasets(prepared=prepared, counts=counts)


# --- phase 7: paired publication -----------------------------------------------------------


def load_publication_journal(path: Path, events: Any) -> dict[str, PublicationReceipt]:
    """Receipts journaled so far for a generation, read from the bucket. A
    lost or unreadable journal is recoverable: `publish_prepared` recognises
    a completed publication by its manifest digest without recommitting."""
    if not path.is_file():
        return {}
    try:
        receipts = publications_from_dict(read_json(path))
    except StateError as error:
        events.emit("publication_journal_invalid", path=str(path), reason=str(error))
        return {}
    events.emit("publication_journal_loaded", kinds=sorted(receipts))
    return receipts


def publish_datasets(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    store: BucketStore,
    api: Any,
    *,
    derived: DerivedGenerationPaths,
    prepared: dict[str, PreparedDataset],
) -> dict[str, PublicationReceipt]:
    """Publish DataCite then arXiv through `publish_pair`, journaling each
    verified receipt to `<derived>/_PUBLICATIONS.json` as it lands."""
    events = deps.events
    token = config.hf_token
    journal_remote = derived.root / PUBLICATIONS_JOURNAL_NAME
    journal_local = paths.work_root / "state" / PUBLICATIONS_JOURNAL_NAME
    receipts = load_publication_journal(journal_remote, events)

    def commit_one(dataset: PreparedDataset) -> PublicationReceipt:
        return deps.publish_prepared(
            api=api, token=token, prepared=dataset, events=events
        )

    def check_current(dataset: PreparedDataset, receipt: PublicationReceipt) -> None:
        deps.verify_current(api=api, token=token, prepared=dataset, receipt=receipt)

    def save_receipt(kind: str, receipt: PublicationReceipt) -> None:
        receipts[kind] = receipt
        payload = publications_to_dict(receipts)
        try:
            write_atomic_json(journal_local, payload)
            store.put_json(payload, journal_remote)
        except Exception as error:
            raise PipelineError(
                f"cannot journal the {kind} publication receipt to "
                f"{journal_remote.name}: {error}"
            ) from error
        events.emit(
            "publication_journaled",
            kind=kind,
            commit=receipt.commit_sha,
            artifact_digest=receipt.artifact_digest,
        )

    with events.phase("publish_datasets"):
        return deps.publish_pair(
            prepared, dict(receipts), commit_one, save_receipt, check_current
        )


# --- phase 8: activation and cleanup ---------------------------------------------------------


def load_active_marker(paths: PipelinePaths, events: Any) -> FullPipelineMarker | None:
    if not paths.pipeline_marker.is_file():
        return None
    try:
        return FullPipelineMarker.from_dict(read_json(paths.pipeline_marker))
    except StateError as error:
        events.emit("pipeline_marker_invalid", reason=str(error))
        return None


def activate(
    paths: PipelinePaths,
    store: BucketStore,
    *,
    identity: RunIdentity,
    snapshot_id: str,
    derived_prefix: str,
    receipts: dict[str, PublicationReceipt],
    events: Any,
) -> FullPipelineMarker:
    """Write `_PIPELINE_COMPLETE.json`; callers invoke this only after
    `publish_pair` returned with both destinations verified."""
    marker = FullPipelineMarker(
        kind=FULL_PIPELINE_KIND,
        schema_version=DATACITE_MARKER_VERSION,
        identity=identity,
        snapshot_id=snapshot_id,
        derived_prefix=derived_prefix,
        publications={kind: receipts[kind] for kind in PUBLICATION_KINDS},
        completed_at=utc_now(),
    )
    write_atomic_json(paths.work_root / "state" / PIPELINE_MARKER_NAME, marker.to_dict())
    with events.phase("activate"):
        store.put_json(marker.to_dict(), paths.pipeline_marker)
    events.emit(
        "pipeline_activated",
        derived_prefix=derived_prefix,
        identity_sha256=identity.fingerprint(),
        publications={kind: receipt.commit_sha for kind, receipt in marker.publications.items()},
    )
    return marker


def cleanup_prior_generations(
    paths: PipelinePaths, store: BucketStore, *, keep: set[str], events: Any
) -> None:
    """Remove derived and raw generations under this prefix that are not in
    `keep`. Nothing outside the owned prefix (the arXiv-owned prefix, the
    affiliations mirror, the index cache) is ever touched; failures are
    reported and do not undo the activation."""
    for root_name in GENERATION_ROOTS:
        root = paths.prefix / root_name
        if not root.is_dir():
            continue
        for entry in sorted(root.iterdir()):
            if not entry.is_dir():
                continue
            relative = f"{root_name}/{entry.name}"
            if relative in keep:
                continue
            try:
                store.delete_tree(entry)
                events.emit("cleanup_removed", prefix=relative)
            except Exception as error:
                events.emit("cleanup_failed", prefix=relative, reason=str(error))


# --- orchestration --------------------------------------------------------------------------------


def _result(
    status: str,
    *,
    config: PipelineConfig,
    snapshot_id: str,
    release: str,
    derived_prefix: str,
    identity: RunIdentity,
    extraction_reused: bool,
    receipts: dict[str, PublicationReceipt],
    counts: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "snapshot_id": snapshot_id,
        "datacite_release": release,
        "derived_prefix": derived_prefix,
        "identity_sha256": identity.fingerprint(),
        "source_tree_sha256": config.source_digest,
        "extraction_reused": extraction_reused,
        "dataset_urls": dict(DATASET_URLS),
        "publications": {kind: receipts[kind].to_dict() for kind in PUBLICATION_KINDS},
        "counts": counts,
    }


def verify_active_publication(
    config: PipelineConfig,
    deps: FullDependencies,
    paths: PipelinePaths,
    api: Any,
    *,
    active: FullPipelineMarker,
    identity: RunIdentity,
) -> dict[str, Any]:
    """An activated run with this identity is a no-op only if both
    repositories still publish exactly the persisted artifacts."""
    events = deps.events
    derived = paths.derived_generation(active.derived_prefix)
    fetched = fetch_prepared_datasets(
        derived.root,
        paths.work_root / "reused",
        identity=identity,
        events=events,
        artifact_digests={
            kind: receipt.artifact_digest for kind, receipt in active.publications.items()
        },
    )
    with events.phase("verify_current_publications"):
        for kind in PUBLICATION_KINDS:
            try:
                deps.verify_current(
                    api=api,
                    token=config.hf_token,
                    prepared=fetched.prepared[kind],
                    receipt=active.publications[kind],
                )
            except Exception as error:
                raise PipelineError(
                    f"concurrent change: the {kind} dataset no longer publishes "
                    f"the activated artifact: {error}"
                ) from error
    events.emit(
        "pipeline_noop",
        derived_prefix=active.derived_prefix,
        publications={kind: r.commit_sha for kind, r in active.publications.items()},
    )
    return _result(
        "no-op",
        config=config,
        snapshot_id=active.snapshot_id,
        release=identity.datacite_release,
        derived_prefix=active.derived_prefix,
        identity=identity,
        extraction_reused=True,
        receipts=active.publications,
        counts=fetched.counts,
    )


def run_full_pipeline(
    config: PipelineConfig, deps: FullDependencies | None = None
) -> dict[str, Any]:
    deps = deps or default_full_dependencies()
    events = deps.events
    check_full_config(config)
    verify_source(config, events)
    paths = full_paths(config)
    api = deps.api_factory(config.hf_token)
    store = deps.store_factory(
        api=api, token=config.hf_token, mount_root=config.bucket_root
    )
    binary = deps.build_extractor(
        config.source_root, config.work_root / "cargo-target", events
    )

    resolved = resolve_snapshot(config, deps, paths, store)
    release = select_release(config, deps)
    bundle = prepare_bundle(config, deps, paths, binary, release, store)
    identity = build_run_identity(
        config, archive_sha256=resolved.archive_sha256, release=release, bundle=bundle
    )
    events.emit("run_identity", identity_sha256=identity.fingerprint(), **identity.to_dict())

    active = load_active_marker(paths, events)
    if active is not None and active.identity == identity and not config.force:
        return verify_active_publication(
            config, deps, paths, api, active=active, identity=identity
        )

    marker = reusable_extraction(paths, identity, events, force=config.force)
    adopted = False
    if marker is None and not config.force:
        with events.phase("find_uploaded_generation"):
            marker = find_uploaded_generation(paths, identity, resolved, events)
        adopted = marker is not None
    if marker is not None:
        if marker.snapshot_id != resolved.snapshot_id:
            raise PipelineError(
                f"extraction marker snapshot {marker.snapshot_id} differs from the "
                f"resolved snapshot {resolved.snapshot_id}"
            )
        derived = paths.derived_generation(marker.derived_prefix)
        marker.require_files(REQUIRED_RAW_FILES)
        fetched = fetch_prepared_datasets(
            derived.root,
            paths.work_root / "reused",
            identity=identity,
            events=events,
            files=marker.files,
        )
        if fetched.counts != marker.counts:
            raise PipelineError(
                "extraction marker counts disagree with the persisted manifests"
            )
        if adopted:
            # Only a generation that passed every reuse check becomes the
            # root marker; cleanup keeps it because it is this run's prefix.
            with events.phase("adopt_extraction_marker"):
                store.put_json(marker.to_dict(), paths.extraction_marker)
            events.emit(
                "extraction_generation_adopted",
                derived_prefix=marker.derived_prefix,
                identity_sha256=identity.fingerprint(),
            )
        prepared = fetched.prepared
        counts = fetched.counts
        extraction_reused = True
        events.emit("extraction_reused", derived_prefix=marker.derived_prefix)
    else:
        archive = stage_archive(paths, resolved, events)
        outputs, _report = extract(
            config, deps, paths, binary, archive=archive, bundle=bundle, identity=identity
        )
        release_archive(archive, config, events)
        transforms = transform_extraction(config, deps, paths, outputs)
        require_nonempty_all(transforms)
        generation = prepare_generation(
            config,
            deps,
            paths,
            outputs=outputs,
            transforms=transforms,
            identity=identity,
            snapshot_id=resolved.snapshot_id,
            extractor_version=binary.version,
        )
        derived = paths.new_derived_generation(resolved.snapshot_id, config.run_id)
        marker = persist_generation(
            paths,
            store,
            generation=generation,
            derived=derived,
            identity=identity,
            resolved=resolved,
            events=events,
            sleep=deps.sleep,
        )
        release_scratch(paths, events)
        prepared = generation.prepared
        counts = generation.counts
        extraction_reused = False

    receipts = publish_datasets(
        config, deps, paths, store, api, derived=derived, prepared=prepared
    )
    activate(
        paths,
        store,
        identity=identity,
        snapshot_id=marker.snapshot_id,
        derived_prefix=marker.derived_prefix,
        receipts=receipts,
        events=events,
    )
    keep = {marker.derived_prefix}
    raw_prefix = resolved.source.get("raw_prefix")
    if resolved.provenance == "download" and isinstance(raw_prefix, str):
        keep.add(raw_prefix)
    cleanup_prior_generations(paths, store, keep=keep, events=events)

    return _result(
        "completed",
        config=config,
        snapshot_id=marker.snapshot_id,
        release=release.release,
        derived_prefix=marker.derived_prefix,
        identity=identity,
        extraction_reused=extraction_reused,
        receipts=receipts,
        counts=counts,
    )
