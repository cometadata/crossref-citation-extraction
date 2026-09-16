from __future__ import annotations

import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from .arxiv_index import discover_datacite_release, ensure_fst
from .bucket import BucketStore, HfBucketStore
from .extractor import build_extractor, run_build_index, run_extraction
from .full_state import ResourceSettings
from .mirror import MirrorError, ResolvedSnapshot, resolve_mirror_snapshot
from .paths import (
    ARCHIVE_NAME,
    BUCKET_ID,
    CONFIG_NAMES,
    DATASET_REPO,
    EXTRACTION_SCHEMA_VERSION,
    MARKER_SCHEMA_VERSION,
    OUTPUT_KINDS,
    PUBLISH_SCHEMA_VERSION,
    PipelinePaths,
    runtime_source_digest,
)
from .pipeline_io import PipelineError, stage_snapshot, tree_file_metadata
from .pipeline_io import compress_file as _compress_file
from .pipeline_io import count_lines as _count_lines
from .publish import (
    ConfigStats,
    PublishError,
    ensure_not_paired_publication,
    publish_dataset,
    render_dataset_card,
    transform_outputs,
)
from .smoke import SMOKE_EXPECTED, create_smoke_datacite_records, create_smoke_snapshot
from .state import (
    ExtractionMarker,
    PipelineMarker,
    StateError,
    can_reuse_extraction,
    read_json,
    utc_now,
    write_atomic_json,
)
from .telemetry import EventLogger
from .transfer import fallback_download_snapshot

PIPELINE_MODES = ("legacy", "datacite", "smoke", "benchmark")


@dataclass(frozen=True)
class PipelineConfig:
    source_root: Path
    source_digest: str
    work_root: Path
    bucket_root: Path | None
    smoke: bool
    force: bool
    datacite_release: str | None
    hf_token: str | None
    crossref_token: str | None
    run_id: str
    # Full mode (Task 11 onwards). Defaults keep every legacy construction
    # site working: `run_pipeline` handles `legacy`; `run_full_pipeline`
    # handles `datacite`; smoke and benchmark reuse the full-mode phases.
    mode: str = "legacy"
    resources: ResourceSettings = field(default_factory=ResourceSettings)
    expected_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in PIPELINE_MODES:
            raise ValueError(
                f"mode must be one of {PIPELINE_MODES}: {self.mode!r}"
            )


@dataclass(frozen=True)
class PipelineResult:
    status: str
    snapshot_id: str | None = None
    snapshot_provenance: str | None = None
    datacite_release: str | None = None
    derived_prefix: str | None = None
    dataset_commit: str | None = None
    dataset_commit_url: str | None = None
    counts: dict[str, Any] | None = None
    extraction_reused: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value for key, value in asdict(self).items() if value is not None
        }


@dataclass(frozen=True)
class PipelineDependencies:
    events: Any
    api_factory: Callable[..., Any]
    store_factory: Callable[..., Any]
    resolve_mirror: Callable[..., Any]
    fallback_transfer: Callable[..., Any]
    build_extractor: Callable[..., Any]
    run_build_index: Callable[..., Any]
    run_extraction: Callable[..., Any]
    discover_release: Callable[..., Any]
    ensure_fst: Callable[..., Any]
    transform: Callable[..., Any]
    render_card: Callable[..., str]
    publish: Callable[..., Any]


def production_dependencies() -> PipelineDependencies:
    from huggingface_hub import HfApi

    def store_factory(*, api: Any, token: str, mount_root: Path) -> BucketStore:
        return HfBucketStore(
            api=api, bucket_id=BUCKET_ID, mount_root=mount_root, token=token
        )

    return PipelineDependencies(
        events=EventLogger(),
        api_factory=lambda token: HfApi(token=token),
        store_factory=store_factory,
        resolve_mirror=resolve_mirror_snapshot,
        fallback_transfer=fallback_download_snapshot,
        build_extractor=build_extractor,
        run_build_index=run_build_index,
        run_extraction=run_extraction,
        discover_release=discover_datacite_release,
        ensure_fst=ensure_fst,
        transform=transform_outputs,
        render_card=render_dataset_card,
        publish=publish_dataset,
    )


def run_pipeline(
    config: PipelineConfig, deps: PipelineDependencies | None = None
) -> PipelineResult:
    deps = deps or production_dependencies()
    with deps.events.phase("verify_source"):
        digest = runtime_source_digest(config.source_root)
        if digest != config.source_digest:
            raise PipelineError(
                "runtime source digest mismatch: "
                f"{digest[:12]} != {config.source_digest[:12]}"
            )
    if config.smoke:
        return _run_smoke(config, deps)
    return _run_production(config, deps)


def _run_smoke(
    config: PipelineConfig, deps: PipelineDependencies
) -> PipelineResult:
    events = deps.events
    work = config.work_root
    archive = work / "smoke" / ARCHIVE_NAME
    records = work / "smoke" / "datacite.jsonl.gz"
    with events.phase("create_smoke_fixtures"):
        create_smoke_snapshot(archive)
        create_smoke_datacite_records(records)
    binary = deps.build_extractor(config.source_root, work / "cargo-target", events)
    fst_path = work / "fst" / "arxiv.fst"
    with events.phase("build_fst"):
        deps.run_build_index(binary, records, fst_path)
    outputs = deps.run_extraction(
        binary,
        archive=archive,
        fst=fst_path,
        output_dir=work / "outputs",
        temp_dir=work / "partitions",
        events=events,
    )
    transform_result = deps.transform(outputs.valid, work / "transform", events)
    counts = {
        name: asdict(transform_result.stats[name]) for name in CONFIG_NAMES
    }
    failed_works = _count_lines(outputs.failed)
    expected = {name: SMOKE_EXPECTED[name] for name in CONFIG_NAMES}
    if counts != expected or failed_works != SMOKE_EXPECTED["failed_works"]:
        raise PipelineError(
            f"smoke counts mismatch: got {counts} failed_works={failed_works}, "
            f"expected {expected} failed_works={SMOKE_EXPECTED['failed_works']}"
        )
    return PipelineResult(
        status="smoke-completed", counts={**counts, "failed_works": failed_works}
    )


def _load_pipeline_marker(paths: PipelinePaths) -> PipelineMarker | None:
    if not paths.pipeline_marker.is_file():
        return None
    try:
        return PipelineMarker.from_dict(read_json(paths.pipeline_marker))
    except (StateError, ValueError, OSError):
        return None


def _dataset_commit_exists(api: Any, token: str | None, commit: str) -> bool:
    try:
        api.dataset_info(DATASET_REPO, revision=commit, token=token)
        return True
    except Exception:
        return False


def _reusable_extraction(
    paths: PipelinePaths,
    config: PipelineConfig,
    resolved: ResolvedSnapshot,
    fst: Any,
) -> tuple[ExtractionMarker, Any] | None:
    if not paths.extraction_marker.is_file():
        return None
    try:
        marker = ExtractionMarker.from_dict(read_json(paths.extraction_marker))
    except (StateError, ValueError, OSError):
        return None
    if not can_reuse_extraction(
        marker,
        archive_sha256=resolved.archive_sha256,
        datacite_release=fst.release,
        fst_sha256=fst.sha256,
        source_tree_sha256=config.source_digest,
    ):
        return None
    derived = paths.derived_generation(marker.derived_prefix)
    for name, meta in marker.files.items():
        candidate = derived.root / name
        if not candidate.is_file() or candidate.stat().st_size != meta["size"]:
            return None
    return marker, derived


def _stage_snapshot(
    paths: PipelinePaths, resolved: ResolvedSnapshot, events: Any
) -> Path:
    # Legacy markers carry the archive digest but the legacy contract never
    # compared it during staging; the full pipeline does (see pipeline_io).
    return stage_snapshot(paths, resolved, events, verify_digest=False)


def _persist_derived_generation(
    *,
    paths: PipelinePaths,
    store: Any,
    derived: Any,
    outputs: Any,
    transform_result: Any,
    config: PipelineConfig,
    resolved: ResolvedSnapshot,
    fst: Any,
    binary: Any,
    events: Any,
) -> ExtractionMarker:
    local = paths.derived_local
    if local.exists():
        shutil.rmtree(local)
    (local / "data").mkdir(parents=True)
    (local / "outputs").mkdir(parents=True)
    for name in CONFIG_NAMES:
        shutil.copyfile(
            transform_result.parquet[name], local / "data" / f"{name}.parquet"
        )
    with events.phase("compress_outputs"):
        for kind in OUTPUT_KINDS:
            _compress_file(
                getattr(outputs, kind), local / "outputs" / f"{kind}.jsonl.zst"
            )
    counts = {
        name: asdict(stat) for name, stat in transform_result.stats.items()
    }
    write_atomic_json(
        local / "report.json",
        {
            "counts": counts,
            "reference_keys": {
                name: list(keys)
                for name, keys in transform_result.reference_keys.items()
            },
            "dropped_reference_keys": list(
                transform_result.dropped_reference_keys
            ),
            "snapshot": {
                **resolved.source,
                "snapshot_id": resolved.snapshot_id,
                "provenance": resolved.provenance,
                "archive_sha256": resolved.archive_sha256,
            },
            "datacite_release": fst.release,
            "extractor_version": binary.version,
            "publish_schema_version": PUBLISH_SCHEMA_VERSION,
        },
    )
    files = tree_file_metadata(local)
    marker = ExtractionMarker(
        schema_version=MARKER_SCHEMA_VERSION,
        extraction_schema_version=EXTRACTION_SCHEMA_VERSION,
        snapshot_id=resolved.snapshot_id,
        snapshot_provenance=resolved.provenance,
        archive_sha256=resolved.archive_sha256,
        datacite_release=fst.release,
        fst_sha256=fst.sha256,
        source_tree_sha256=config.source_digest,
        extractor_version=binary.version,
        derived_prefix=derived.relative_prefix,
        files=files,
        counts=counts,
        completed_at=utc_now(),
    )
    write_atomic_json(local / "_EXTRACTION_COMPLETE.json", marker.to_dict())

    def report(payload: dict[str, object]) -> None:
        events.emit("progress", **payload)

    with events.phase("persist_derived"):
        store.put_tree(local, derived.root, progress=report)
        store.put_json(marker.to_dict(), paths.extraction_marker)
    return marker


def _cleanup_prior_generations(
    paths: PipelinePaths,
    store: Any,
    prior: PipelineMarker | None,
    current: PipelineMarker,
    events: Any,
) -> None:
    if prior is None:
        return
    targets: list[Path] = []
    if prior.derived_prefix != current.derived_prefix:
        targets.append(paths.derived_generation(prior.derived_prefix).root)
    if prior.raw_prefix and prior.raw_prefix != current.raw_prefix:
        targets.append(paths.raw_generation(prior.raw_prefix).root)
    for target in targets:
        try:
            store.delete_tree(target)
            events.emit("cleanup_removed", prefix=str(target))
        except Exception as error:
            events.emit("cleanup_failed", prefix=str(target), reason=str(error))


def _run_production(
    config: PipelineConfig, deps: PipelineDependencies
) -> PipelineResult:
    if config.bucket_root is None:
        raise PipelineError(
            "production requires --bucket-root (the mounted shared-storage bucket)"
        )
    if not config.hf_token:
        raise PipelineError("production requires the HF_TOKEN secret")
    events = deps.events
    paths = PipelinePaths.from_roots(config.bucket_root, config.work_root)
    api = deps.api_factory(config.hf_token)
    # The full DataCite pipeline owns the arXiv dataset once it has published
    # it; refuse before any build or extraction work (publication checks
    # again at the commit's parent revision).
    with events.phase("check_dataset_ownership"):
        try:
            ensure_not_paired_publication(api, config.hf_token)
        except PublishError as error:
            raise PipelineError(str(error)) from error
    store = deps.store_factory(
        api=api, token=config.hf_token, mount_root=config.bucket_root
    )

    binary = deps.build_extractor(
        config.source_root, config.work_root / "cargo-target", events
    )

    resolved: ResolvedSnapshot | None
    with events.phase("resolve_snapshot"):
        try:
            resolved = deps.resolve_mirror(config.bucket_root)
            events.emit(
                "snapshot_resolved",
                provenance="mirror",
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
        )

    with events.phase("prepare_index"):
        release = deps.discover_release(
            config.bucket_root, config.datacite_release
        )
    fst = deps.ensure_fst(
        paths=paths,
        release=release,
        build_index=lambda records, output: deps.run_build_index(
            binary, records, output
        ),
        extractor_version=binary.version,
        store=store,
        events=events,
        force=config.force,
    )

    prior_marker = _load_pipeline_marker(paths)
    if (
        not config.force
        and prior_marker is not None
        and prior_marker.archive_sha256 == resolved.archive_sha256
        and prior_marker.datacite_release == fst.release
        and prior_marker.fst_sha256 == fst.sha256
        and prior_marker.source_tree_sha256 == config.source_digest
        and _dataset_commit_exists(api, config.hf_token, prior_marker.dataset_commit)
    ):
        events.emit("pipeline_noop", dataset_commit=prior_marker.dataset_commit)
        return PipelineResult(
            status="no-op",
            snapshot_id=prior_marker.snapshot_id,
            snapshot_provenance=prior_marker.snapshot_provenance,
            datacite_release=prior_marker.datacite_release,
            derived_prefix=prior_marker.derived_prefix,
            dataset_commit=prior_marker.dataset_commit,
            dataset_commit_url=prior_marker.dataset_commit_url,
        )

    reuse = None if config.force else _reusable_extraction(
        paths, config, resolved, fst
    )
    if reuse is not None:
        extraction_marker, derived_paths = reuse
        events.emit(
            "extraction_reused", derived_prefix=extraction_marker.derived_prefix
        )
        paths.transform_dir.mkdir(parents=True, exist_ok=True)
        parquet: dict[str, Path] = {}
        with events.phase("fetch_reused_outputs"):
            for name in CONFIG_NAMES:
                target = paths.transform_dir / f"{name}.parquet"
                shutil.copyfile(derived_paths.data / f"{name}.parquet", target)
                parquet[name] = target
        stats = {
            name: ConfigStats(**extraction_marker.counts[name])
            for name in CONFIG_NAMES
        }
        derived_prefix = extraction_marker.derived_prefix
        extraction_reused = True
    else:
        local_archive = _stage_snapshot(paths, resolved, events)
        outputs = deps.run_extraction(
            binary,
            archive=local_archive,
            fst=fst.local_path,
            output_dir=paths.outputs_dir,
            temp_dir=paths.partitions,
            events=events,
        )
        transform_result = deps.transform(
            outputs.valid, paths.transform_dir, events
        )
        derived = paths.new_derived_generation(resolved.snapshot_id, config.run_id)
        _persist_derived_generation(
            paths=paths,
            store=store,
            derived=derived,
            outputs=outputs,
            transform_result=transform_result,
            config=config,
            resolved=resolved,
            fst=fst,
            binary=binary,
            events=events,
        )
        parquet = transform_result.parquet
        stats = transform_result.stats
        derived_prefix = derived.relative_prefix
        extraction_reused = False

    card = deps.render_card(
        stats,
        snapshot_id=resolved.snapshot_id,
        datacite_release=fst.release,
        extractor_version=binary.version,
    )
    publication = deps.publish(
        api=api,
        token=config.hf_token,
        parquet=parquet,
        card=card,
        snapshot_id=resolved.snapshot_id,
        events=events,
    )

    pipeline_marker = PipelineMarker(
        schema_version=MARKER_SCHEMA_VERSION,
        snapshot_id=resolved.snapshot_id,
        snapshot_provenance=resolved.provenance,
        archive_sha256=resolved.archive_sha256,
        datacite_release=fst.release,
        fst_sha256=fst.sha256,
        source_tree_sha256=config.source_digest,
        derived_prefix=derived_prefix,
        raw_prefix=(
            resolved.source.get("raw_prefix")
            if resolved.provenance == "download"
            else None
        ),
        dataset_repo=DATASET_REPO,
        dataset_commit=publication.commit_sha,
        dataset_commit_url=publication.commit_url,
        completed_at=utc_now(),
    )
    with events.phase("activate"):
        store.put_json(pipeline_marker.to_dict(), paths.pipeline_marker)
    _cleanup_prior_generations(paths, store, prior_marker, pipeline_marker, events)

    return PipelineResult(
        status="completed",
        snapshot_id=resolved.snapshot_id,
        snapshot_provenance=resolved.provenance,
        datacite_release=fst.release,
        derived_prefix=derived_prefix,
        dataset_commit=publication.commit_sha,
        dataset_commit_url=publication.commit_url,
        counts={name: asdict(stat) for name, stat in stats.items()},
        extraction_reused=extraction_reused,
    )
