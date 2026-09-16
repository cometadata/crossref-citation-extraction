"""Full DataCite pipeline orchestration: phases, durable state, paired publication.

Every test runs the real transform and dataset preparation on a tiny fixture,
persists through a real `LocalBucketStore`, and reads markers and journals
back from the mount, so retries see genuine durable state.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from citations_pipeline.bucket import BucketPersistenceError, LocalBucketStore
from citations_pipeline.datacite_index import (
    BUNDLE_FILES,
    BUNDLE_MARKER_NAME,
    BundleResult,
    DataciteIndexError,
    DataciteRelease,
    discover_release,
)
from citations_pipeline.datacite_publish import transform_datacite_outputs
from citations_pipeline.extractor import ExtractorBinary, FullExtractionOutputs
from citations_pipeline.full_pipeline import (
    FullDependencies,
    default_full_dependencies,
    run_full_pipeline,
)
from citations_pipeline.full_state import (
    FULL_EXTRACTION_KIND,
    FULL_PIPELINE_KIND,
    FullExtractionMarker,
    FullPipelineMarker,
    PublicationReceipt,
    ResourceSettings,
)
from citations_pipeline.mirror import MirrorError, ResolvedSnapshot
from citations_pipeline.paired_publish import (
    MANIFEST_PATH,
    prepare_dataset,
    publish_pair,
)
from citations_pipeline.paths import (
    DATACITE_PREFIX_NAME,
    PipelinePaths,
    runtime_source_digest,
)
from citations_pipeline.pipeline import PipelineConfig, PipelineError
from citations_pipeline.publish import PublishError
from citations_pipeline.state import read_json
from citations_pipeline.telemetry import EventLogger

RUN = "9" * 32
MIRROR_RUN = "7" * 32
OLD_RUN = "8" * 32
TOKEN = "hf-secret-token"
ARCHIVE_BYTES = b"crossref-snapshot-fixture\n" * 64
ARXIV_TARGET = "10.48550/arxiv.2403.03542"
REPOS = {
    "datacite": "cometadata/crossref-datacite-citations",
    "arxiv": "cometadata/crossref-arxiv-citations",
}
KINDS_BY_REPO = {repo: kind for kind, repo in REPOS.items()}
RAW_OUTPUT_NAMES = (
    "outputs/valid.jsonl.zst",
    "outputs/failed.jsonl.zst",
    "outputs/publisher.jsonl.zst",
    "outputs/crossref.jsonl.zst",
    "outputs/mined.jsonl.zst",
    "outputs/ambiguous_urls.jsonl.zst",
    "extraction-report.json",
)
STATS = {
    "items": 2, "references": 3, "confirmed_matches": 3,
    "doi_matches": 1, "url_matches": 1, "arxiv_matches": 1,
    "ambiguous_urls": 1, "invalid_arxiv": 0,
    "unmatched_dois": 0, "unmatched_urls": 0,
    "malformed_references": 0, "malformed_fields": 0,
    "filtered_self_citations": 0, "filtered_provenance": 0,
}
AGGREGATION = {
    "partitions_processed": 1, "valid_count": 2, "failed_count": 1, "total_citations": 3,
    "publisher_citations": 1, "crossref_citations": 0, "mined_citations": 2,
    "in_memory_partitions": 1, "external_partitions": 0, "reused_partitions": 0,
}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sha256(payload: dict) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())


def make_source_tree(root: Path) -> str:
    (root / "jobs").mkdir(parents=True)
    (root / "jobs" / "pipeline.py").write_text("# job\n")
    (root / "src").mkdir()
    (root / "src" / "main.rs").write_text("fn main() {}\n")
    (root / "Cargo.toml").write_text('[package]\nversion = "2.1.0"\n')
    (root / "Cargo.lock").write_text("# lock\n")
    return runtime_source_digest(root)


def entry(citer: str, ref_index: int, provenance: str, match_type: str, reference: dict) -> dict:
    return {
        "doi": citer,
        "ref_index": ref_index,
        "provenance": provenance,
        "match_type": match_type,
        "raw_match": next(iter(reference.values())),
        "reference": reference,
    }


def fixture_records(*, with_arxiv: bool = True) -> list[dict[str, Any]]:
    records = [
        {
            "doi": "10.1234/a",
            "cited_by": [
                entry("10.9999/w1", 0, "publisher", "doi", {"DOI": "10.1234/a"}),
                entry("10.9999/w1", 1, "mined", "url", {"URL": "https://example.org/a"}),
            ],
        }
    ]
    if with_arxiv:
        records.append(
            {
                "doi": ARXIV_TARGET,
                "cited_by": [
                    entry("10.9999/w2", 0, "mined", "arxiv", {"unstructured": "arXiv:2403.03542"}),
                ],
            }
        )
    return records


def fixture_ambiguity_rows() -> list[dict[str, Any]]:
    return [
        {
            "citing_doi": "10.9999/w1",
            "ref_index": 2,
            "url": "https://example.org/x",
            "raw_match": "example.org/x",
            "candidate_dois": ["10.1/a", "10.1/b"],
            "reference": {"unstructured": "see https://example.org/x"},
        }
    ]


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_fake_bundle(directory: Path, release: DataciteRelease) -> str:
    directory.mkdir(parents=True)
    files = {}
    for name in BUNDLE_FILES:
        data = f"{name}-bytes".encode()
        (directory / name).write_bytes(data)
        files[name] = {"size": len(data), "sha256": sha256(data)}
    manifest = {
        "kind": "datacite-index",
        "format_version": 2,
        "normalization_version": 1,
        "release": release.release,
        "source_inventory_sha256": release.inventory_sha256,
        "files": files,
        "counts": {"dois": 1, "urls": 1, "missing_urls": 0, "shared_urls": 0},
    }
    marker = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    (directory / BUNDLE_MARKER_NAME).write_bytes(marker)
    return sha256(marker)


def flip_last_byte(path: Path) -> None:
    data = path.read_bytes()
    path.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))


def tree_snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


class RecordingStore(LocalBucketStore):
    """A real local bucket that records marker uploads and can fail or corrupt
    a specific upload on demand."""

    def __init__(self, case: FullPipelineCase) -> None:
        super().__init__(mount_root=case.mount)
        self.case = case

    def put_json(self, payload: dict[str, Any], destination: Path) -> None:
        if destination.name == self.case.fail_json_name:
            raise BucketPersistenceError(f"injected upload failure: {destination.name}")
        super().put_json(payload, destination)
        self.case.uploaded_json.append(destination)
        kind = payload.get("kind")
        if kind is not None:
            self.case.persisted_marker_kinds.append(kind)

    def put_tree(self, source: Path, destination: Path, **kwargs: Any) -> None:
        super().put_tree(source, destination, **kwargs)
        self.case.uploaded_trees.append(destination)
        if self.case.corrupt_upload is not None:
            flip_last_byte(destination / self.case.corrupt_upload)

    def delete_tree(self, destination: Path) -> None:
        super().delete_tree(destination)
        self.case.deleted_trees.append(destination)


class FullPipelineCase:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.mount = tmp_path / "mnt"
        self.work = tmp_path / "work"
        self.source_root = tmp_path / "sources"
        self.mount.mkdir(exist_ok=True)
        self.digest = make_source_tree(self.source_root)
        self.paths = PipelinePaths.from_roots(
            self.mount, self.work, prefix_name=DATACITE_PREFIX_NAME
        )
        self.lines: list[str] = []
        self.calls: list[str] = []
        self.extraction_calls = 0
        self.extraction_kwargs: dict[str, Any] = {}
        self.bundle_calls = 0
        self.publication_calls: Counter[str] = Counter()
        self.verify_calls: list[str] = []
        self.persisted_marker_kinds: list[str] = []
        self.uploaded_json: list[Path] = []
        self.uploaded_trees: list[Path] = []
        self.deleted_trees: list[Path] = []
        self.tokens_seen: set[Any] = set()
        self.remote: dict[str, str] = {}
        # Failure injection.
        self.mirror_error: MirrorError | None = None
        self.fail_repo: str | None = None
        self.extraction_error: Exception | None = None
        self.omit_output: str | None = None
        self.report_identity: str | None = None
        self.bundle_error: Exception | None = None
        self.fail_json_name: str | None = None
        self.corrupt_upload: str | None = None
        self.records = fixture_records()
        self.ambiguity_rows = fixture_ambiguity_rows()

        mirror_archive = (
            self.mount / "crossref-affiliations-latest"
            / f"raw-generations/2026-07-{MIRROR_RUN}" / "all.json.tar.gz"
        )
        mirror_archive.parent.mkdir(parents=True)
        mirror_archive.write_bytes(ARCHIVE_BYTES)
        self.mirror_snapshot = ResolvedSnapshot(
            snapshot_id="2026-07",
            provenance="mirror",
            archive_sha256=sha256(ARCHIVE_BYTES),
            archive_size=len(ARCHIVE_BYTES),
            archive_path=mirror_archive,
            archive_is_local=False,
            source={
                "kind": "affiliations-mirror",
                "prefix": "crossref-affiliations-latest",
                "raw_prefix": f"raw-generations/2026-07-{MIRROR_RUN}",
            },
        )
        self.config = PipelineConfig(
            source_root=self.source_root,
            source_digest=self.digest,
            work_root=self.work,
            bucket_root=self.mount,
            smoke=False,
            force=False,
            datacite_release=None,
            hf_token=TOKEN,
            crossref_token=None,
            run_id=RUN,
            mode="datacite",
            resources=ResourceSettings(duckdb_memory_mb=256),
        )

    # -- durable state accessors ------------------------------------------

    def events(self) -> list[dict[str, Any]]:
        return [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in self.lines]

    def event_names(self) -> list[str]:
        return [event["event"] for event in self.events()]

    def extraction_marker(self) -> FullExtractionMarker:
        return FullExtractionMarker.from_dict(read_json(self.paths.extraction_marker))

    def pipeline_marker(self) -> FullPipelineMarker:
        return FullPipelineMarker.from_dict(read_json(self.paths.pipeline_marker))

    def derived_root(self) -> Path:
        return self.paths.derived_generation(self.extraction_marker().derived_prefix).root

    def journal(self) -> dict[str, Any]:
        return read_json(self.derived_root() / "_PUBLICATIONS.json")

    # -- fakes ----------------------------------------------------------------

    def _resolve_mirror(self, bucket_root: Path) -> ResolvedSnapshot:
        self.calls.append("resolve_mirror")
        assert bucket_root == self.mount
        if self.mirror_error is not None:
            raise self.mirror_error
        return self.mirror_snapshot

    def _fallback_transfer(self, **kwargs: Any) -> ResolvedSnapshot:
        self.calls.append("fallback_transfer")
        assert kwargs["credential"] == "crossref-token"
        assert kwargs["paths"].prefix == self.paths.prefix
        raw_prefix = f"raw-generations/2026-07-{RUN}"
        generation = self.paths.raw_generation(raw_prefix)
        generation.archive.parent.mkdir(parents=True, exist_ok=True)
        generation.archive.write_bytes(ARCHIVE_BYTES)
        local = self.paths.snapshot_local
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(ARCHIVE_BYTES)
        return ResolvedSnapshot(
            snapshot_id="2026-07",
            provenance="download",
            archive_sha256=sha256(ARCHIVE_BYTES),
            archive_size=len(ARCHIVE_BYTES),
            archive_path=local,
            archive_is_local=True,
            source={"kind": "own-transfer", "raw_prefix": raw_prefix, "reused": False},
        )

    def _build_extractor(self, source_root: Path, target_dir: Path, events: Any) -> ExtractorBinary:
        self.calls.append("build_extractor")
        return ExtractorBinary(path=self.work / "bin", version="2.1.0")

    def _ensure_bundle(self, **kwargs: Any) -> BundleResult:
        self.calls.append("ensure_bundle")
        self.bundle_calls += 1
        if self.bundle_error is not None:
            raise self.bundle_error
        release: DataciteRelease = kwargs["release"]
        directory = (
            self.paths.bundle_local
            / f"{release.release}-v2-{release.inventory_sha256}"
            / "generations"
            / ("c" * 32)
        )
        if directory.is_dir():
            manifest_sha256 = sha256((directory / BUNDLE_MARKER_NAME).read_bytes())
        else:
            manifest_sha256 = write_fake_bundle(directory, release)
        return BundleResult(
            directory=directory,
            release=release.release,
            inventory_sha256=release.inventory_sha256,
            manifest_sha256=manifest_sha256,
            reused=True,
        )

    def _run_extraction(self, binary: ExtractorBinary, **kwargs: Any) -> FullExtractionOutputs:
        self.calls.append("run_extraction")
        self.extraction_calls += 1
        self.extraction_kwargs = kwargs
        if self.extraction_error is not None:
            raise self.extraction_error
        identity = read_json(kwargs["identity_path"])
        bundle: Path = kwargs["bundle"]
        # What the Rust binary validates before touching the filesystem.
        assert identity["extraction_mode"] == "datacite"
        assert identity["partition_algorithm"] == "fnv1a64-v1"
        assert identity["partitions"] == kwargs["resources"].partitions
        assert identity["bundle_manifest_sha256"] == sha256(
            (bundle / BUNDLE_MARKER_NAME).read_bytes()
        )
        assert kwargs["archive"].is_file()
        output_dir: Path = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        kwargs["temp_dir"].mkdir(parents=True, exist_ok=True)
        names = {
            kind: output_dir / f"all.json_{kind}.jsonl"
            for kind in ("valid", "failed", "publisher", "crossref", "mined", "ambiguous_urls")
        }
        write_jsonl(names["valid"], self.records)
        write_jsonl(names["failed"], [{"doi": "10.48550/arxiv.9999.99999", "cited_by": []}])
        by_provenance = {"publisher": [], "crossref": [], "mined": []}
        for record in self.records:
            for provenance in by_provenance:
                cited_by = [c for c in record["cited_by"] if c["provenance"] == provenance]
                if cited_by:
                    by_provenance[provenance].append({**record, "cited_by": cited_by})
        for provenance, records in by_provenance.items():
            write_jsonl(names[provenance], records)
        write_jsonl(names["ambiguous_urls"], self.ambiguity_rows)
        if self.omit_output is not None:
            names[self.omit_output].unlink()
        report = {
            "kind": "datacite-extraction-report",
            "schema_version": 2,
            "identity_sha256": self.report_identity or canonical_sha256(identity),
            "completed": True,
            "files_processed": 1,
            "fatal_source_errors": 0,
            "stats": dict(STATS),
            "aggregation": dict(AGGREGATION),
        }
        report_path = output_dir / "extraction-report.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        return FullExtractionOutputs(**names, report=report_path)

    def _transform(self, valid: Path, ambiguous: Path, transform_dir: Path, events: Any, resources: ResourceSettings):
        self.calls.append("transform")
        return transform_datacite_outputs(valid, ambiguous, transform_dir, events, resources)

    def _prepare_dataset(self, *args: Any, **kwargs: Any):
        self.calls.append("prepare_dataset")
        return prepare_dataset(*args, **kwargs)

    def _publish_prepared(self, *, api: Any, token: Any, prepared: Any, events: Any) -> PublicationReceipt:
        kind = KINDS_BY_REPO[prepared.repo_id]
        self.calls.append(f"publish:{kind}")
        self.publication_calls[kind] += 1
        self.tokens_seen.add(token)
        if self.fail_repo == kind:
            raise PublishError(f"injected publication failure for {kind}")
        sha = hashlib.sha1(f"{kind}:{self.publication_calls[kind]}".encode()).hexdigest()
        self.remote[kind] = prepared.artifact_digest
        return PublicationReceipt(
            repo_id=prepared.repo_id,
            commit_sha=sha,
            commit_url=f"https://huggingface.co/datasets/{prepared.repo_id}/commit/{sha}",
            artifact_digest=prepared.artifact_digest,
        )

    def _verify_current(self, *, api: Any, token: Any, prepared: Any, receipt: Any) -> None:
        kind = KINDS_BY_REPO[prepared.repo_id]
        self.calls.append(f"verify:{kind}")
        self.verify_calls.append(kind)
        self.tokens_seen.add(token)
        if receipt.repo_id != prepared.repo_id:
            raise PublishError("receipt belongs to another repository")
        if receipt.artifact_digest != prepared.artifact_digest:
            raise PublishError(f"{prepared.repo_id}: receipt artifact differs from prepared artifact")
        if self.remote.get(kind) != receipt.artifact_digest:
            raise PublishError(f"concurrent change in {prepared.repo_id}")

    def dependencies(self) -> FullDependencies:
        def api_factory(token: Any) -> object:
            self.tokens_seen.add(token)
            return object()

        def store_factory(*, api: Any, token: Any, mount_root: Path) -> RecordingStore:
            self.tokens_seen.add(token)
            assert mount_root == self.mount
            return RecordingStore(self)

        return FullDependencies(
            events=EventLogger(output=self.lines.append),
            api_factory=api_factory,
            store_factory=store_factory,
            resolve_mirror=self._resolve_mirror,
            fallback_transfer=self._fallback_transfer,
            build_extractor=self._build_extractor,
            discover_release=discover_release,
            ensure_bundle=self._ensure_bundle,
            run_extraction=self._run_extraction,
            transform=self._transform,
            prepare_dataset=self._prepare_dataset,
            publish_pair=publish_pair,
            verify_current=self._verify_current,
            publish_prepared=self._publish_prepared,
        )


@pytest.fixture
def full_pipeline_case(tmp_path: Path, full_datacite_release: Path) -> FullPipelineCase:
    return FullPipelineCase(tmp_path)


# --- the brief's failure test -------------------------------------------------


def test_failed_second_publication_keeps_extraction_reusable(full_pipeline_case):
    case = full_pipeline_case
    case.fail_repo = "arxiv"
    with pytest.raises(RuntimeError, match="publication"):
        run_full_pipeline(case.config, case.dependencies())
    assert "datacite-extraction" in case.persisted_marker_kinds
    assert "datacite-pipeline" not in case.persisted_marker_kinds
    case.fail_repo = None
    run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 1
    assert case.publication_calls["datacite"] == 1
    assert case.publication_calls["arxiv"] == 2


# --- happy paths ---------------------------------------------------------------


def test_full_pipeline_completes_via_mirror(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    result = run_full_pipeline(case.config, case.dependencies())

    assert result["status"] == "completed"
    assert result["snapshot_id"] == "2026-07"
    assert result["datacite_release"] == "2026-07"
    assert result["derived_prefix"] == f"derived-generations/2026-07-{RUN}"
    assert result["source_tree_sha256"] == case.digest
    assert result["extraction_reused"] is False
    assert result["dataset_urls"] == {
        "datacite": "https://huggingface.co/datasets/cometadata/crossref-datacite-citations",
        "arxiv": "https://huggingface.co/datasets/cometadata/crossref-arxiv-citations",
    }
    assert set(result["publications"]) == {"datacite", "arxiv"}
    assert result["counts"]["datacite"]["all"] == {"works": 2, "citations": 2, "references": 3}
    assert result["counts"]["datacite"]["mined"] == {"works": 2, "citations": 2, "references": 2}
    assert result["counts"]["arxiv"]["all"] == {"works": 1, "citations": 1, "references": 1}
    assert result["counts"]["ambiguous_urls"] == 1
    assert set(result) == {
        "status", "snapshot_id", "datacite_release", "derived_prefix", "identity_sha256",
        "source_tree_sha256", "extraction_reused", "dataset_urls", "publications", "counts",
    }

    # Phase order: build, resolve, bundle, extract, transform, prepare, publish.
    order = [name for name in case.calls if not name.startswith("verify:")]
    assert order == [
        "build_extractor", "resolve_mirror", "ensure_bundle", "run_extraction",
        "transform", "prepare_dataset", "prepare_dataset", "publish:datacite", "publish:arxiv",
    ]
    assert case.persisted_marker_kinds == [FULL_EXTRACTION_KIND, FULL_PIPELINE_KIND]

    # The Rust invocation received the staged local archive and the bundle.
    kwargs = case.extraction_kwargs
    assert kwargs["archive"] == case.paths.snapshot_local
    assert kwargs["bundle"].name == "c" * 32
    assert kwargs["identity_path"] == case.paths.identity_local
    assert kwargs["output_dir"] == case.paths.outputs_dir
    assert kwargs["temp_dir"] == case.paths.partitions
    assert kwargs["resources"] == case.config.resources
    # The staged archive is released after extraction.
    assert not case.paths.snapshot_local.exists()

    # Durable markers are real files with the run identity.
    extraction = case.extraction_marker()
    identity = extraction.identity
    assert identity.archive_sha256 == sha256(ARCHIVE_BYTES)
    assert identity.datacite_release == "2026-07"
    assert identity.source_tree_sha256 == case.digest
    assert identity.bundle_manifest_sha256 == sha256(
        (kwargs["bundle"] / BUNDLE_MARKER_NAME).read_bytes()
    )
    assert identity.fingerprint() == result["identity_sha256"]
    assert extraction.snapshot_provenance == "mirror"
    assert extraction.derived_prefix == result["derived_prefix"]
    assert extraction.counts == result["counts"]
    pipeline = case.pipeline_marker()
    assert pipeline.identity == identity
    assert pipeline.derived_prefix == result["derived_prefix"]
    assert {kind: receipt.to_dict() for kind, receipt in pipeline.publications.items()} == result["publications"]

    # The derived generation holds both prepared subtrees, the compressed raw
    # outputs, the report and the journal; the marker lists every file.
    derived = case.derived_root()
    present = tree_snapshot(derived)
    for name in (
        "datacite/README.md", "datacite/metadata/pipeline-manifest.json",
        "datacite/data/all/part-00000.parquet", "datacite/data/asserted/part-00000.parquet",
        "datacite/data/mined/part-00000.parquet", "datacite/data/ambiguous_urls/part-00000.parquet",
        "arxiv/README.md", "arxiv/metadata/pipeline-manifest.json",
        "arxiv/data/all.parquet", "arxiv/data/asserted.parquet", "arxiv/data/mined.parquet",
        *RAW_OUTPUT_NAMES, "_PUBLICATIONS.json", "_EXTRACTION_COMPLETE.json",
    ):
        assert name in present, name
    assert set(extraction.files) == set(present) - {"_PUBLICATIONS.json", "_EXTRACTION_COMPLETE.json"}
    for name, meta in extraction.files.items():
        assert meta.size == len(present[name]), name
        assert meta.sha256 == sha256(present[name]), name
    assert case.journal() == result["publications"]
    for kind in ("datacite", "arxiv"):
        manifest = json.loads(present[f"{kind}/{MANIFEST_PATH}"])
        assert manifest["identity"] == identity.to_dict()
        assert result["publications"][kind]["artifact_digest"] == sha256(present[f"{kind}/{MANIFEST_PATH}"])

    # Scratch is bounded: raw outputs and transform scratch are gone.
    assert not case.paths.outputs_dir.exists()
    assert not case.paths.transform_dir.exists()

    # Credentials reach only the API/store/publication calls.
    assert case.tokens_seen == {TOKEN}
    for line in case.lines:
        assert TOKEN not in line
    for name in ("_EXTRACTION_COMPLETE.json", "_PIPELINE_COMPLETE.json"):
        assert TOKEN not in (case.paths.prefix / name).read_text()
    assert TOKEN not in json.dumps(present["_PUBLICATIONS.json"].decode())


def test_fallback_source_reuses_the_verified_local_archive(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.mirror_error = MirrorError("gone")
    case.config = replace(case.config, crossref_token="crossref-token")
    result = run_full_pipeline(case.config, case.dependencies())
    assert "fallback_transfer" in case.calls
    assert result["status"] == "completed"
    assert case.extraction_kwargs["archive"] == case.paths.snapshot_local
    assert "stage_snapshot" not in [e.get("phase") for e in case.events()]
    assert case.extraction_marker().snapshot_provenance == "download"
    # The fallback's raw generation survives cleanup; the released archive is local only.
    assert case.paths.raw_generation(f"raw-generations/2026-07-{RUN}").archive.is_file()
    assert not case.paths.snapshot_local.exists()


def test_mirror_unavailable_without_token_fails_before_any_work(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.mirror_error = MirrorError("gone")
    with pytest.raises(PipelineError, match="CROSSREF_API_TOKEN"):
        run_full_pipeline(case.config, case.dependencies())
    assert "fallback_transfer" not in case.calls
    assert case.bundle_calls == 0 and case.extraction_calls == 0


# --- input identity guards -----------------------------------------------------


def test_expected_snapshot_mismatch_fails_before_bundle_and_extraction(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.config = replace(case.config, expected_snapshot_id="2026-06")
    with pytest.raises(PipelineError, match="2026-06.*2026-07|2026-07.*2026-06"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.bundle_calls == 0
    assert case.extraction_calls == 0
    assert not case.paths.snapshot_local.exists()


def test_expected_snapshot_match_proceeds(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    case.config = replace(case.config, expected_snapshot_id="2026-07")
    assert run_full_pipeline(case.config, case.dependencies())["status"] == "completed"


def test_pinned_release_must_be_the_selected_release(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.config = replace(case.config, datacite_release="2026-06")
    with pytest.raises(DataciteIndexError, match="2026-06"):
        run_full_pipeline(case.config, case.dependencies())
    # A discover_release that ignores the pin is rejected by the orchestrator.
    deps = case.dependencies()
    deps = replace(
        deps, discover_release=lambda bucket_root, pinned=None: discover_release(bucket_root)
    )
    with pytest.raises(PipelineError, match="pinned.*2026-06"):
        run_full_pipeline(case.config, deps)
    assert case.bundle_calls == 0 and case.extraction_calls == 0


def test_staged_archive_digest_mismatch_fails(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    case.mirror_snapshot = replace(case.mirror_snapshot, archive_sha256="0" * 64)
    with pytest.raises(PipelineError, match="digest"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 0
    assert case.persisted_marker_kinds == []


def test_bundle_corruption_fails_before_extraction(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.bundle_error = DataciteIndexError("bundle checksum mismatch: dois.fst")
    with pytest.raises(DataciteIndexError, match="dois.fst"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 0
    assert not case.paths.snapshot_local.exists()
    assert case.persisted_marker_kinds == []


# --- extraction and transform failures ----------------------------------------


def test_extraction_failure_persists_nothing(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    case.extraction_error = RuntimeError("extractor exited 1")
    with pytest.raises(RuntimeError, match="exited 1"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.persisted_marker_kinds == []
    assert not case.paths.extraction_marker.exists()
    assert "transform" not in case.calls
    failed = [e for e in case.events() if e["event"] == "phase_failed"]
    assert failed and failed[-1]["error_type"] == "RuntimeError"


def test_missing_extraction_output_fails_without_touching(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.omit_output = "mined"
    with pytest.raises(PipelineError, match="all.json_mined.jsonl"):
        run_full_pipeline(case.config, case.dependencies())
    assert not (case.paths.outputs_dir / "all.json_mined.jsonl").exists()
    assert "transform" not in case.calls
    assert case.persisted_marker_kinds == []


def test_report_identity_mismatch_fails(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    case.report_identity = "0" * 64
    with pytest.raises(RuntimeError, match="identity"):
        run_full_pipeline(case.config, case.dependencies())
    assert "transform" not in case.calls
    assert case.persisted_marker_kinds == []


def test_empty_required_all_fails_before_persistence_and_publication(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.records = fixture_records(with_arxiv=False)
    with pytest.raises(PipelineError, match="arxiv.*all"):
        run_full_pipeline(case.config, case.dependencies())
    assert "transform" in case.calls
    assert "prepare_dataset" not in case.calls
    assert case.publication_calls == Counter()
    assert case.persisted_marker_kinds == []


# --- durable generation verification -----------------------------------------


def test_upload_corruption_prevents_the_extraction_marker(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.corrupt_upload = "arxiv/data/all.parquet"
    with pytest.raises(PipelineError, match="arxiv/data/all.parquet"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.uploaded_trees, "the generation was uploaded before verification"
    assert case.persisted_marker_kinds == []
    assert case.publication_calls == Counter()


@pytest.mark.parametrize(
    ("name", "mutate"),
    [
        ("datacite/data/all/part-00000.parquet", flip_last_byte),
        ("arxiv/data/all.parquet", lambda path: path.write_bytes(path.read_bytes()[:-1])),
        ("datacite/README.md", flip_last_byte),
        ("arxiv/metadata/pipeline-manifest.json", flip_last_byte),
        ("datacite/data/mined/part-00000.parquet", Path.unlink),
    ],
)
def test_reuse_verifies_every_derived_file(
    full_pipeline_case: FullPipelineCase, name: str, mutate
) -> None:
    case = full_pipeline_case
    run_full_pipeline(case.config, case.dependencies())
    # Remove the activation so the next run must reuse the extraction.
    case.paths.pipeline_marker.unlink()
    mutate(case.derived_root() / name)
    case.calls.clear()
    with pytest.raises(PipelineError, match=name.replace(".", r"\.")):
        run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 1
    assert case.publication_calls == Counter({"datacite": 1, "arxiv": 1})
    assert "publish:datacite" not in case.calls and "publish:arxiv" not in case.calls
    assert not case.paths.pipeline_marker.exists()


def test_stale_extraction_marker_is_recomputed(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    run_full_pipeline(case.config, case.dependencies())
    case.paths.pipeline_marker.unlink()
    payload = read_json(case.paths.extraction_marker)
    payload["identity"]["archive_sha256"] = "0" * 64
    case.paths.extraction_marker.write_text(json.dumps(payload))
    case.config = replace(case.config, run_id="a" * 32)
    result = run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 2
    assert result["extraction_reused"] is False
    assert result["derived_prefix"] == "derived-generations/2026-07-" + "a" * 32
    assert "extraction_marker_stale" in case.event_names()


def test_force_recomputes_and_still_verifies(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    first = run_full_pipeline(case.config, case.dependencies())
    case.config = replace(case.config, force=True, run_id="a" * 32)
    case.persisted_marker_kinds.clear()
    second = run_full_pipeline(case.config, case.dependencies())
    assert case.extraction_calls == 2
    assert second["status"] == "completed"
    assert second["extraction_reused"] is False
    assert second["derived_prefix"] != first["derived_prefix"]
    assert second["identity_sha256"] == first["identity_sha256"]
    assert case.persisted_marker_kinds == [FULL_EXTRACTION_KIND, FULL_PIPELINE_KIND]
    assert "verify_derived" in [e.get("phase") for e in case.events()]
    # The prior generation is removed only after the new activation.
    assert not (case.paths.prefix / first["derived_prefix"]).exists()
    assert (case.paths.prefix / second["derived_prefix"]).is_dir()
    assert case.pipeline_marker().derived_prefix == second["derived_prefix"]
    # Force with upload corruption still fails verification.
    case.config = replace(case.config, run_id="b" * 32)
    case.corrupt_upload = "datacite/data/all/part-00000.parquet"
    with pytest.raises(PipelineError, match="datacite/data/all/part-00000.parquet"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.pipeline_marker().derived_prefix == second["derived_prefix"]


# --- publication state ----------------------------------------------------------


def test_receipt_write_failure_is_recoverable(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    case.fail_json_name = "_PUBLICATIONS.json"
    with pytest.raises(RuntimeError, match="_PUBLICATIONS.json"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.publication_calls == Counter({"datacite": 1})
    assert case.persisted_marker_kinds == [FULL_EXTRACTION_KIND]
    assert not case.paths.pipeline_marker.exists()
    assert not (case.derived_root() / "_PUBLICATIONS.json").exists()
    case.fail_json_name = None
    result = run_full_pipeline(case.config, case.dependencies())
    assert result["status"] == "completed"
    assert case.extraction_calls == 1
    # Without a journaled receipt the destination is published (recovered) again.
    assert case.publication_calls == Counter({"datacite": 2, "arxiv": 1})
    assert case.journal() == result["publications"]


def test_partial_journal_is_read_from_the_bucket_on_retry(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    case.fail_repo = "arxiv"
    with pytest.raises(PublishError):
        run_full_pipeline(case.config, case.dependencies())
    journal = case.journal()
    assert set(journal) == {"datacite"}
    assert journal["datacite"]["artifact_digest"] == case.remote["datacite"]
    # A concurrent change to the DataCite repository blocks the retry.
    case.fail_repo = None
    case.remote["datacite"] = "0" * 64
    with pytest.raises(PublishError, match="concurrent change"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.publication_calls == Counter({"datacite": 1, "arxiv": 1})
    assert not case.paths.pipeline_marker.exists()


def test_noop_requires_both_current_publications_to_verify(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    first = run_full_pipeline(case.config, case.dependencies())
    marker_before = case.paths.pipeline_marker.read_bytes()
    case.calls.clear()
    case.verify_calls.clear()

    second = run_full_pipeline(case.config, case.dependencies())
    assert second["status"] == "no-op"
    assert second["publications"] == first["publications"]
    assert second["counts"] == first["counts"]
    assert second["derived_prefix"] == first["derived_prefix"]
    assert second["identity_sha256"] == first["identity_sha256"]
    assert second["extraction_reused"] is True
    assert case.verify_calls == ["datacite", "arxiv"]
    assert case.extraction_calls == 1
    assert case.publication_calls == Counter({"datacite": 1, "arxiv": 1})
    assert "run_extraction" not in case.calls and "transform" not in case.calls
    assert case.paths.pipeline_marker.read_bytes() == marker_before

    # A repository that no longer publishes the activated artifact is a
    # concurrent change, never a silent re-publication or a success.
    case.remote["arxiv"] = "0" * 64
    with pytest.raises(PipelineError, match="concurrent change"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.publication_calls == Counter({"datacite": 1, "arxiv": 1})
    assert case.paths.pipeline_marker.read_bytes() == marker_before


def test_noop_verifies_the_persisted_datasets_by_digest(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    run_full_pipeline(case.config, case.dependencies())
    marker_before = case.paths.pipeline_marker.read_bytes()
    flip_last_byte(case.derived_root() / "datacite/data/all/part-00000.parquet")
    case.verify_calls.clear()
    with pytest.raises(PipelineError, match="datacite/data/all/part-00000.parquet"):
        run_full_pipeline(case.config, case.dependencies())
    assert case.verify_calls == []
    assert case.publication_calls == Counter({"datacite": 1, "arxiv": 1})
    assert case.paths.pipeline_marker.read_bytes() == marker_before


def test_publication_failure_leaves_prior_activation_intact(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    first = run_full_pipeline(case.config, case.dependencies())
    marker_before = case.paths.pipeline_marker.read_bytes()
    case.config = replace(case.config, force=True, run_id="a" * 32)
    case.fail_repo = "datacite"
    with pytest.raises(PublishError):
        run_full_pipeline(case.config, case.dependencies())
    assert case.paths.pipeline_marker.read_bytes() == marker_before
    assert (case.paths.prefix / first["derived_prefix"]).is_dir()
    assert (case.paths.prefix / ("derived-generations/2026-07-" + "a" * 32)).is_dir()


# --- activation and cleanup ----------------------------------------------------


def test_cleanup_removes_only_prior_generations_under_the_owned_prefix(
    full_pipeline_case: FullPipelineCase,
) -> None:
    case = full_pipeline_case
    prefix = case.paths.prefix
    for relative in (
        f"derived-generations/2026-06-{OLD_RUN}/data/all.parquet",
        f"raw-generations/2026-06-{OLD_RUN}/all.json.tar.gz",
        f"indexes/2026-07-v2-{'e' * 64}/generations/{'f' * 32}/dois.fst",
    ):
        path = prefix / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"old")
    arxiv_prefix = case.mount / "crossref-arxiv-citations-latest"
    for relative in (
        f"derived-generations/2026-06-{OLD_RUN}/data/all.parquet",
        f"raw-generations/2026-06-{OLD_RUN}/all.json.tar.gz",
        "_PIPELINE_COMPLETE.json",
        "_EXTRACTION_COMPLETE.json",
    ):
        path = arxiv_prefix / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"legacy")
    arxiv_before = tree_snapshot(arxiv_prefix)
    affiliations_before = tree_snapshot(case.mount / "crossref-affiliations-latest")

    result = run_full_pipeline(case.config, case.dependencies())

    assert not (prefix / f"derived-generations/2026-06-{OLD_RUN}").exists()
    assert not (prefix / f"raw-generations/2026-06-{OLD_RUN}").exists()
    assert (prefix / result["derived_prefix"]).is_dir()
    assert (prefix / f"indexes/2026-07-v2-{'e' * 64}/generations/{'f' * 32}/dois.fst").is_file()
    assert tree_snapshot(arxiv_prefix) == arxiv_before
    assert tree_snapshot(case.mount / "crossref-affiliations-latest") == affiliations_before
    assert sorted(path.name for path in case.deleted_trees) == [f"2026-06-{OLD_RUN}"] * 2
    # Cleanup ran only after the activation marker was written.
    events = case.events()
    activated = next(
        index
        for index, event in enumerate(events)
        if event["event"] == "phase_completed" and event.get("phase") == "activate"
    )
    removals = [index for index, event in enumerate(events) if event["event"] == "cleanup_removed"]
    assert len(removals) == 2 and min(removals) > activated


def test_cleanup_failure_does_not_undo_activation(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    stale = case.paths.prefix / f"derived-generations/2026-06-{OLD_RUN}"
    stale.mkdir(parents=True)
    (stale / "data.parquet").write_bytes(b"old")
    deps = case.dependencies()

    class FailingCleanupStore(RecordingStore):
        def delete_tree(self, destination: Path) -> None:
            raise BucketPersistenceError("listing failed")

    deps = replace(deps, store_factory=lambda **kwargs: FailingCleanupStore(case))
    result = run_full_pipeline(case.config, deps)
    assert result["status"] == "completed"
    assert case.pipeline_marker().derived_prefix == result["derived_prefix"]
    assert "cleanup_failed" in case.event_names()


# --- configuration and wiring --------------------------------------------------


def test_pipeline_config_validates_mode_and_keeps_legacy_defaults(tmp_path: Path) -> None:
    config = PipelineConfig(
        source_root=tmp_path, source_digest="a" * 64, work_root=tmp_path, bucket_root=None,
        smoke=False, force=False, datacite_release=None, hf_token=None, crossref_token=None,
        run_id=RUN,
    )
    assert config.mode == "legacy"
    assert config.resources == ResourceSettings()
    assert config.expected_snapshot_id is None
    with pytest.raises(ValueError, match="mode"):
        replace(config, mode="bogus")
    deps = FullPipelineCase(tmp_path).dependencies()
    for mode in ("legacy", "smoke", "benchmark"):
        with pytest.raises(PipelineError, match="datacite"):
            run_full_pipeline(replace(config, mode=mode), deps)


def test_full_pipeline_requires_bucket_and_token(full_pipeline_case: FullPipelineCase) -> None:
    case = full_pipeline_case
    with pytest.raises(PipelineError, match="bucket-root"):
        run_full_pipeline(replace(case.config, bucket_root=None), case.dependencies())
    with pytest.raises(PipelineError, match="HF_TOKEN"):
        run_full_pipeline(replace(case.config, hf_token=None), case.dependencies())
    with pytest.raises(PipelineError, match="digest"):
        run_full_pipeline(replace(case.config, source_digest="0" * 64), case.dependencies())
    assert case.calls == []


def test_default_dependencies_wire_the_real_functions() -> None:
    from citations_pipeline import (
        datacite_index,
        datacite_publish,
        extractor,
        mirror,
        paired_publish,
        transfer,
    )

    deps = default_full_dependencies()
    assert deps.resolve_mirror is mirror.resolve_mirror_snapshot
    assert deps.fallback_transfer is transfer.fallback_download_snapshot
    assert deps.build_extractor is extractor.build_extractor
    assert deps.discover_release is datacite_index.discover_release
    assert deps.ensure_bundle is datacite_index.ensure_bundle
    assert deps.run_extraction is extractor.run_datacite_extraction
    assert deps.transform is datacite_publish.transform_datacite_outputs
    assert deps.prepare_dataset is paired_publish.prepare_dataset
    assert deps.publish_pair is paired_publish.publish_pair
    assert deps.verify_current is paired_publish.verify_current
    assert deps.publish_prepared is paired_publish.publish_prepared
    assert isinstance(deps.events, EventLogger)


def test_legacy_helpers_remain_importable_under_their_old_names() -> None:
    from citations_pipeline import pipeline, pipeline_io

    assert pipeline.PipelineError is pipeline_io.PipelineError
    assert pipeline._compress_file is pipeline_io.compress_file
    assert pipeline._count_lines is pipeline_io.count_lines
    assert callable(pipeline._stage_snapshot)
