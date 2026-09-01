import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from citations_pipeline.arxiv_index import DataciteRelease, FstResult
from citations_pipeline.extractor import ExtractionOutputs, ExtractorBinary
from citations_pipeline.mirror import MirrorError, ResolvedSnapshot
from citations_pipeline.paths import PipelinePaths, runtime_source_digest
from citations_pipeline.pipeline import (
    PipelineConfig,
    PipelineDependencies,
    PipelineError,
    run_pipeline,
)
from citations_pipeline.publish import (
    ConfigStats,
    PublicationResult,
    PublishError,
    TransformResult,
)
from citations_pipeline.telemetry import EventLogger

RUN = "9" * 32
STATS = {
    "all": ConfigStats(works=2, citations=3, references=4),
    "asserted": ConfigStats(works=1, citations=2, references=2),
    "mined": ConfigStats(works=2, citations=2, references=2),
}


def make_source_tree(root: Path) -> str:
    (root / "jobs").mkdir(parents=True)
    (root / "jobs" / "pipeline.py").write_text("# job\n")
    (root / "src").mkdir()
    (root / "src" / "main.rs").write_text("fn main() {}\n")
    (root / "Cargo.toml").write_text('[package]\nversion = "2.1.0"\n')
    (root / "Cargo.lock").write_text("# lock\n")
    return runtime_source_digest(root)


class FakeStore:
    def __init__(self) -> None:
        self.files: list[tuple[Path, Path]] = []
        self.jsons: list[tuple[dict, Path]] = []
        self.trees: list[tuple[Path, Path]] = []
        self.deleted_trees: list[Path] = []

    def put_file(self, source, destination, **kwargs) -> None:
        self.files.append((source, destination))

    def put_json(self, payload, destination) -> None:
        self.jsons.append((payload, destination))

    def put_tree(self, source, destination, **kwargs) -> None:
        self.trees.append((source, destination))

    def delete(self, destination) -> None:  # pragma: no cover - unused
        pass

    def delete_tree(self, destination) -> None:
        self.deleted_trees.append(destination)


class FakeApi:
    def __init__(self) -> None:
        self.known_revisions: set[str] = set()

    def dataset_info(self, repo_id, revision=None, token=None):
        if revision is not None and revision not in self.known_revisions:
            raise RuntimeError("unknown revision")
        return SimpleNamespace(sha=revision or "tip")


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.mount = tmp_path / "mnt"
        self.work = tmp_path / "work"
        self.source_root = tmp_path / "sources"
        self.mount.mkdir()
        self.digest = make_source_tree(self.source_root)
        self.paths = PipelinePaths.from_roots(self.mount, self.work)
        self.store = FakeStore()
        self.api = FakeApi()
        self.calls: list[str] = []
        self.mirror_error: MirrorError | None = None

        mirror_archive = (
            self.mount / "crossref-affiliations-latest"
            / f"raw-generations/2026-07-{RUN}" / "all.json.tar.gz"
        )
        mirror_archive.parent.mkdir(parents=True)
        mirror_archive.write_bytes(b"tar!!")
        self.mirror_snapshot = ResolvedSnapshot(
            snapshot_id="2026-07",
            provenance="mirror",
            archive_sha256="a" * 64,
            archive_size=5,
            archive_path=mirror_archive,
            archive_is_local=False,
            source={"kind": "affiliations-mirror", "raw_prefix": f"raw-generations/2026-07-{RUN}"},
        )

    def config(self, **overrides) -> PipelineConfig:
        values = {
            "source_root": self.source_root,
            "source_digest": self.digest,
            "work_root": self.work,
            "bucket_root": self.mount,
            "smoke": False,
            "force": False,
            "datacite_release": None,
            "hf_token": "hf-token",
            "crossref_token": None,
            "run_id": RUN,
        }
        values.update(overrides)
        return PipelineConfig(**values)

    def deps(self) -> PipelineDependencies:
        harness = self

        def resolve_mirror(bucket_root):
            harness.calls.append("resolve_mirror")
            if harness.mirror_error is not None:
                raise harness.mirror_error
            return harness.mirror_snapshot

        def fallback_transfer(**kwargs):
            harness.calls.append("fallback_transfer")
            local = harness.paths.snapshot_local
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(b"tar!!")
            return ResolvedSnapshot(
                snapshot_id="2026-07",
                provenance="download",
                archive_sha256="a" * 64,
                archive_size=5,
                archive_path=local,
                archive_is_local=True,
                source={"kind": "own-transfer", "raw_prefix": f"raw-generations/2026-07-{RUN}", "reused": False},
            )

        def build_extractor(source_root, target_dir, events, **kwargs):
            harness.calls.append("build_extractor")
            return ExtractorBinary(path=harness.work / "bin", version="2.1.0")

        def run_extraction(binary, *, archive, fst, output_dir, temp_dir, events, **kwargs):
            harness.calls.append("run_extraction")
            output_dir.mkdir(parents=True, exist_ok=True)
            outputs = {}
            for kind in ("valid", "failed", "publisher", "crossref", "mined"):
                path = output_dir / f"all.json_{kind}.jsonl"
                path.write_text('{"doi": "x"}\n' if kind in {"valid", "failed"} else "")
                outputs[kind] = path
            return ExtractionOutputs(**outputs)

        def transform(valid_path, transform_dir, events):
            harness.calls.append("transform")
            transform_dir.mkdir(parents=True, exist_ok=True)
            parquet = {}
            for name in ("all", "asserted", "mined"):
                path = transform_dir / f"{name}.parquet"
                path.write_bytes(b"pq-" + name.encode())
                parquet[name] = path
            return TransformResult(
                parquet=parquet,
                stats=STATS,
                reference_keys={name: ("key",) for name in ("all", "asserted", "mined")},
                dropped_reference_keys=(),
            )

        def publish(**kwargs):
            harness.calls.append("publish")
            harness.publish_kwargs = kwargs
            harness.api.known_revisions.add("c" * 40)
            return PublicationResult(
                commit_sha="c" * 40,
                commit_url="https://huggingface.co/datasets/x/commit/c",
            )

        return PipelineDependencies(
            events=EventLogger(),
            api_factory=lambda token: harness.api,
            store_factory=lambda **kwargs: harness.store,
            resolve_mirror=resolve_mirror,
            fallback_transfer=fallback_transfer,
            build_extractor=build_extractor,
            run_build_index=lambda binary, records, output: harness.calls.append("run_build_index"),
            run_extraction=run_extraction,
            discover_release=lambda bucket_root, pinned=None: DataciteRelease(
                release="2026-07", dois_path=harness.mount / "datacite" / "dois"
            ),
            ensure_fst=self._ensure_fst,
            transform=transform,
            render_card=lambda stats, **kwargs: "card",
            publish=publish,
        )

    def _ensure_fst(self, **kwargs):
        self.calls.append("ensure_fst")
        local = self.paths.fst_local
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(b"fst")
        return FstResult(
            local_path=local, sha256="b" * 64, size=3, release="2026-07", reused=True
        )


def marker_payloads(store: FakeStore, name: str) -> list[dict]:
    return [
        payload
        for payload, destination in store.jsons
        if destination.name == name
    ]


def test_production_completes_via_mirror(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    result = run_pipeline(harness.config(), harness.deps())
    assert result.status == "completed"
    assert result.snapshot_provenance == "mirror"
    assert result.extraction_reused is False
    assert result.dataset_commit == "c" * 40
    assert result.counts["all"] == asdict(STATS["all"])
    assert "run_extraction" in harness.calls
    assert harness.calls.index("build_extractor") < harness.calls.index("run_extraction")

    extraction_markers = marker_payloads(harness.store, "_EXTRACTION_COMPLETE.json")
    assert extraction_markers, "root extraction marker must be written"
    assert extraction_markers[0]["archive_sha256"] == "a" * 64
    pipeline_markers = marker_payloads(harness.store, "_PIPELINE_COMPLETE.json")
    assert pipeline_markers[0]["dataset_commit"] == "c" * 40
    assert pipeline_markers[0]["raw_prefix"] is None  # mirror provenance
    # derived generation uploaded before markers
    assert harness.store.trees and harness.store.trees[0][1].name.startswith("2026-07-")


def test_source_digest_mismatch_fails(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    with pytest.raises(PipelineError, match="digest"):
        run_pipeline(harness.config(source_digest="0" * 64), harness.deps())


def test_mirror_unavailable_without_token_fails(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.mirror_error = MirrorError("gone")
    with pytest.raises(PipelineError, match="CROSSREF_API_TOKEN"):
        run_pipeline(harness.config(), harness.deps())
    assert "fallback_transfer" not in harness.calls


def test_mirror_unavailable_with_token_uses_fallback(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.mirror_error = MirrorError("gone")
    result = run_pipeline(
        harness.config(crossref_token="crossref-token"), harness.deps()
    )
    assert "fallback_transfer" in harness.calls
    assert result.snapshot_provenance == "download"
    pipeline_markers = marker_payloads(harness.store, "_PIPELINE_COMPLETE.json")
    assert pipeline_markers[0]["raw_prefix"] == f"raw-generations/2026-07-{RUN}"


def test_extraction_reuse_skips_extraction(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    derived_prefix = f"derived-generations/2026-07-{RUN}"
    derived_root = harness.paths.prefix / derived_prefix
    files = {}
    for name in ("all", "asserted", "mined"):
        path = derived_root / "data" / f"{name}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"pq-" + name.encode())
        files[f"data/{name}.parquet"] = {"size": path.stat().st_size, "sha256": "d" * 64}
    harness.paths.prefix.mkdir(parents=True, exist_ok=True)
    harness.paths.extraction_marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "extraction_schema_version": 1,
                "snapshot_id": "2026-07",
                "snapshot_provenance": "mirror",
                "archive_sha256": "a" * 64,
                "datacite_release": "2026-07",
                "fst_sha256": "b" * 64,
                "source_tree_sha256": harness.digest,
                "extractor_version": "2.1.0",
                "derived_prefix": derived_prefix,
                "files": files,
                "counts": {name: asdict(stat) for name, stat in STATS.items()},
                "completed_at": "2026-08-31T00:00:00+00:00",
            }
        )
    )
    result = run_pipeline(harness.config(), harness.deps())
    assert result.status == "completed"
    assert result.extraction_reused is True
    assert "run_extraction" not in harness.calls
    assert "transform" not in harness.calls
    # publish received local copies of the reused parquet files
    published = harness.publish_kwargs["parquet"]
    assert published["all"].read_bytes() == b"pq-all"


def test_force_reruns_despite_reusable_marker(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.paths.prefix.mkdir(parents=True, exist_ok=True)
    harness.paths.extraction_marker.write_text("{}")  # invalid is fine; force skips it
    result = run_pipeline(harness.config(force=True), harness.deps())
    assert "run_extraction" in harness.calls
    assert result.extraction_reused is False


def test_noop_when_pipeline_marker_current(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    harness.api.known_revisions.add("e" * 40)
    harness.paths.prefix.mkdir(parents=True, exist_ok=True)
    harness.paths.pipeline_marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_id": "2026-07",
                "snapshot_provenance": "mirror",
                "archive_sha256": "a" * 64,
                "datacite_release": "2026-07",
                "fst_sha256": "b" * 64,
                "source_tree_sha256": harness.digest,
                "derived_prefix": f"derived-generations/2026-07-{RUN}",
                "raw_prefix": None,
                "dataset_repo": "cometadata/crossref-arxiv-citations",
                "dataset_commit": "e" * 40,
                "dataset_commit_url": "https://example.org/commit/e",
                "completed_at": "2026-08-31T00:00:00+00:00",
            }
        )
    )
    result = run_pipeline(harness.config(), harness.deps())
    assert result.status == "no-op"
    assert "run_extraction" not in harness.calls
    assert "publish" not in harness.calls


def test_publish_failure_leaves_no_pipeline_marker(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    deps = harness.deps()

    def failing_publish(**kwargs):
        raise PublishError("dataset commit failed")

    deps = PipelineDependencies(**{**deps.__dict__, "publish": failing_publish})
    with pytest.raises(PublishError):
        run_pipeline(harness.config(), deps)
    assert marker_payloads(harness.store, "_PIPELINE_COMPLETE.json") == []


def test_cleanup_removes_prior_generation(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    old_prefix = "derived-generations/2026-06-" + "8" * 32
    harness.paths.prefix.mkdir(parents=True, exist_ok=True)
    harness.paths.pipeline_marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_id": "2026-06",
                "snapshot_provenance": "mirror",
                "archive_sha256": "f" * 64,
                "datacite_release": "2026-06",
                "fst_sha256": "f" * 64,
                "source_tree_sha256": "f" * 64,
                "derived_prefix": old_prefix,
                "raw_prefix": None,
                "dataset_repo": "cometadata/crossref-arxiv-citations",
                "dataset_commit": "e" * 40,
                "dataset_commit_url": "https://example.org/commit/e",
                "completed_at": "2026-07-31T00:00:00+00:00",
            }
        )
    )
    run_pipeline(harness.config(), harness.deps())
    assert harness.store.deleted_trees == [harness.paths.prefix / old_prefix]


def test_smoke_flow(tmp_path: Path) -> None:
    from citations_pipeline.smoke import SMOKE_EXPECTED

    harness = Harness(tmp_path)
    smoke_stats = {
        name: ConfigStats(**SMOKE_EXPECTED[name])
        for name in ("all", "asserted", "mined")
    }
    deps = harness.deps()

    def smoke_transform(valid_path, transform_dir, events):
        transform_dir.mkdir(parents=True, exist_ok=True)
        parquet = {}
        for name in ("all", "asserted", "mined"):
            path = transform_dir / f"{name}.parquet"
            path.write_bytes(b"pq")
            parquet[name] = path
        return TransformResult(
            parquet=parquet,
            stats=smoke_stats,
            reference_keys={name: () for name in ("all", "asserted", "mined")},
            dropped_reference_keys=(),
        )

    deps = PipelineDependencies(**{**deps.__dict__, "transform": smoke_transform})
    result = run_pipeline(harness.config(smoke=True, hf_token=None), deps)
    assert result.status == "smoke-completed"
    assert result.counts["failed_works"] == 1
    assert "resolve_mirror" not in harness.calls
    assert "publish" not in harness.calls


def test_smoke_counts_mismatch_fails(tmp_path: Path) -> None:
    harness = Harness(tmp_path)
    with pytest.raises(PipelineError, match="smoke counts"):
        # harness transform returns production STATS, which differ from SMOKE_EXPECTED
        run_pipeline(harness.config(smoke=True, hf_token=None), harness.deps())
