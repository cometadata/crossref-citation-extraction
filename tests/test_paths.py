from pathlib import Path

import pytest

from citations_pipeline import paths


def test_constants() -> None:
    assert paths.BUCKET_PREFIX_NAME == "crossref-arxiv-citations-latest"
    assert paths.AFFILIATIONS_PREFIX_NAME == "crossref-affiliations-latest"
    assert paths.DATASET_REPO == "cometadata/crossref-arxiv-citations"
    assert paths.INPUT_STEM == "all.json"
    assert paths.CONFIG_PROVENANCES["asserted"] == ("publisher", "crossref")


def test_pipeline_paths_layout(tmp_path: Path) -> None:
    p = paths.PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    assert p.prefix == tmp_path / "mnt" / "crossref-arxiv-citations-latest"
    assert p.pipeline_marker == p.prefix / "_PIPELINE_COMPLETE.json"
    assert p.extraction_marker == p.prefix / "_EXTRACTION_COMPLETE.json"
    assert p.transfer_marker == p.prefix / "_TRANSFER_COMPLETE.json"
    assert p.affiliations_prefix == tmp_path / "mnt" / "crossref-affiliations-latest"
    assert p.snapshot_local == tmp_path / "work" / "snapshot" / "all.json.tar.gz"


def test_generation_paths(tmp_path: Path) -> None:
    p = paths.PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    run_id = "0" * 32
    raw = p.new_raw_generation("2026-07", run_id)
    assert raw.relative_prefix == f"raw-generations/2026-07-{run_id}"
    assert raw.archive == raw.root / "all.json.tar.gz"
    derived = p.new_derived_generation("2026-07", run_id)
    assert derived.relative_prefix == f"derived-generations/2026-07-{run_id}"
    assert derived.data == derived.root / "data"
    assert derived.outputs == derived.root / "outputs"
    assert derived.report == derived.root / "report.json"


def test_fst_prefix(tmp_path: Path) -> None:
    p = paths.PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    fst = p.fst_prefix("2026-07")
    assert fst.relative_prefix == "fst/2026-07-v1"
    assert fst.fst == p.prefix / "fst" / "2026-07-v1" / "arxiv.fst"
    assert fst.marker == p.prefix / "fst" / "2026-07-v1" / "_FST_COMPLETE.json"
    with pytest.raises(paths.GenerationPathError):
        p.fst_prefix("not-a-release")


def test_generation_prefix_validation() -> None:
    with pytest.raises(paths.GenerationPathError):
        paths.validate_generation_prefix("/abs/path", "raw-generations")
    with pytest.raises(paths.GenerationPathError):
        paths.validate_generation_prefix("raw-generations/../x", "raw-generations")
    with pytest.raises(paths.GenerationPathError):
        paths.validate_generation_prefix("other-root/name", "raw-generations")


def test_runtime_source_digest_tracks_content(tmp_path: Path) -> None:
    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / "pipeline.py").write_text("print(1)\n")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.rs").write_text("fn main() {}\n")
    (tmp_path / "Cargo.toml").write_text("[package]\n")
    (tmp_path / "Cargo.lock").write_text("# lock\n")
    (tmp_path / "benches").mkdir()
    (tmp_path / "benches" / "extraction_bench.rs").write_text("fn bench() {}\n")
    files = paths.runtime_source_files(tmp_path)
    assert (tmp_path / "Cargo.lock") in files
    assert (tmp_path / "benches" / "extraction_bench.rs") in files
    before = paths.runtime_source_digest(tmp_path)
    (tmp_path / "src" / "main.rs").write_text("fn main() { }\n")
    assert paths.runtime_source_digest(tmp_path) != before
