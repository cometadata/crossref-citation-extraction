import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from citations_pipeline.extractor import (
    ExtractorBinary,
    ExtractorError,
    FullExtractionOutputs,
    build_extractor,
    run_build_index,
    run_datacite_extraction,
    run_extraction,
)
from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.telemetry import EventLogger


class FakeRunner:
    def __init__(self, returncode: int = 0) -> None:
        self.calls: list[dict] = []
        self.returncode = returncode
        self.on_call = None

    def __call__(self, arguments, **kwargs):
        self.calls.append({"arguments": list(arguments), **kwargs})
        if self.on_call is not None:
            self.on_call(arguments, kwargs)
        return subprocess.CompletedProcess(arguments, self.returncode)


def make_crate(root: Path) -> None:
    (root / "Cargo.toml").write_text(
        '[package]\nname = "crossref-citation-extraction"\nversion = "2.1.0"\n'
    )


def test_build_extractor_uses_locked_build_and_target_dir(tmp_path: Path) -> None:
    source = tmp_path / "src"
    target = tmp_path / "target"
    source.mkdir()
    make_crate(source)
    runner = FakeRunner()

    def create_binary(arguments, kwargs):
        binary = target / "release" / "crossref-citation-extraction"
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_bytes(b"\x7fELF")

    runner.on_call = create_binary
    binary = build_extractor(source, target, EventLogger(), runner=runner)
    call = runner.calls[0]
    assert call["arguments"] == ["cargo", "build", "--release", "--locked"]
    assert call["cwd"] == source
    assert call["env"]["CARGO_TARGET_DIR"] == str(target)
    assert binary.version == "2.1.0"
    assert binary.path == target / "release" / "crossref-citation-extraction"


def test_build_extractor_fails_on_nonzero(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    make_crate(source)
    with pytest.raises(ExtractorError, match="cargo"):
        build_extractor(
            source, tmp_path / "t", EventLogger(), runner=FakeRunner(returncode=1)
        )


def test_run_build_index_composes_command(tmp_path: Path) -> None:
    binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
    runner = FakeRunner()
    run_build_index(binary, tmp_path / "dois", tmp_path / "out" / "arxiv.fst", runner=runner)
    assert runner.calls[0]["arguments"] == [
        str(tmp_path / "bin"),
        "build-index",
        "--input",
        str(tmp_path / "dois"),
        "--output",
        str(tmp_path / "out" / "arxiv.fst"),
    ]


def test_run_extraction_composes_command_and_touches_missing_outputs(
    tmp_path: Path,
) -> None:
    binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
    runner = FakeRunner()
    out_dir = tmp_path / "outputs"

    def create_valid_only(arguments, kwargs):
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "all.json_valid.jsonl").write_text('{"doi": "x"}\n')

    runner.on_call = create_valid_only
    outputs = run_extraction(
        binary,
        archive=tmp_path / "all.json.tar.gz",
        fst=tmp_path / "arxiv.fst",
        output_dir=out_dir,
        temp_dir=tmp_path / "partitions",
        events=EventLogger(),
        runner=runner,
    )
    arguments = runner.calls[0]["arguments"]
    assert arguments[:2] == [str(tmp_path / "bin"), "pipeline"]
    assert "--outputs" in arguments and arguments[arguments.index("--outputs") + 1] == "all"
    assert outputs.valid.read_text() == '{"doi": "x"}\n'
    assert outputs.failed.exists() and outputs.failed.stat().st_size == 0
    assert outputs.mined.name == "all.json_mined.jsonl"


def test_run_extraction_fails_on_nonzero(tmp_path: Path) -> None:
    binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
    with pytest.raises(ExtractorError):
        run_extraction(
            binary,
            archive=tmp_path / "a",
            fst=tmp_path / "f",
            output_dir=tmp_path / "o",
            temp_dir=tmp_path / "t",
            events=EventLogger(),
            runner=FakeRunner(returncode=2),
        )


# --- full-mode extraction wrapper ---------------------------------------------

FULL_KINDS = ("valid", "failed", "publisher", "crossref", "mined", "ambiguous_urls")
IDENTITY = {
    "extraction_mode": "datacite",
    "partition_algorithm": "fnv1a64-v1",
    "partitions": 4,
    "bundle_manifest_sha256": "b" * 64,
    "archive_sha256": "a" * 64,
}
STATS = {
    "items": 2, "references": 3, "confirmed_matches": 3,
    "doi_matches": 1, "url_matches": 1, "arxiv_matches": 1,
    "ambiguous_urls": 0, "invalid_arxiv": 0,
    "unmatched_dois": 0, "unmatched_urls": 0,
    "malformed_references": 0, "malformed_fields": 0,
    "filtered_self_citations": 0, "filtered_provenance": 0,
}
AGGREGATION = {
    "partitions_processed": 1, "valid_count": 2, "failed_count": 0, "total_citations": 3,
    "publisher_citations": 1, "crossref_citations": 0, "mined_citations": 2,
    "in_memory_partitions": 1, "external_partitions": 0, "reused_partitions": 0,
}


def canonical_sha256(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def report_payload(identity_sha256: str, **overrides) -> dict:
    payload = {
        "kind": "datacite-extraction-report",
        "schema_version": 2,
        "identity_sha256": identity_sha256,
        "completed": True,
        "files_processed": 1,
        "fatal_source_errors": 0,
        "stats": dict(STATS),
        "aggregation": dict(AGGREGATION),
    }
    payload.update(overrides)
    return payload


def write_full_outputs(
    out_dir: Path,
    stem: str,
    report: dict | None,
    *,
    omit: tuple[str, ...] = (),
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for kind in FULL_KINDS:
        if kind in omit:
            continue
        content = "" if kind in {"failed", "ambiguous_urls"} else '{"doi": "x"}\n'
        (out_dir / f"{stem}_{kind}.jsonl").write_text(content)
    if report is not None:
        (out_dir / "extraction-report.json").write_text(json.dumps(report, indent=2) + "\n")


class FullCase:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
        self.archive = tmp_path / "all.json.tar.gz"
        self.bundle = tmp_path / "bundle"
        self.identity_path = tmp_path / "run-identity.json"
        self.identity_path.write_text(json.dumps(IDENTITY, indent=2) + "\n")
        self.identity_sha256 = canonical_sha256(IDENTITY)
        self.out_dir = tmp_path / "outputs"
        self.temp_dir = tmp_path / "temp"
        self.resources = ResourceSettings(partitions=4)
        self.report: dict | None = report_payload(self.identity_sha256)
        self.omit: tuple[str, ...] = ()
        self.runner = FakeRunner()
        self.runner.on_call = self._write_outputs

    def _write_outputs(self, arguments, kwargs) -> None:
        write_full_outputs(self.out_dir, "all.json", self.report, omit=self.omit)

    def run(self) -> FullExtractionOutputs:
        return run_datacite_extraction(
            self.binary,
            archive=self.archive,
            bundle=self.bundle,
            identity_path=self.identity_path,
            output_dir=self.out_dir,
            temp_dir=self.temp_dir,
            resources=self.resources,
            events=EventLogger(output=lambda _line: None),
            runner=self.runner,
        )


def test_run_datacite_extraction_forwards_bundle_identity_and_resources(
    tmp_path: Path,
) -> None:
    case = FullCase(tmp_path)
    outputs = case.run()
    assert case.runner.calls[0]["arguments"] == [
        str(case.binary.path),
        "pipeline",
        "--input",
        str(case.archive),
        "--datacite-index",
        str(case.bundle),
        "--run-identity",
        str(case.identity_path),
        "--outputs",
        "all",
        "--output-dir",
        str(case.out_dir),
        "--temp-dir",
        str(case.temp_dir),
        *case.resources.rust_args(),
    ]
    assert "--partitions" in case.runner.calls[0]["arguments"]
    assert outputs == FullExtractionOutputs(
        valid=case.out_dir / "all.json_valid.jsonl",
        failed=case.out_dir / "all.json_failed.jsonl",
        publisher=case.out_dir / "all.json_publisher.jsonl",
        crossref=case.out_dir / "all.json_crossref.jsonl",
        mined=case.out_dir / "all.json_mined.jsonl",
        ambiguous_urls=case.out_dir / "all.json_ambiguous_urls.jsonl",
        report=case.out_dir / "extraction-report.json",
    )
    # Zero-byte diagnostic outputs are valid.
    assert outputs.failed.stat().st_size == 0
    assert outputs.ambiguous_urls.stat().st_size == 0
    assert case.temp_dir.is_dir()


def test_run_datacite_extraction_derives_the_stem_like_the_extractor(
    tmp_path: Path,
) -> None:
    case = FullCase(tmp_path)
    case.archive = tmp_path / "sample-2026.tar.gz"
    case.runner.on_call = lambda arguments, kwargs: write_full_outputs(
        case.out_dir, "sample-2026", case.report
    )
    outputs = case.run()
    assert outputs.valid.name == "sample-2026_valid.jsonl"
    assert outputs.ambiguous_urls.name == "sample-2026_ambiguous_urls.jsonl"


@pytest.mark.parametrize("missing", ["mined", "ambiguous_urls", "failed"])
def test_run_datacite_extraction_fails_on_missing_output_without_touching(
    tmp_path: Path, missing: str
) -> None:
    case = FullCase(tmp_path)
    case.omit = (missing,)
    with pytest.raises(ExtractorError, match=f"all.json_{missing}.jsonl"):
        case.run()
    assert not (case.out_dir / f"all.json_{missing}.jsonl").exists()


def test_run_datacite_extraction_requires_the_report(tmp_path: Path) -> None:
    case = FullCase(tmp_path)
    case.report = None
    with pytest.raises(ExtractorError, match="extraction-report.json"):
        case.run()


def test_run_datacite_extraction_rejects_identity_mismatch(tmp_path: Path) -> None:
    case = FullCase(tmp_path)
    case.report = report_payload("0" * 64)
    with pytest.raises(ExtractorError, match="identity"):
        case.run()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"completed": False}, "completed"),
        ({"fatal_source_errors": 2}, "fatal_source_errors"),
        ({"files_processed": 0}, "files_processed"),
        ({"kind": "arxiv-extraction-report"}, "kind"),
        ({"schema_version": 1}, "schema_version"),
        ({"stats": {**STATS, "doi_matches": 2}}, "confirmed_matches"),
        ({"stats": {**STATS, "items": "2"}}, "items"),
        ({"stats": {**STATS, "future_counter": "x"}}, "future_counter"),
        ({"stats": {key: STATS[key] for key in STATS if key != "url_matches"}}, "url_matches"),
        ({"aggregation": {**AGGREGATION, "valid_count": None}}, "valid_count"),
    ],
)
def test_run_datacite_extraction_rejects_inconsistent_reports(
    tmp_path: Path, overrides: dict, message: str
) -> None:
    case = FullCase(tmp_path)
    case.report = report_payload(case.identity_sha256, **overrides)
    with pytest.raises(ExtractorError, match=message):
        case.run()


def test_run_datacite_extraction_tolerates_unknown_integer_counters(
    tmp_path: Path,
) -> None:
    case = FullCase(tmp_path)
    case.report = report_payload(
        case.identity_sha256, stats={**STATS, "future_counter": 7}
    )
    case.run()


def test_run_datacite_extraction_fails_on_nonzero(tmp_path: Path) -> None:
    case = FullCase(tmp_path)
    case.runner = FakeRunner(returncode=3)
    with pytest.raises(ExtractorError, match="exit code 3"):
        case.run()
    assert not case.out_dir.joinpath("extraction-report.json").exists()
