import subprocess
from pathlib import Path

import pytest

from citations_pipeline.extractor import (
    ExtractorBinary,
    ExtractorError,
    build_extractor,
    run_build_index,
    run_extraction,
)
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
