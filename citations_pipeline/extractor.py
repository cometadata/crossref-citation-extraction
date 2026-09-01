from __future__ import annotations

import os
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .paths import INPUT_STEM, OUTPUT_KINDS


class ExtractorError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess]


@dataclass(frozen=True)
class ExtractorBinary:
    path: Path
    version: str


def _run(runner: Runner, arguments: list[str], **kwargs: Any) -> None:
    result = runner(arguments, **kwargs)
    returncode = getattr(result, "returncode", 0)
    if returncode != 0:
        raise ExtractorError(
            f"command failed with exit code {returncode}: {' '.join(arguments[:2])}"
        )


def build_extractor(
    source_root: Path,
    target_dir: Path,
    events: Any,
    runner: Runner = subprocess.run,
) -> ExtractorBinary:
    manifest = source_root / "Cargo.toml"
    if not manifest.is_file():
        raise ExtractorError(f"Cargo.toml not found under {source_root}")
    version = tomllib.loads(manifest.read_text())["package"]["version"]
    env = dict(os.environ)
    env["CARGO_TARGET_DIR"] = str(target_dir)
    with events.phase("build_extractor"):
        _run(
            runner,
            ["cargo", "build", "--release", "--locked"],
            cwd=source_root,
            env=env,
        )
    binary = target_dir / "release" / "crossref-citation-extraction"
    if not binary.is_file():
        raise ExtractorError(f"extractor binary missing after build: {binary}")
    return ExtractorBinary(path=binary, version=version)


def run_build_index(
    binary: ExtractorBinary,
    records: Path,
    output: Path,
    runner: Runner = subprocess.run,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    _run(
        runner,
        [
            str(binary.path),
            "build-index",
            "--input",
            str(records),
            "--output",
            str(output),
        ],
    )


@dataclass(frozen=True)
class ExtractionOutputs:
    valid: Path
    failed: Path
    publisher: Path
    crossref: Path
    mined: Path


def run_extraction(
    binary: ExtractorBinary,
    *,
    archive: Path,
    fst: Path,
    output_dir: Path,
    temp_dir: Path,
    events: Any,
    runner: Runner = subprocess.run,
) -> ExtractionOutputs:
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    with events.phase("extract"):
        _run(
            runner,
            [
                str(binary.path),
                "pipeline",
                "--input",
                str(archive),
                "--arxiv-fst",
                str(fst),
                "--outputs",
                "all",
                "--output-dir",
                str(output_dir),
                "--temp-dir",
                str(temp_dir),
                "--log-level",
                "INFO",
            ],
        )
    outputs: dict[str, Path] = {}
    for kind in OUTPUT_KINDS:
        path = output_dir / f"{INPUT_STEM}_{kind}.jsonl"
        if not path.is_file():
            path.touch()
        outputs[kind] = path
    return ExtractionOutputs(**outputs)
