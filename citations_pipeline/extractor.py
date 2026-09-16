from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .full_state import ResourceSettings
from .paths import INPUT_STEM, OUTPUT_KINDS
from .state import StateError, read_json

# Full-mode (`pipeline --datacite-index`) outputs and report contract; see the
# Rust `commands::datacite_pipeline` module.
REPORT_NAME = "extraction-report.json"
REPORT_KIND = "datacite-extraction-report"
REPORT_SCHEMA_VERSION = 2
FULL_OUTPUT_KINDS = (*OUTPUT_KINDS, "ambiguous_urls")
REPORT_STAT_KEYS = (
    "items",
    "references",
    "confirmed_matches",
    "doi_matches",
    "url_matches",
    "arxiv_matches",
    "ambiguous_urls",
    "invalid_arxiv",
    "unmatched_dois",
    "unmatched_urls",
    "malformed_references",
    "malformed_fields",
    "filtered_self_citations",
    "filtered_provenance",
)
_STEM_SUFFIXES = (".tar.gz", ".jsonl.gz", ".jsonl", ".json")


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


def run_build_datacite_index(
    binary: ExtractorBinary,
    records: Path,
    output: Path,
    sort_memory_mb: int,
    runner: Runner = subprocess.run,
) -> None:
    """Build a full DataCite index bundle from a monthly release root.

    `output` is left for the Rust builder to create: it stages under the
    parent and refuses an existing, non-empty output directory."""
    if (
        isinstance(sort_memory_mb, bool)
        or not isinstance(sort_memory_mb, int)
        or sort_memory_mb <= 0
    ):
        raise ExtractorError("sort_memory_mb must be a positive integer")
    output.parent.mkdir(parents=True, exist_ok=True)
    _run(
        runner,
        [
            str(binary.path),
            "build-datacite-index",
            "--input",
            str(records),
            "--output",
            str(output),
            "--sort-memory-mb",
            str(sort_memory_mb),
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


# --- full-mode extraction --------------------------------------------------------


@dataclass(frozen=True)
class FullExtractionOutputs:
    valid: Path
    failed: Path
    publisher: Path
    crossref: Path
    mined: Path
    ambiguous_urls: Path
    report: Path

    def paths(self) -> tuple[Path, ...]:
        return (
            self.valid,
            self.failed,
            self.publisher,
            self.crossref,
            self.mined,
            self.ambiguous_urls,
            self.report,
        )


@dataclass(frozen=True)
class ExtractionReport:
    """The validated content of `extraction-report.json`."""

    identity_sha256: str
    files_processed: int
    stats: dict[str, int]
    aggregation: dict[str, int]


def input_stem(archive: Path) -> str:
    """Mirror the extractor's `extract_input_stem`: the file name with one
    known compound extension removed (`all.json.tar.gz` -> `all.json`)."""
    name = archive.name
    for suffix in _STEM_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def full_output_paths(output_dir: Path, archive: Path) -> FullExtractionOutputs:
    stem = input_stem(archive)
    return FullExtractionOutputs(
        **{kind: output_dir / f"{stem}_{kind}.jsonl" for kind in FULL_OUTPUT_KINDS},
        report=output_dir / REPORT_NAME,
    )


def identity_file_sha256(identity_path: Path) -> str:
    """The `identity_sha256` the extractor derives from a `--run-identity`
    file: SHA-256 of its canonical JSON (sorted keys, no whitespace)."""
    try:
        payload = read_json(identity_path)
    except StateError as error:
        raise ExtractorError(f"run identity file is unreadable: {identity_path}") from error
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _counters(value: Any, required: tuple[str, ...], label: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ExtractorError(f"{label} must be an object")
    missing = [key for key in required if key not in value]
    if missing:
        raise ExtractorError(f"{label} is missing counters: {missing}")
    counters: dict[str, int] = {}
    for key, item in value.items():
        if not _is_count(item):
            raise ExtractorError(
                f"{label}.{key} must be a non-negative integer, got {item!r}"
            )
        counters[key] = item
    return counters


def load_extraction_report(path: Path, *, identity_sha256: str) -> ExtractionReport:
    """Parse and validate `extraction-report.json` against the run identity.

    A report is only accepted when it names the invoked identity, reports a
    complete traversal with zero fatal source errors and at least one file,
    carries every counter as a non-negative integer (unknown counters are
    tolerated only when they are integers too), and its match counters add
    up.
    """
    label = path.name
    if not path.is_file():
        raise ExtractorError(f"extraction report missing: {path}")
    try:
        payload = read_json(path)
    except StateError as error:
        raise ExtractorError(f"{label} is unreadable: {path}") from error
    kind = payload.get("kind")
    if kind != REPORT_KIND:
        raise ExtractorError(f"{label}: kind must be {REPORT_KIND!r}, got {kind!r}")
    schema_version = payload.get("schema_version")
    if isinstance(schema_version, bool) or schema_version != REPORT_SCHEMA_VERSION:
        raise ExtractorError(
            f"{label}: schema_version must be {REPORT_SCHEMA_VERSION}, "
            f"got {schema_version!r}"
        )
    reported = payload.get("identity_sha256")
    if reported != identity_sha256:
        raise ExtractorError(
            f"{label}: identity_sha256 {reported!r} does not match the run "
            f"identity {identity_sha256}"
        )
    if payload.get("completed") is not True:
        raise ExtractorError(f"{label}: completed must be true (traversal incomplete)")
    fatal = payload.get("fatal_source_errors")
    if not _is_count(fatal) or fatal != 0:
        raise ExtractorError(f"{label}: fatal_source_errors must be 0, got {fatal!r}")
    files_processed = payload.get("files_processed")
    if not _is_count(files_processed) or files_processed == 0:
        raise ExtractorError(
            f"{label}: files_processed must be a positive integer, "
            f"got {files_processed!r}"
        )
    stats = _counters(payload.get("stats"), REPORT_STAT_KEYS, f"{label}: stats")
    matched = stats["doi_matches"] + stats["url_matches"] + stats["arxiv_matches"]
    if matched != stats["confirmed_matches"]:
        raise ExtractorError(
            f"{label}: doi_matches + url_matches + arxiv_matches ({matched}) must "
            f"equal confirmed_matches ({stats['confirmed_matches']})"
        )
    aggregation = _counters(payload.get("aggregation"), (), f"{label}: aggregation")
    return ExtractionReport(
        identity_sha256=reported,
        files_processed=files_processed,
        stats=stats,
        aggregation=aggregation,
    )


def run_datacite_extraction(
    binary: ExtractorBinary,
    *,
    archive: Path,
    bundle: Path,
    identity_path: Path,
    output_dir: Path,
    temp_dir: Path,
    resources: ResourceSettings,
    events: Any,
    runner: Runner = subprocess.run,
) -> FullExtractionOutputs:
    """Run `pipeline --datacite-index` and return its seven outputs.

    Every output must exist afterwards (zero-byte diagnostic JSONL is valid;
    nothing is ever created here to paper over a missing file), and the
    report must describe this invocation's identity and a complete run.
    """
    arguments = [
        str(binary.path),
        "pipeline",
        "--input",
        str(archive),
        "--datacite-index",
        str(bundle),
        "--run-identity",
        str(identity_path),
        "--outputs",
        "all",
        "--output-dir",
        str(output_dir),
        "--temp-dir",
        str(temp_dir),
        *resources.rust_args(),
    ]
    expected_identity = identity_file_sha256(identity_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    with events.phase("extract"):
        _run(runner, arguments)
    outputs = full_output_paths(output_dir, archive)
    missing = [path.name for path in outputs.paths() if not path.is_file()]
    if missing:
        raise ExtractorError(
            f"extraction outputs missing from {output_dir}: {missing}"
        )
    load_extraction_report(outputs.report, identity_sha256=expected_identity)
    return outputs
