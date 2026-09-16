# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.3,<2",
#   "huggingface-hub>=1.23.0,<2",
#   "pyarrow>=20,<22",
#   "requests>=2.32,<3",
#   "zstandard>=0.23,<1",
# ]
# ///
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # the source root joins sys.path only inside main()
    from citations_pipeline.pipeline import PipelineConfig

DEBIAN_PACKAGES = (
    "ca-certificates",
    "curl",
    "build-essential",
    "cmake",
    "pkg-config",
    "libssl-dev",
    "zlib1g-dev",
)
DEBIAN_MARKER = Path("/etc/debian_version")
RUSTUP_INSTALLER = Path("/tmp/citations-rustup-init.sh")
PIPELINE_MODES = ("legacy", "datacite", "smoke", "benchmark")
# `ResourceSettings` field -> job flag. The first nine are the extraction
# flags (`ResourceSettings.rust_args()`); the last two drive the Python
# bundle build and transform.
RESOURCE_FLAGS = (
    ("partitions", "--partitions"),
    ("writer_memory_mb", "--writer-memory-mb"),
    ("result_batch_mb", "--result-batch-mb"),
    ("queue_batches", "--queue-batches"),
    ("aggregation_memory_mb", "--aggregation-memory-mb"),
    ("merge_fan_in", "--merge-fan-in"),
    ("sort_workers", "--sort-workers"),
    ("lookup_memory_mb", "--lookup-memory-mb"),
    ("max_entry_mb", "--max-entry-mb"),
    ("index_sort_memory_mb", "--index-sort-memory-mb"),
    ("duckdb_memory_mb", "--duckdb-memory-mb"),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--bucket-root", type=Path)
    parser.add_argument(
        "--work-root", type=Path, default=Path("/tmp/crossref-arxiv-citations")
    )
    parser.add_argument("--mode", choices=PIPELINE_MODES, default=None)
    parser.add_argument("--datacite-release")
    parser.add_argument("--snapshot-id")
    parser.add_argument("--force", action="store_true")
    # Informational; exported as BENCHMARK_FLAVOR so the benchmark reports
    # the hardware it measured on.
    parser.add_argument("--flavor")
    # Legacy alias for `--mode smoke`.
    parser.add_argument("--smoke", action="store_true")
    for name, flag in RESOURCE_FLAGS:
        parser.add_argument(flag, dest=name, type=int, default=None)
    args = parser.parse_args(argv)
    if args.smoke:
        if args.mode not in (None, "smoke"):
            parser.error(f"--smoke conflicts with --mode {args.mode}")
        args.mode = "smoke"
    elif args.mode is None:
        args.mode = "datacite"
    args.smoke = args.mode == "smoke"
    return args


def _run_checked(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            arguments,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        # capture_output swallows the child's diagnostics, so echo them before
        # re-raising; otherwise a bootstrap failure only reports an exit code.
        for stream in (error.stdout, error.stderr):
            if not stream:
                continue
            if isinstance(stream, bytes):
                stream = stream.decode(errors="replace")
            print(stream, file=sys.stderr, end="" if stream.endswith("\n") else "\n")
        print(
            f"command failed with exit code {error.returncode}: {' '.join(arguments)}",
            file=sys.stderr,
            flush=True,
        )
        raise


def _ensure_debian_packages() -> None:
    """Install the build prerequisites on the Debian job image. A no-op
    outside Debian, without apt-get, or without root (local runs)."""
    if not DEBIAN_MARKER.exists() or shutil.which("dpkg-query") is None:
        return
    missing = []
    for package in DEBIAN_PACKAGES:
        result = subprocess.run(
            ["dpkg-query", "-W", "-f=${Status}", package],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or "install ok installed" not in result.stdout:
            missing.append(package)
    if not missing:
        return
    if shutil.which("apt-get") is None or os.geteuid() != 0:
        print(
            "skipping package bootstrap (apt-get unavailable or not root); "
            f"missing: {' '.join(missing)}",
            file=sys.stderr,
            flush=True,
        )
        return
    _run_checked(["apt-get", "update"])
    _run_checked(["apt-get", "install", "-y", "--no-install-recommends", *missing])


def _ensure_rust() -> None:
    """Install a stable toolchain only when cargo is absent; always record
    the toolchain versions for the run."""
    if shutil.which("cargo") is None or shutil.which("rustc") is None:
        _run_checked(
            [
                "curl",
                "--proto",
                "=https",
                "--tlsv1.2",
                "-sSf",
                "https://sh.rustup.rs",
                "-o",
                str(RUSTUP_INSTALLER),
            ]
        )
        try:
            _run_checked(
                [
                    "sh",
                    str(RUSTUP_INSTALLER),
                    "-y",
                    "--profile",
                    "minimal",
                    "--default-toolchain",
                    "stable",
                ]
            )
        finally:
            RUSTUP_INSTALLER.unlink(missing_ok=True)
        cargo_bin = Path.home() / ".cargo" / "bin"
        os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    rustc = _run_checked(["rustc", "--version"]).stdout.strip()
    cargo = _run_checked(["cargo", "--version"]).stdout.strip()
    os.environ["CROSSREF_RUSTC_VERSION"] = rustc
    os.environ["CROSSREF_CARGO_VERSION"] = cargo


def _verify_source(args: argparse.Namespace) -> None:
    """Refuse to run against a staged tree other than the one submitted;
    every runner verifies again before touching durable state."""
    from citations_pipeline.paths import runtime_source_digest

    actual = runtime_source_digest(Path(args.source_root))
    if actual != args.source_digest:
        raise RuntimeError(
            "source digest mismatch: submitted "
            f"{args.source_digest[:12]}, staged tree is {actual[:12]}"
        )


def config_from_args(args: argparse.Namespace) -> PipelineConfig:
    """Build the `PipelineConfig` for this job from the parsed arguments and
    the credentials in the environment."""
    from citations_pipeline.full_state import ResourceSettings
    from citations_pipeline.pipeline import PipelineConfig

    overrides = {
        name: getattr(args, name)
        for name, _ in RESOURCE_FLAGS
        if getattr(args, name) is not None
    }
    resources = ResourceSettings(**overrides)
    resources.validate()
    return PipelineConfig(
        source_root=Path(args.source_root),
        source_digest=args.source_digest,
        work_root=Path(args.work_root),
        bucket_root=Path(args.bucket_root) if args.bucket_root else None,
        smoke=args.mode == "smoke",
        force=bool(args.force),
        datacite_release=args.datacite_release,
        hf_token=os.environ.get("HF_TOKEN"),
        crossref_token=os.environ.get("CROSSREF_API_TOKEN"),
        run_id=uuid.uuid4().hex,
        mode=args.mode,
        resources=resources,
        expected_snapshot_id=args.snapshot_id,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    sys.path.insert(0, str(args.source_root))
    from citations_pipeline.telemetry import EventLogger

    events = EventLogger()
    with events.phase("verify_entry_source"):
        _verify_source(args)
    with events.phase("bootstrap_packages"):
        _ensure_debian_packages()
    with events.phase("bootstrap_rust"):
        _ensure_rust()
    if args.flavor:
        os.environ["BENCHMARK_FLAVOR"] = args.flavor
    from citations_pipeline.benchmark import run_benchmark
    from citations_pipeline.datacite_smoke import run_datacite_smoke
    from citations_pipeline.full_pipeline import run_full_pipeline
    from citations_pipeline.pipeline import run_pipeline

    # Remote dispatch after source verification and environment bootstrap.
    if args.mode == "legacy":
        result = run_pipeline(config_from_args(args))
    elif args.mode == "datacite":
        result = run_full_pipeline(config_from_args(args))
    elif args.mode == "smoke":
        result = run_datacite_smoke(config_from_args(args))
    else:
        result = run_benchmark(config_from_args(args))
    payload = result.to_dict() if hasattr(result, "to_dict") else result
    print("RESULT_JSON=" + json.dumps(payload, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
