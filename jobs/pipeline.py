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
from pathlib import Path

DEBIAN_PACKAGES = (
    "ca-certificates",
    "curl",
    "build-essential",
    "cmake",
    "pkg-config",
    "libssl-dev",
    "zlib1g-dev",
)
RUSTUP_INSTALLER = Path("/tmp/citations-rustup-init.sh")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--bucket-root", type=Path)
    parser.add_argument(
        "--work-root", type=Path, default=Path("/tmp/crossref-arxiv-citations")
    )
    parser.add_argument("--datacite-release")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    return parser.parse_args()


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
    if not Path("/etc/debian_version").exists() or shutil.which("dpkg-query") is None:
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
    if missing:
        _run_checked(["apt-get", "update"])
        _run_checked(["apt-get", "install", "-y", "--no-install-recommends", *missing])


def _ensure_rust() -> None:
    if shutil.which("cargo") is None:
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


def main() -> None:
    args = parse_args()
    sys.path.insert(0, str(args.source_root))
    from citations_pipeline.telemetry import EventLogger

    events = EventLogger()
    with events.phase("bootstrap_packages"):
        _ensure_debian_packages()
    with events.phase("bootstrap_rust"):
        _ensure_rust()
    from citations_pipeline.pipeline import config_from_args, run_pipeline

    result = run_pipeline(config_from_args(args))
    print(
        "RESULT_JSON=" + json.dumps(result.to_dict(), sort_keys=True),
        flush=True,
    )


if __name__ == "__main__":
    main()
