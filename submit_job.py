# /// script
# requires-python = ">=3.11"
# dependencies = ["huggingface-hub>=1.23.0,<2", "requests>=2.32,<3"]
# ///
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, JobInfo, Volume, get_token, sync_job_volume

from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.paths import (
    AFFILIATIONS_PREFIX_NAME,
    BUCKET_ID,
    runtime_source_digest,
    runtime_source_files,
)

PROJECT_ROOT = Path(__file__).resolve().parent
JOB_SCRIPT = PROJECT_ROOT / "jobs" / "pipeline.py"
NAMESPACE = "cometadata"
PIPELINE_LABEL = "crossref-arxiv-citations"
SUBMISSION_MODES = ("legacy", "datacite", "smoke", "benchmark")
MODE_LABELS = {
    "legacy": PIPELINE_LABEL,
    "datacite": "crossref-datacite-citations",
    "smoke": "crossref-datacite-smoke",
    "benchmark": "crossref-datacite-benchmark",
}
# (flavor, timeout) per mode; `--flavor`/`--timeout` or a settings file override.
MODE_DEFAULTS = {
    "legacy": ("cpu-xl", "24h"),
    "datacite": ("cpu-xl", "24h"),
    "smoke": ("cpu-basic", "30m"),
    "benchmark": ("cpu-xl", "24h"),
}
# The `production-settings.json` contract written by the benchmark
# (`citations_pipeline.benchmark.SETTINGS_KEYS`); anything else is refused.
SETTINGS_REQUIRED_KEYS = (
    "source_tree_sha256",
    "datacite_release",
    "snapshot_id",
    "flavor",
    "timeout",
    "resources",
)
SETTINGS_OPTIONAL_KEYS = ("benchmark_report_sha256",)
SETTINGS_KEYS = SETTINGS_REQUIRED_KEYS + SETTINGS_OPTIONAL_KEYS
SOURCE_MOUNT = "/workspace"
BUCKET_MOUNT = "/mnt/shared-storage"

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_RELEASE = re.compile(r"^\d{4}-\d{2}$")
_TIMEOUT = re.compile(r"^[1-9]\d*[smhd]?$")


class SettingsError(RuntimeError):
    """A settings file or its relationship to the source tree is unusable."""


@dataclass(frozen=True)
class SubmissionConfig:
    source_root: Path
    smoke: bool
    force: bool
    datacite_release: str | None
    mode: str = "datacite"
    flavor: str | None = None
    timeout: str | None = None
    resources: ResourceSettings = field(default_factory=ResourceSettings)
    expected_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in SUBMISSION_MODES:
            raise ValueError(
                f"mode must be one of {SUBMISSION_MODES}: {self.mode!r}"
            )
        # `smoke=True` predates `mode`; it still selects the smoke, and the
        # two stay consistent so callers can test either.
        if self.smoke and self.mode == "datacite":
            object.__setattr__(self, "mode", "smoke")
        elif self.smoke and self.mode != "smoke":
            raise ValueError(f"smoke=True conflicts with mode {self.mode!r}")
        elif self.mode == "smoke":
            object.__setattr__(self, "smoke", True)
        if self.flavor is not None and not self.flavor.strip():
            raise ValueError("flavor must be a non-empty hardware name")
        if self.timeout is not None and not _TIMEOUT.fullmatch(self.timeout):
            raise ValueError(
                f"timeout must be a duration such as 30m, 24h or 3600: {self.timeout!r}"
            )


@dataclass(frozen=True)
class ProductionSettings:
    source_tree_sha256: str
    datacite_release: str
    snapshot_id: str
    flavor: str
    timeout: str
    resources: ResourceSettings
    benchmark_report_sha256: str | None = None


@dataclass(frozen=True)
class SubmissionServices:
    get_token: Callable[[], str | None]
    sync_job_volume: Callable[..., Volume]
    volume_factory: Callable[..., Volume]
    api_factory: Callable[..., HfApi]


DEFAULT_SERVICES = SubmissionServices(
    get_token=get_token,
    sync_job_volume=sync_job_volume,
    volume_factory=Volume,
    api_factory=HfApi,
)


# --- settings contract ---


def _settings_string(
    payload: dict[str, Any], key: str, pattern: re.Pattern[str] | None, what: str
) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise SettingsError(f"settings key {key!r} must be {what}")
    if pattern is not None and not pattern.fullmatch(value):
        raise SettingsError(f"settings key {key!r} must be {what}: {value!r}")
    return value


def load_settings(path: Path) -> ProductionSettings:
    """Read and validate a `production-settings.json` written by the
    benchmark. Unknown keys, missing required keys, malformed values, and
    resource settings that fail `ResourceSettings.validate()` are refused.
    The file is read by the submitter only; it is never staged."""
    try:
        payload = json.loads(Path(path).read_text())
    except OSError as error:
        raise SettingsError(f"cannot read settings file {path}: {error}") from error
    except ValueError as error:
        raise SettingsError(f"settings file {path} is not valid JSON: {error}") from error
    if not isinstance(payload, dict):
        raise SettingsError(f"settings file {path} must contain a JSON object")
    unknown = sorted(set(payload) - set(SETTINGS_KEYS))
    if unknown:
        raise SettingsError(f"unknown settings keys: {', '.join(unknown)}")
    missing = [key for key in SETTINGS_REQUIRED_KEYS if key not in payload]
    if missing:
        raise SettingsError(f"missing settings keys: {', '.join(missing)}")
    source_tree_sha256 = _settings_string(
        payload, "source_tree_sha256", _SHA256_HEX, "a lowercase SHA-256 digest"
    )
    datacite_release = _settings_string(
        payload, "datacite_release", _RELEASE, "a YYYY-MM release"
    )
    snapshot_id = _settings_string(payload, "snapshot_id", _RELEASE, "a YYYY-MM snapshot")
    flavor = _settings_string(payload, "flavor", None, "a non-empty hardware name")
    timeout = _settings_string(
        payload, "timeout", _TIMEOUT, "a duration such as 24h"
    )
    benchmark_report_sha256 = None
    if payload.get("benchmark_report_sha256") is not None:
        benchmark_report_sha256 = _settings_string(
            payload, "benchmark_report_sha256", _SHA256_HEX, "a lowercase SHA-256 digest"
        )
    resources = payload["resources"]
    if not isinstance(resources, dict):
        raise SettingsError("settings key 'resources' must be an object")
    try:
        parsed = ResourceSettings(**resources)
    except TypeError as error:
        raise SettingsError(f"settings key 'resources' is invalid: {error}") from error
    try:
        parsed.validate()
    except ValueError as error:
        raise SettingsError(f"settings key 'resources' is invalid: {error}") from error
    return ProductionSettings(
        source_tree_sha256=source_tree_sha256,
        datacite_release=datacite_release,
        snapshot_id=snapshot_id,
        flavor=flavor,
        timeout=timeout,
        resources=parsed,
        benchmark_report_sha256=benchmark_report_sha256,
    )


def check_settings_source(settings: ProductionSettings, source_root: Path) -> str:
    """Refuse settings measured on a different source tree; return the
    current digest otherwise."""
    digest = runtime_source_digest(source_root)
    if settings.source_tree_sha256 != digest:
        raise SettingsError(
            "settings were measured on source tree "
            f"{settings.source_tree_sha256[:12]} but the current source tree is "
            f"{digest[:12]}; re-run the benchmark or submit without --settings"
        )
    return digest


# --- submission ---


def mirror_probably_available(api: Any, token: str) -> bool:
    try:
        entries = list(
            api.list_bucket_tree(
                BUCKET_ID,
                prefix=AFFILIATIONS_PREFIX_NAME,
                recursive=True,
                token=token,
            )
        )
    except Exception:
        return False
    names = {
        entry.path for entry in entries if getattr(entry, "type", None) == "file"
    }
    has_marker = f"{AFFILIATIONS_PREFIX_NAME}/_PIPELINE_COMPLETE.json" in names
    has_archive = any(
        name.startswith(f"{AFFILIATIONS_PREFIX_NAME}/raw-generations/")
        and name.endswith("/all.json.tar.gz")
        for name in names
    )
    return has_marker and has_archive


def resolved_flavor(config: SubmissionConfig) -> str:
    return config.flavor or MODE_DEFAULTS[config.mode][0]


def resolved_timeout(config: SubmissionConfig) -> str:
    return config.timeout or MODE_DEFAULTS[config.mode][1]


def job_labels(config: SubmissionConfig, digest: str) -> dict[str, str]:
    return {
        "pipeline": MODE_LABELS[config.mode],
        "mode": config.mode,
        "source": digest[:12],
    }


def script_arguments(config: SubmissionConfig, digest: str) -> list[str]:
    """Every validated setting as individual job arguments; the remote
    parser (`jobs/pipeline.py`) accepts exactly these."""
    arguments = [
        "--source-root",
        SOURCE_MOUNT,
        "--source-digest",
        digest,
        "--mode",
        config.mode,
    ]
    if config.mode != "smoke":
        arguments.extend(["--bucket-root", BUCKET_MOUNT])
    if config.datacite_release:
        arguments.extend(["--datacite-release", config.datacite_release])
    if config.expected_snapshot_id:
        arguments.extend(["--snapshot-id", config.expected_snapshot_id])
    if config.force:
        arguments.append("--force")
    # Informational: the job exports it as BENCHMARK_FLAVOR so a benchmark's
    # `measured_on` and cost line name the hardware it actually ran on.
    arguments.extend(["--flavor", resolved_flavor(config)])
    arguments.extend(config.resources.rust_args())
    arguments.extend(
        [
            "--index-sort-memory-mb",
            str(config.resources.index_sort_memory_mb),
            "--duckdb-memory-mb",
            str(config.resources.duckdb_memory_mb),
        ]
    )
    return arguments


def submit(
    config: SubmissionConfig,
    services: SubmissionServices = DEFAULT_SERVICES,
) -> JobInfo:
    hf_token = services.get_token()
    if not hf_token:
        raise RuntimeError("Run `hf auth login` before submitting")
    api = services.api_factory(token=hf_token)
    needs_bucket = config.mode != "smoke"
    crossref_token = os.environ.get("CROSSREF_API_TOKEN")
    if needs_bucket and not mirror_probably_available(api, hf_token):
        # The token is required only when the mirror cannot be used; it is
        # forwarded whenever available so a mirror that disappears between
        # submission and runtime still has the authenticated fallback.
        if not crossref_token:
            raise RuntimeError(
                "affiliations mirror not found in the bucket and "
                "CROSSREF_API_TOKEN is not set for the fallback download"
            )
    if not config.source_root.is_dir():
        raise RuntimeError(f"Source root does not exist: {config.source_root}")
    if not JOB_SCRIPT.is_file():
        raise RuntimeError(f"Job script does not exist: {JOB_SCRIPT}")
    digest = runtime_source_digest(config.source_root)
    # Validates the resource settings before anything is staged or synced.
    script_args = script_arguments(config, digest)
    with tempfile.TemporaryDirectory(
        prefix="crossref-arxiv-citations-runtime-"
    ) as staging:
        staging_root = Path(staging)
        for source in runtime_source_files(config.source_root):
            destination = staging_root / source.relative_to(config.source_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        source_volume = services.sync_job_volume(
            staging_root,
            SOURCE_MOUNT,
            remote_name=f"crossref-arxiv-citations-runtime-v1-{digest[:12]}",
            read_only=True,
            namespace=NAMESPACE,
            token=hf_token,
        )
    volumes = [source_volume]
    if needs_bucket:
        volumes.append(
            services.volume_factory(
                type="bucket",
                source=BUCKET_ID,
                mount_path=BUCKET_MOUNT,
                read_only=True,
            )
        )
    secrets: dict[str, str] = {}
    if needs_bucket:
        secrets["HF_TOKEN"] = hf_token
        if crossref_token:
            secrets["CROSSREF_API_TOKEN"] = crossref_token
    return api.run_uv_job(
        str(JOB_SCRIPT),
        script_args=script_args,
        env={"HF_XET_HIGH_PERFORMANCE": "1"},
        secrets=secrets,
        flavor=resolved_flavor(config),
        timeout=resolved_timeout(config),
        labels=job_labels(config, digest),
        volumes=volumes,
        namespace=NAMESPACE,
        token=hf_token,
    )


def _status_value(job: Any) -> str:
    status = getattr(job, "status", None)
    stage = getattr(status, "stage", status)
    return str(getattr(stage, "value", stage))


# --- CLI ---


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Submit the citation pipeline as a Hugging Face Job. Without a "
            "mode flag the full DataCite pipeline is submitted."
        )
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--smoke",
        action="store_true",
        help="run the deterministic DataCite smoke (no bucket, no secrets)",
    )
    modes.add_argument(
        "--benchmark",
        action="store_true",
        help="measure the full workload and write production settings",
    )
    modes.add_argument(
        "--legacy-arxiv",
        action="store_true",
        help="run the legacy arXiv-only pipeline",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="recompute reusable phases instead of reusing verified state",
    )
    parser.add_argument(
        "--datacite-release",
        help="pin the DataCite release (YYYY-MM); overrides --settings",
    )
    parser.add_argument(
        "--snapshot-id",
        help="require this Crossref snapshot (YYYY-MM); overrides --settings",
    )
    parser.add_argument(
        "--flavor",
        help="job hardware flavor (default per mode: %s); overrides --settings"
        % ", ".join(f"{mode}={flavor}" for mode, (flavor, _) in MODE_DEFAULTS.items()),
    )
    parser.add_argument(
        "--timeout",
        help="job timeout such as 30m or 24h (default per mode: %s); overrides --settings"
        % ", ".join(f"{mode}={timeout}" for mode, (_, timeout) in MODE_DEFAULTS.items()),
    )
    parser.add_argument(
        "--settings",
        type=Path,
        help=(
            "production-settings.json from a benchmark run; supplies the "
            "release, snapshot, flavor, timeout and resource settings and "
            "must match the current source tree"
        ),
    )
    return parser.parse_args(argv)


def build_config(
    args: argparse.Namespace,
    settings: ProductionSettings | None,
    *,
    source_root: Path,
) -> SubmissionConfig:
    """Explicit CLI values win; the settings file fills the rest."""
    if args.smoke:
        mode = "smoke"
    elif args.benchmark:
        mode = "benchmark"
    elif args.legacy_arxiv:
        mode = "legacy"
    else:
        mode = "datacite"
    return SubmissionConfig(
        source_root=source_root,
        smoke=mode == "smoke",
        force=bool(args.force),
        datacite_release=args.datacite_release
        or (settings.datacite_release if settings else None),
        mode=mode,
        flavor=args.flavor or (settings.flavor if settings else None),
        timeout=args.timeout or (settings.timeout if settings else None),
        resources=settings.resources if settings else ResourceSettings(),
        expected_snapshot_id=args.snapshot_id
        or (settings.snapshot_id if settings else None),
    )


def submission_summary(
    job: Any,
    config: SubmissionConfig,
    digest: str,
    settings: ProductionSettings | None,
) -> dict[str, Any]:
    """What the submitter can honestly report: the job identity and the
    submitted settings. Dataset commits come only from the job's RESULT_JSON."""
    return {
        "id": job.id,
        "url": job.url,
        "status": _status_value(job),
        "mode": config.mode,
        "flavor": resolved_flavor(config),
        "timeout": resolved_timeout(config),
        "source_tree_sha256": digest,
        "datacite_release": config.datacite_release,
        "expected_snapshot_id": config.expected_snapshot_id,
        "resources": asdict(config.resources),
        "benchmark_report_sha256": settings.benchmark_report_sha256 if settings else None,
        "labels": job_labels(config, digest),
    }


def main(
    argv: list[str] | None = None,
    services: SubmissionServices = DEFAULT_SERVICES,
) -> None:
    args = parse_args(argv)
    source_root = PROJECT_ROOT
    try:
        settings = load_settings(args.settings) if args.settings else None
        if settings is not None:
            digest = check_settings_source(settings, source_root)
        else:
            digest = runtime_source_digest(source_root)
        config = build_config(args, settings, source_root=source_root)
        job = submit(config, services)
    except (SettingsError, RuntimeError, ValueError) as error:
        raise SystemExit(f"submit_job: {error}") from error
    print(json.dumps(submission_summary(job, config, digest, settings), sort_keys=True))


if __name__ == "__main__":
    main()
