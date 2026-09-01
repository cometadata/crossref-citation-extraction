# /// script
# requires-python = ">=3.11"
# dependencies = ["huggingface-hub>=1.23.0,<2", "requests>=2.32,<3"]
# ///
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, JobInfo, Volume, get_token, sync_job_volume

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


@dataclass(frozen=True)
class SubmissionConfig:
    source_root: Path
    smoke: bool
    force: bool
    datacite_release: str | None


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


def submit(
    config: SubmissionConfig,
    services: SubmissionServices = DEFAULT_SERVICES,
) -> JobInfo:
    hf_token = services.get_token()
    if not hf_token:
        raise RuntimeError("Run `hf auth login` before submitting")
    api = services.api_factory(token=hf_token)
    crossref_token = os.environ.get("CROSSREF_API_TOKEN")
    if not config.smoke and not crossref_token:
        if not mirror_probably_available(api, hf_token):
            raise RuntimeError(
                "affiliations mirror not found in the bucket and "
                "CROSSREF_API_TOKEN is not set for the fallback download"
            )
    if not config.source_root.is_dir():
        raise RuntimeError(f"Source root does not exist: {config.source_root}")
    if not JOB_SCRIPT.is_file():
        raise RuntimeError(f"Job script does not exist: {JOB_SCRIPT}")
    digest = runtime_source_digest(config.source_root)
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
            "/workspace",
            remote_name=f"crossref-arxiv-citations-runtime-v1-{digest[:12]}",
            read_only=True,
            namespace=NAMESPACE,
            token=hf_token,
        )
    volumes = [source_volume]
    if not config.smoke:
        volumes.append(
            services.volume_factory(
                type="bucket",
                source=BUCKET_ID,
                mount_path="/mnt/shared-storage",
                read_only=True,
            )
        )
    script_args = ["--source-root", "/workspace", "--source-digest", digest]
    if config.smoke:
        script_args.append("--smoke")
    else:
        script_args.extend(["--bucket-root", "/mnt/shared-storage"])
    if config.datacite_release:
        script_args.extend(["--datacite-release", config.datacite_release])
    if config.force:
        script_args.append("--force")
    secrets: dict[str, str] = {}
    if not config.smoke:
        secrets["HF_TOKEN"] = hf_token
        if crossref_token:
            secrets["CROSSREF_API_TOKEN"] = crossref_token
    return api.run_uv_job(
        str(JOB_SCRIPT),
        script_args=script_args,
        env={"HF_XET_HIGH_PERFORMANCE": "1"},
        secrets=secrets,
        flavor="cpu-basic" if config.smoke else "cpu-xl",
        timeout="30m" if config.smoke else "24h",
        labels={
            "pipeline": PIPELINE_LABEL,
            "mode": "smoke" if config.smoke else "production",
            "source": digest[:12],
        },
        volumes=volumes,
        namespace=NAMESPACE,
        token=hf_token,
    )


def _status_value(job: Any) -> str:
    status = getattr(job, "status", None)
    stage = getattr(status, "stage", status)
    return str(getattr(stage, "value", stage))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--datacite-release")
    args = parser.parse_args()
    config = SubmissionConfig(
        source_root=PROJECT_ROOT,
        smoke=args.smoke,
        force=args.force,
        datacite_release=args.datacite_release,
    )
    digest = runtime_source_digest(config.source_root)
    job = submit(config)
    print(
        json.dumps(
            {
                "flavor": str(getattr(job.flavor, "value", job.flavor)),
                "id": job.id,
                "mode": "smoke" if config.smoke else "production",
                "source": digest[:12],
                "status": _status_value(job),
                "url": job.url,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
