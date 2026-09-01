from pathlib import Path
from types import SimpleNamespace

import pytest

from submit_job import SubmissionConfig, SubmissionServices, submit


class FakeApi:
    def __init__(self, bucket_files: list[str]) -> None:
        self.bucket_files = bucket_files
        self.run_kwargs: dict | None = None
        self.list_calls: list[dict] = []

    def list_bucket_tree(self, bucket_id, *, prefix, recursive, token):
        self.list_calls.append({"bucket_id": bucket_id, "prefix": prefix})
        return [
            SimpleNamespace(path=path, type="file") for path in self.bucket_files
        ]

    def run_uv_job(self, script, **kwargs):
        self.run_kwargs = {"script": script, **kwargs}
        return SimpleNamespace(
            id="job-1",
            url="https://huggingface.co/jobs/cometadata/job-1",
            status=SimpleNamespace(stage="PENDING"),
            flavor=kwargs["flavor"],
        )


MIRROR_FILES = [
    "crossref-affiliations-latest/_PIPELINE_COMPLETE.json",
    "crossref-affiliations-latest/raw-generations/2026-07-abc/all.json.tar.gz",
]


def make_services(api: FakeApi, volumes: list | None = None):
    def sync_job_volume(staging_root, mount_path, **kwargs):
        volume = SimpleNamespace(kind="source", staging=Path(staging_root), **kwargs)
        if volumes is not None:
            volumes.append(volume)
        return volume

    return SubmissionServices(
        get_token=lambda: "hf-token",
        sync_job_volume=sync_job_volume,
        volume_factory=lambda **kwargs: SimpleNamespace(kind="bucket", **kwargs),
        api_factory=lambda token: api,
    )


def make_source_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    (root / "jobs").mkdir(parents=True)
    (root / "jobs" / "pipeline.py").write_text("# job\n")
    (root / "citations_pipeline").mkdir()
    (root / "citations_pipeline" / "__init__.py").write_text("")
    (root / "src").mkdir()
    (root / "src" / "main.rs").write_text("fn main() {}\n")
    (root / "Cargo.toml").write_text("[package]\n")
    (root / "Cargo.lock").write_text("# lock\n")
    return root


def test_smoke_submit_has_no_secrets_or_bucket(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=[])
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=True,
        force=False,
        datacite_release=None,
    )
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {}
    assert api.run_kwargs["flavor"] == "cpu-basic"
    assert api.run_kwargs["timeout"] == "30m"
    assert all(
        getattr(volume, "kind", "") != "bucket"
        for volume in api.run_kwargs["volumes"]
    )
    assert "--smoke" in api.run_kwargs["script_args"]


def test_production_with_mirror_needs_no_crossref_token(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=False,
        force=False,
        datacite_release=None,
    )
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {"HF_TOKEN": "hf-token"}
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-arxiv-citations"
    bucket_volumes = [
        volume
        for volume in api.run_kwargs["volumes"]
        if getattr(volume, "kind", "") == "bucket"
    ]
    assert len(bucket_volumes) == 1
    assert bucket_volumes[0].read_only is True
    assert bucket_volumes[0].mount_path == "/mnt/shared-storage"
    assert "--bucket-root" in api.run_kwargs["script_args"]


def test_production_without_mirror_or_token_fails(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=[])
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=False,
        force=False,
        datacite_release=None,
    )
    with pytest.raises(RuntimeError, match="CROSSREF_API_TOKEN"):
        submit(config, make_services(api))
    assert api.run_kwargs is None


def test_production_without_mirror_uses_fallback_token(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    api = FakeApi(bucket_files=[])
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=False,
        force=False,
        datacite_release=None,
    )
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"]["CROSSREF_API_TOKEN"] == "crossref-secret"


def test_source_volume_name_uses_digest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    volumes: list = []
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=True,
        force=False,
        datacite_release=None,
    )
    submit(config, make_services(api, volumes))
    from citations_pipeline.paths import runtime_source_digest

    digest = runtime_source_digest(config.source_root)
    assert volumes[0].remote_name == (
        f"crossref-arxiv-citations-runtime-v1-{digest[:12]}"
    )
    assert volumes[0].read_only is True


def test_datacite_release_forwarded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = SubmissionConfig(
        source_root=make_source_root(tmp_path),
        smoke=False,
        force=True,
        datacite_release="2026-06",
    )
    submit(config, make_services(api))
    script_args = api.run_kwargs["script_args"]
    assert script_args[script_args.index("--datacite-release") + 1] == "2026-06"
    assert "--force" in script_args
