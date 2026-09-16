import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

import submit_job
from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.paths import runtime_source_digest
from submit_job import (
    MODE_DEFAULTS,
    MODE_LABELS,
    SETTINGS_KEYS,
    ProductionSettings,
    SettingsError,
    SubmissionConfig,
    SubmissionServices,
    build_config,
    check_settings_source,
    load_settings,
    parse_args,
    script_arguments,
    submit,
)


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


def make_config(root: Path, **overrides) -> SubmissionConfig:
    values = {
        "source_root": root,
        "smoke": False,
        "force": False,
        "datacite_release": None,
    }
    values.update(overrides)
    return SubmissionConfig(**values)


def value_after(args: list[str], flag: str) -> str:
    return args[args.index(flag) + 1]


def settings_payload(root: Path, **overrides) -> dict:
    payload = {
        "source_tree_sha256": runtime_source_digest(root),
        "datacite_release": "2026-07",
        "snapshot_id": "2026-07",
        "flavor": "cpu-performance",
        "timeout": "48h",
        "resources": asdict(ResourceSettings(partitions=512, sort_workers=8)),
        "benchmark_report_sha256": "b" * 64,
    }
    payload.update(overrides)
    return payload


def write_settings(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "production-settings.json"
    path.write_text(json.dumps(payload))
    return path


# --- the brief's verbatim test ---


def test_full_submission_forwards_selected_resources(tmp_path, monkeypatch):
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = SubmissionConfig(source_root=make_source_root(tmp_path), smoke=False,
        force=False, datacite_release="2026-07", mode="datacite", flavor="cpu-xl",
        timeout="24h", resources=ResourceSettings(partitions=512),
        expected_snapshot_id="2026-07")
    submit(config, make_services(api))
    args = api.run_kwargs["script_args"]
    assert args[args.index("--mode") + 1] == "datacite"
    assert args[args.index("--partitions") + 1] == "512"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-datacite-citations"
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"


# --- modes, defaults, labels ---


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
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--mode") == "smoke"
    assert "--bucket-root" not in script_args
    assert api.run_kwargs["labels"] == {
        "pipeline": "crossref-datacite-smoke",
        "mode": "smoke",
        "source": runtime_source_digest(config.source_root)[:12],
    }
    assert api.list_calls == []


def test_default_mode_is_full_datacite(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(make_source_root(tmp_path))
    assert config.mode == "datacite"
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {"HF_TOKEN": "hf-token"}
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-datacite-citations"
    assert api.run_kwargs["labels"]["mode"] == "datacite"
    assert api.run_kwargs["namespace"] == "cometadata"
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--mode") == "datacite"
    assert value_after(script_args, "--bucket-root") == "/mnt/shared-storage"
    bucket_volumes = [
        volume
        for volume in api.run_kwargs["volumes"]
        if getattr(volume, "kind", "") == "bucket"
    ]
    assert len(bucket_volumes) == 1
    assert bucket_volumes[0].read_only is True
    assert bucket_volumes[0].mount_path == "/mnt/shared-storage"


def test_legacy_mode_keeps_label_and_defaults(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(make_source_root(tmp_path), mode="legacy")
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {"HF_TOKEN": "hf-token"}
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-arxiv-citations"
    assert api.run_kwargs["labels"]["mode"] == "legacy"
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--mode") == "legacy"
    assert "--bucket-root" in script_args


def test_benchmark_mode_labels_and_defaults(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(make_source_root(tmp_path), mode="benchmark")
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {"HF_TOKEN": "hf-token"}
    assert api.run_kwargs["flavor"] == "cpu-xl"
    assert api.run_kwargs["timeout"] == "24h"
    assert api.run_kwargs["labels"]["pipeline"] == "crossref-datacite-benchmark"
    assert api.run_kwargs["labels"]["mode"] == "benchmark"
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--mode") == "benchmark"
    assert value_after(script_args, "--bucket-root") == "/mnt/shared-storage"
    assert any(getattr(v, "kind", "") == "bucket" for v in api.run_kwargs["volumes"])


def test_smoke_forwards_no_secrets_even_with_a_crossref_token(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    api = FakeApi(bucket_files=[])
    submit(make_config(make_source_root(tmp_path), smoke=True), make_services(api))
    assert api.run_kwargs["secrets"] == {}
    assert "crossref-secret" not in json.dumps(api.run_kwargs["script_args"])
    assert api.list_calls == []


@pytest.mark.parametrize("mode", ["legacy", "benchmark"])
def test_bucket_modes_forward_the_crossref_token_when_set(
    tmp_path: Path, monkeypatch, mode: str
) -> None:
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    api = FakeApi(bucket_files=MIRROR_FILES)
    submit(make_config(make_source_root(tmp_path), mode=mode), make_services(api))
    assert api.run_kwargs["secrets"] == {
        "HF_TOKEN": "hf-token",
        "CROSSREF_API_TOKEN": "crossref-secret",
    }
    assert "crossref-secret" not in json.dumps(api.run_kwargs["script_args"])
    assert "crossref-secret" not in json.dumps(api.run_kwargs["env"])


@pytest.mark.parametrize(
    "flags", [["--smoke"], ["--legacy-arxiv"], ["--benchmark"], []]
)
def test_tokens_never_appear_on_stdout(
    tmp_path: Path, monkeypatch, capsys, flags: list[str]
) -> None:
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)
    api = FakeApi(bucket_files=MIRROR_FILES)
    submit_job.main(flags, services=make_services(api))
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["id"] == "job-1"
    for secret in ("crossref-secret", "hf-token"):
        assert secret not in captured.out
        assert secret not in captured.err


def test_mode_defaults_table() -> None:
    assert MODE_DEFAULTS == {
        "legacy": ("cpu-xl", "24h"),
        "datacite": ("cpu-xl", "24h"),
        "smoke": ("cpu-basic", "30m"),
        "benchmark": ("cpu-xl", "24h"),
    }
    assert set(MODE_LABELS) == set(MODE_DEFAULTS)


def test_flavor_and_timeout_override_mode_defaults(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(
        make_source_root(tmp_path),
        mode="benchmark",
        flavor="cpu-performance",
        timeout="48h",
    )
    submit(config, make_services(api))
    assert api.run_kwargs["flavor"] == "cpu-performance"
    assert api.run_kwargs["timeout"] == "48h"
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--flavor") == "cpu-performance"


def test_smoke_flavor_override(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=[])
    config = make_config(make_source_root(tmp_path), smoke=True, flavor="cpu-upgrade")
    submit(config, make_services(api))
    assert api.run_kwargs["flavor"] == "cpu-upgrade"
    assert api.run_kwargs["timeout"] == "30m"
    assert api.run_kwargs["secrets"] == {}


# --- config validation ---


def test_config_rejects_unknown_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="mode"):
        make_config(make_source_root(tmp_path), mode="production")


def test_config_smoke_flag_derives_smoke_mode(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    config = make_config(root, smoke=True)
    assert config.mode == "smoke"
    config = make_config(root, mode="smoke")
    assert config.smoke is True
    config = make_config(root, smoke=True, mode="smoke")
    assert config.mode == "smoke" and config.smoke is True


def test_config_smoke_flag_conflicts_with_other_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="smoke"):
        make_config(make_source_root(tmp_path), smoke=True, mode="benchmark")


def test_config_rejects_malformed_timeout(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    with pytest.raises(ValueError, match="timeout"):
        make_config(root, timeout="one day")
    with pytest.raises(ValueError, match="flavor"):
        make_config(root, flavor="")


def test_config_defaults(tmp_path: Path) -> None:
    config = make_config(make_source_root(tmp_path))
    assert config.flavor is None
    assert config.timeout is None
    assert config.resources == ResourceSettings()
    assert config.expected_snapshot_id is None


# --- secrets and the mirror probe ---


def test_production_without_mirror_or_token_fails(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=[])
    config = make_config(make_source_root(tmp_path))
    with pytest.raises(RuntimeError, match="CROSSREF_API_TOKEN"):
        submit(config, make_services(api))
    assert api.run_kwargs is None


def test_production_without_mirror_uses_fallback_token(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    api = FakeApi(bucket_files=[])
    config = make_config(make_source_root(tmp_path))
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {
        "HF_TOKEN": "hf-token",
        "CROSSREF_API_TOKEN": "crossref-secret",
    }


def test_crossref_token_forwarded_when_mirror_available(
    tmp_path: Path, monkeypatch
) -> None:
    # A mirror present at submission may be gone at runtime; the job keeps
    # the authenticated fallback whenever the submitter has the token.
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(make_source_root(tmp_path))
    submit(config, make_services(api))
    assert api.run_kwargs["secrets"] == {
        "HF_TOKEN": "hf-token",
        "CROSSREF_API_TOKEN": "crossref-secret",
    }
    assert api.list_calls == [
        {"bucket_id": "cometadata/shared-storage", "prefix": "crossref-affiliations-latest"}
    ]


def test_benchmark_without_mirror_or_token_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=[])
    config = make_config(make_source_root(tmp_path), mode="benchmark")
    with pytest.raises(RuntimeError, match="CROSSREF_API_TOKEN"):
        submit(config, make_services(api))


def test_missing_hf_login_is_refused_before_any_call(tmp_path: Path) -> None:
    api = FakeApi(bucket_files=MIRROR_FILES)
    services = SubmissionServices(
        get_token=lambda: None,
        sync_job_volume=make_services(api).sync_job_volume,
        volume_factory=make_services(api).volume_factory,
        api_factory=lambda token: api,
    )
    with pytest.raises(RuntimeError, match="hf auth login"):
        submit(make_config(make_source_root(tmp_path)), services)
    assert api.run_kwargs is None


# --- source volume and forwarded arguments ---


def test_source_volume_name_uses_digest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    volumes: list = []
    config = make_config(make_source_root(tmp_path), smoke=True)
    submit(config, make_services(api, volumes))
    digest = runtime_source_digest(config.source_root)
    assert volumes[0].remote_name == (
        f"crossref-arxiv-citations-runtime-v1-{digest[:12]}"
    )
    assert volumes[0].read_only is True
    assert volumes[0].namespace == "cometadata"


def test_staged_volume_excludes_settings_and_submitter(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    root = make_source_root(tmp_path)
    (root / "production-settings.json").write_text("{}")
    (root / "submit_job.py").write_text("# submitter\n")
    (root / "citations_pipeline" / "benchmark.py").write_text("# bench\n")
    (root / "benches").mkdir()
    (root / "benches" / "extraction_bench.rs").write_text("fn b() {}\n")
    api = FakeApi(bucket_files=MIRROR_FILES)
    staged: list[Path] = []

    def sync_job_volume(staging_root, mount_path, **kwargs):
        staged.extend(
            p.relative_to(staging_root)
            for p in Path(staging_root).rglob("*")
            if p.is_file()
        )
        return SimpleNamespace(kind="source", **kwargs)

    services = SubmissionServices(
        get_token=lambda: "hf-token",
        sync_job_volume=sync_job_volume,
        volume_factory=lambda **kwargs: SimpleNamespace(kind="bucket", **kwargs),
        api_factory=lambda token: api,
    )
    submit(make_config(root), services)
    names = {path.as_posix() for path in staged}
    assert "citations_pipeline/benchmark.py" in names
    assert "benches/extraction_bench.rs" in names
    assert "jobs/pipeline.py" in names
    assert "production-settings.json" not in names
    assert "submit_job.py" not in names


def test_datacite_release_forwarded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    config = make_config(
        make_source_root(tmp_path), force=True, datacite_release="2026-06"
    )
    submit(config, make_services(api))
    script_args = api.run_kwargs["script_args"]
    assert value_after(script_args, "--datacite-release") == "2026-06"
    assert "--force" in script_args


def test_force_not_forwarded_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    api = FakeApi(bucket_files=MIRROR_FILES)
    submit(make_config(make_source_root(tmp_path)), make_services(api))
    assert "--force" not in api.run_kwargs["script_args"]


def test_script_arguments_forward_every_setting_individually(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    resources = ResourceSettings(
        partitions=128,
        writer_memory_mb=2048,
        result_batch_mb=8,
        queue_batches=4,
        aggregation_memory_mb=8192,
        merge_fan_in=16,
        sort_workers=2,
        lookup_memory_mb=32,
        max_entry_mb=256,
        index_sort_memory_mb=1024,
        duckdb_memory_mb=16384,
    )
    config = make_config(
        root,
        mode="datacite",
        force=True,
        datacite_release="2026-07",
        expected_snapshot_id="2026-06",
        resources=resources,
    )
    digest = "a" * 64
    args = script_arguments(config, digest)
    assert args[:6] == [
        "--source-root", "/workspace", "--source-digest", digest, "--mode", "datacite",
    ]
    assert value_after(args, "--bucket-root") == "/mnt/shared-storage"
    assert value_after(args, "--datacite-release") == "2026-07"
    assert value_after(args, "--snapshot-id") == "2026-06"
    assert value_after(args, "--flavor") == "cpu-xl"
    assert "--force" in args
    expected_flags = {
        "--partitions": "128",
        "--writer-memory-mb": "2048",
        "--result-batch-mb": "8",
        "--queue-batches": "4",
        "--aggregation-memory-mb": "8192",
        "--merge-fan-in": "16",
        "--sort-workers": "2",
        "--lookup-memory-mb": "32",
        "--max-entry-mb": "256",
        "--index-sort-memory-mb": "1024",
        "--duckdb-memory-mb": "16384",
    }
    for flag, value in expected_flags.items():
        assert value_after(args, flag) == value
    assert all(" " not in argument for argument in args)
    assert all("=" not in argument for argument in args)


def test_script_arguments_omit_optional_values(tmp_path: Path) -> None:
    config = make_config(make_source_root(tmp_path), smoke=True)
    args = script_arguments(config, "a" * 64)
    assert "--bucket-root" not in args
    assert "--datacite-release" not in args
    assert "--snapshot-id" not in args
    assert "--force" not in args
    assert "--smoke" not in args
    assert value_after(args, "--mode") == "smoke"
    assert value_after(args, "--flavor") == "cpu-basic"


def test_script_arguments_reject_invalid_resources(tmp_path: Path) -> None:
    config = make_config(
        make_source_root(tmp_path), resources=ResourceSettings(partitions=3)
    )
    with pytest.raises(ValueError, match="partitions"):
        script_arguments(config, "a" * 64)


# --- settings contract ---


def test_settings_keys_match_benchmark_contract() -> None:
    from citations_pipeline import benchmark

    assert set(SETTINGS_KEYS) == set(benchmark.SETTINGS_KEYS)


def test_load_settings_round_trip(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    settings = load_settings(write_settings(tmp_path, settings_payload(root)))
    assert settings == ProductionSettings(
        source_tree_sha256=runtime_source_digest(root),
        datacite_release="2026-07",
        snapshot_id="2026-07",
        flavor="cpu-performance",
        timeout="48h",
        resources=ResourceSettings(partitions=512, sort_workers=8),
        benchmark_report_sha256="b" * 64,
    )


def test_load_settings_benchmark_report_digest_is_optional(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    payload = settings_payload(root)
    del payload["benchmark_report_sha256"]
    settings = load_settings(write_settings(tmp_path, payload))
    assert settings.benchmark_report_sha256 is None


def test_load_settings_rejects_unknown_key(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    payload = settings_payload(root, extra_key=1)
    with pytest.raises(SettingsError, match="extra_key"):
        load_settings(write_settings(tmp_path, payload))


def test_load_settings_rejects_missing_key(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    payload = settings_payload(root)
    del payload["timeout"]
    with pytest.raises(SettingsError, match="timeout"):
        load_settings(write_settings(tmp_path, payload))


def test_load_settings_rejects_bad_values(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    cases = {
        "source_tree_sha256": "not-a-digest",
        "snapshot_id": "July 2026",
        "datacite_release": 202607,
        "flavor": "",
        "timeout": "soon",
        "benchmark_report_sha256": "xyz",
        "resources": [1, 2, 3],
    }
    for key, value in cases.items():
        payload = settings_payload(root, **{key: value})
        with pytest.raises(SettingsError, match=key):
            load_settings(write_settings(tmp_path, payload))


def test_load_settings_rejects_unknown_resource_key(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    payload = settings_payload(root)
    payload["resources"]["threads"] = 4
    with pytest.raises(SettingsError, match="threads"):
        load_settings(write_settings(tmp_path, payload))


def test_load_settings_rejects_invalid_resources(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    payload = settings_payload(root)
    payload["resources"]["partitions"] = 100
    with pytest.raises(SettingsError, match="partitions"):
        load_settings(write_settings(tmp_path, payload))


def test_load_settings_rejects_non_object_and_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "settings.json"
    path.write_text("[]")
    with pytest.raises(SettingsError, match="object"):
        load_settings(path)
    with pytest.raises(SettingsError, match="settings"):
        load_settings(tmp_path / "absent.json")


def test_settings_digest_mismatch_is_refused(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    settings = load_settings(
        write_settings(tmp_path, settings_payload(root, source_tree_sha256="c" * 64))
    )
    with pytest.raises(SettingsError, match="source tree"):
        check_settings_source(settings, root)
    (root / "citations_pipeline" / "new_module.py").write_text("X = 1\n")
    current = load_settings(write_settings(tmp_path, settings_payload(root)))
    assert check_settings_source(current, root) == runtime_source_digest(root)


# --- CLI ---


def test_parse_args_modes_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--smoke", "--benchmark"])
    with pytest.raises(SystemExit):
        parse_args(["--legacy-arxiv", "--smoke"])
    args = parse_args([])
    assert not args.smoke and not args.benchmark and not args.legacy_arxiv
    assert args.settings is None and args.flavor is None and args.timeout is None
    assert args.snapshot_id is None


def test_build_config_defaults_to_datacite(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    config = build_config(parse_args([]), None, source_root=root)
    assert config == make_config(root)
    assert build_config(parse_args(["--smoke"]), None, source_root=root).mode == "smoke"
    assert (
        build_config(parse_args(["--benchmark"]), None, source_root=root).mode
        == "benchmark"
    )
    assert (
        build_config(parse_args(["--legacy-arxiv"]), None, source_root=root).mode
        == "legacy"
    )


def test_build_config_applies_settings(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    settings = load_settings(write_settings(tmp_path, settings_payload(root)))
    config = build_config(parse_args([]), settings, source_root=root)
    assert config.mode == "datacite"
    assert config.datacite_release == "2026-07"
    assert config.expected_snapshot_id == "2026-07"
    assert config.flavor == "cpu-performance"
    assert config.timeout == "48h"
    assert config.resources == ResourceSettings(partitions=512, sort_workers=8)


def test_cli_values_override_settings(tmp_path: Path) -> None:
    root = make_source_root(tmp_path)
    settings = load_settings(write_settings(tmp_path, settings_payload(root)))
    args = parse_args(
        [
            "--benchmark",
            "--flavor", "cpu-xl",
            "--timeout", "24h",
            "--snapshot-id", "2026-08",
            "--datacite-release", "2026-08",
            "--force",
        ]
    )
    config = build_config(args, settings, source_root=root)
    assert config.mode == "benchmark"
    assert config.flavor == "cpu-xl"
    assert config.timeout == "24h"
    assert config.expected_snapshot_id == "2026-08"
    assert config.datacite_release == "2026-08"
    assert config.force is True
    assert config.resources == settings.resources


def test_main_prints_submission_json(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)
    settings_path = write_settings(tmp_path, settings_payload(root))
    api = FakeApi(bucket_files=MIRROR_FILES)
    submit_job.main(["--settings", str(settings_path)], services=make_services(api))
    out = capsys.readouterr().out
    payload = json.loads(out)
    digest = runtime_source_digest(root)
    assert payload == {
        "id": "job-1",
        "url": "https://huggingface.co/jobs/cometadata/job-1",
        "status": "PENDING",
        "mode": "datacite",
        "flavor": "cpu-performance",
        "timeout": "48h",
        "source_tree_sha256": digest,
        "datacite_release": "2026-07",
        "expected_snapshot_id": "2026-07",
        "resources": asdict(ResourceSettings(partitions=512, sort_workers=8)),
        "benchmark_report_sha256": "b" * 64,
        "labels": {
            "pipeline": "crossref-datacite-citations",
            "mode": "datacite",
            "source": digest[:12],
        },
    }
    assert "hf-token" not in out
    assert api.run_kwargs["flavor"] == "cpu-performance"
    assert api.run_kwargs["timeout"] == "48h"
    assert value_after(api.run_kwargs["script_args"], "--partitions") == "512"
    assert value_after(api.run_kwargs["script_args"], "--snapshot-id") == "2026-07"


def test_main_keeps_stdout_to_the_submission_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    # huggingface_hub narrates the volume sync and the script upload on
    # stdout; the submitter must leave stdout parseable as one JSON document,
    # which is how `json.loads(Path(...).read_text())` inspects it.
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)

    class ChattyApi(FakeApi):
        def run_uv_job(self, script, **kwargs):
            print("Your script and Job artifacts will be saved in this bucket: hf://x")
            return super().run_uv_job(script, **kwargs)

    api = ChattyApi(bucket_files=[])
    quiet = make_services(api)

    def chatty_sync_job_volume(staging_root, mount_path, **kwargs):
        print("Sync plan: staging -> hf://buckets/cometadata/jobs-artifacts/runtime")
        print("Sync completed.")
        return quiet.sync_job_volume(staging_root, mount_path, **kwargs)

    services = SubmissionServices(
        get_token=quiet.get_token,
        sync_job_volume=chatty_sync_job_volume,
        volume_factory=quiet.volume_factory,
        api_factory=quiet.api_factory,
    )
    submit_job.main(["--smoke"], services=services)
    captured = capsys.readouterr()

    job = SimpleNamespace(
        id="job-1",
        url="https://huggingface.co/jobs/cometadata/job-1",
        status=SimpleNamespace(stage="PENDING"),
    )
    expected = submit_job.submission_summary(
        job,
        build_config(parse_args(["--smoke"]), None, source_root=root),
        runtime_source_digest(root),
        None,
    )
    assert json.loads(captured.out) == expected
    assert captured.out == json.dumps(expected, sort_keys=True) + "\n"
    assert "Sync plan" in captured.err and "Sync completed." in captured.err
    assert "artifacts will be saved" in captured.err
    assert api.run_kwargs is not None


def test_main_without_settings_reports_null_report_digest(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)
    api = FakeApi(bucket_files=[])
    submit_job.main(["--smoke", "--timeout", "45m"], services=make_services(api))
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "smoke"
    assert payload["flavor"] == "cpu-basic"
    assert payload["timeout"] == "45m"
    assert payload["benchmark_report_sha256"] is None
    assert payload["datacite_release"] is None
    assert payload["expected_snapshot_id"] is None
    assert payload["resources"] == asdict(ResourceSettings())
    assert payload["labels"]["pipeline"] == "crossref-datacite-smoke"


def test_main_refuses_stale_settings_before_submitting(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)
    settings_path = write_settings(
        tmp_path, settings_payload(root, source_tree_sha256="c" * 64)
    )
    api = FakeApi(bucket_files=MIRROR_FILES)
    with pytest.raises(SystemExit) as excinfo:
        submit_job.main(["--settings", str(settings_path)], services=make_services(api))
    assert "source tree" in str(excinfo.value)
    assert api.run_kwargs is None
    assert capsys.readouterr().out == ""


def test_main_reports_settings_errors_without_traceback(
    tmp_path: Path, monkeypatch
) -> None:
    root = make_source_root(tmp_path)
    monkeypatch.setattr(submit_job, "PROJECT_ROOT", root)
    settings_path = write_settings(tmp_path, settings_payload(root, mystery=True))
    api = FakeApi(bucket_files=MIRROR_FILES)
    with pytest.raises(SystemExit) as excinfo:
        submit_job.main(["--settings", str(settings_path)], services=make_services(api))
    assert "mystery" in str(excinfo.value)
    assert api.run_kwargs is None
