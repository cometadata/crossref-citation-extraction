import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.paths import runtime_source_digest
from citations_pipeline.pipeline import PipelineConfig

ROOT = Path(__file__).resolve().parent.parent


def load_job_module():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "job_pipeline_entry", root / "jobs" / "pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_args(root: Path = ROOT, digest: str | None = None) -> list[str]:
    return [
        "--source-root",
        str(root),
        "--source-digest",
        digest if digest is not None else runtime_source_digest(root),
    ]


RESOURCE_FLAG_VALUES = {
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
RESOURCE_SETTINGS = ResourceSettings(
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


def resource_flags() -> list[str]:
    return [item for flag, value in RESOURCE_FLAG_VALUES.items() for item in (flag, value)]


# --- argument parsing ---


def test_parse_args_defaults(monkeypatch) -> None:
    module = load_job_module()
    monkeypatch.setattr(
        "sys.argv",
        ["pipeline.py", "--source-root", "/workspace", "--source-digest", "d" * 64],
    )
    args = module.parse_args()
    assert args.work_root == Path("/tmp/crossref-arxiv-citations")
    assert args.bucket_root is None
    assert args.smoke is False and args.force is False
    assert args.mode == "datacite"
    assert args.snapshot_id is None
    assert args.flavor is None
    assert args.datacite_release is None
    for name in (
        "partitions",
        "writer_memory_mb",
        "result_batch_mb",
        "queue_batches",
        "aggregation_memory_mb",
        "merge_fan_in",
        "sort_workers",
        "lookup_memory_mb",
        "max_entry_mb",
        "index_sort_memory_mb",
        "duckdb_memory_mb",
    ):
        assert getattr(args, name) is None


def test_parse_args_accepts_every_submitter_argument(tmp_path: Path) -> None:
    from submit_job import SubmissionConfig, script_arguments

    module = load_job_module()
    config = SubmissionConfig(
        source_root=tmp_path,
        smoke=False,
        force=True,
        datacite_release="2026-07",
        mode="benchmark",
        flavor="cpu-performance",
        timeout="48h",
        resources=RESOURCE_SETTINGS,
        expected_snapshot_id="2026-06",
    )
    digest = "a" * 64
    args = module.parse_args(script_arguments(config, digest))
    assert args.source_root == Path("/workspace")
    assert args.source_digest == digest
    assert args.mode == "benchmark"
    assert args.bucket_root == Path("/mnt/shared-storage")
    assert args.datacite_release == "2026-07"
    assert args.snapshot_id == "2026-06"
    assert args.force is True
    assert args.flavor == "cpu-performance"
    assert args.smoke is False
    for name, value in RESOURCE_SETTINGS.__dict__.items():
        assert getattr(args, name) == value


def test_parse_args_accepts_smoke_submission(tmp_path: Path) -> None:
    from submit_job import SubmissionConfig, script_arguments

    module = load_job_module()
    config = SubmissionConfig(
        source_root=tmp_path, smoke=True, force=False, datacite_release=None
    )
    args = module.parse_args(script_arguments(config, "a" * 64))
    assert args.mode == "smoke" and args.smoke is True
    assert args.bucket_root is None
    assert args.flavor == "cpu-basic"


def test_legacy_smoke_flag_is_alias_for_mode_smoke() -> None:
    module = load_job_module()
    base = ["--source-root", "/workspace", "--source-digest", "d" * 64]
    args = module.parse_args([*base, "--smoke"])
    assert args.mode == "smoke" and args.smoke is True
    args = module.parse_args([*base, "--smoke", "--mode", "smoke"])
    assert args.mode == "smoke" and args.smoke is True
    args = module.parse_args([*base, "--mode", "smoke"])
    assert args.mode == "smoke" and args.smoke is True
    with pytest.raises(SystemExit):
        module.parse_args([*base, "--smoke", "--mode", "benchmark"])
    with pytest.raises(SystemExit):
        module.parse_args([*base, "--mode", "production"])
    with pytest.raises(SystemExit):
        module.parse_args([*base, "--partitions", "many"])


# --- config_from_args ---


def test_config_from_args_forwards_every_setting(monkeypatch, tmp_path: Path) -> None:
    module = load_job_module()
    monkeypatch.setenv("HF_TOKEN", "hf-secret")
    monkeypatch.setenv("CROSSREF_API_TOKEN", "crossref-secret")
    args = module.parse_args(
        [
            "--source-root", str(tmp_path / "src"),
            "--source-digest", "e" * 64,
            "--mode", "benchmark",
            "--bucket-root", "/mnt/shared-storage",
            "--work-root", str(tmp_path / "work"),
            "--datacite-release", "2026-07",
            "--snapshot-id", "2026-06",
            "--force",
            "--flavor", "cpu-xl",
            *resource_flags(),
        ]
    )
    config = module.config_from_args(args)
    assert isinstance(config, PipelineConfig)
    assert config.source_root == tmp_path / "src"
    assert config.source_digest == "e" * 64
    assert config.work_root == tmp_path / "work"
    assert config.bucket_root == Path("/mnt/shared-storage")
    assert config.smoke is False
    assert config.force is True
    assert config.datacite_release == "2026-07"
    assert config.hf_token == "hf-secret"
    assert config.crossref_token == "crossref-secret"
    assert len(config.run_id) == 32 and int(config.run_id, 16) >= 0
    assert config.mode == "benchmark"
    assert config.resources == RESOURCE_SETTINGS
    assert config.expected_snapshot_id == "2026-06"
    second = module.config_from_args(args)
    assert second.run_id != config.run_id


def test_config_from_args_defaults(monkeypatch) -> None:
    module = load_job_module()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.delenv("CROSSREF_API_TOKEN", raising=False)
    args = module.parse_args(
        ["--source-root", "/workspace", "--source-digest", "d" * 64, "--smoke"]
    )
    config = module.config_from_args(args)
    assert config.mode == "smoke" and config.smoke is True
    assert config.bucket_root is None
    assert config.hf_token is None and config.crossref_token is None
    assert config.resources == ResourceSettings()
    assert config.expected_snapshot_id is None
    assert config.work_root == Path("/tmp/crossref-arxiv-citations")


def test_config_from_args_rejects_invalid_resources() -> None:
    module = load_job_module()
    args = module.parse_args(
        [
            "--source-root", "/workspace",
            "--source-digest", "d" * 64,
            "--partitions", "3",
        ]
    )
    with pytest.raises(ValueError, match="partitions"):
        module.config_from_args(args)


# --- dispatch ---


class LegacyResult:
    def to_dict(self) -> dict:
        return {"status": "completed", "zeta": 1, "alpha": 2}


class Dispatch:
    """Record which runner ran, with what config and environment."""

    def __init__(self, monkeypatch, module) -> None:
        self.calls: list[tuple[str, PipelineConfig, str | None]] = []
        self.bootstrap: list[str] = []
        monkeypatch.setattr(
            "citations_pipeline.pipeline.run_pipeline", self.runner("legacy", LegacyResult())
        )
        monkeypatch.setattr(
            "citations_pipeline.full_pipeline.run_full_pipeline",
            self.runner("datacite", {"status": "completed", "publications": {}}),
        )
        monkeypatch.setattr(
            "citations_pipeline.datacite_smoke.run_datacite_smoke",
            self.runner("smoke", {"status": "smoke-completed", "counts": {"b": 1, "a": 2}}),
        )
        monkeypatch.setattr(
            "citations_pipeline.benchmark.run_benchmark",
            self.runner("benchmark", {"status": "benchmark-ready", "ready": True}),
        )
        monkeypatch.setattr(
            module, "_ensure_debian_packages", lambda: self.bootstrap.append("packages")
        )
        monkeypatch.setattr(module, "_ensure_rust", lambda: self.bootstrap.append("rust"))

    def runner(self, name: str, result):
        def run(config, *args, **kwargs):
            self.calls.append((name, config, os.environ.get("BENCHMARK_FLAVOR")))
            return result

        return run


def result_payload(out: str) -> dict:
    lines = [line for line in out.splitlines() if line.startswith("RESULT_JSON=")]
    assert len(lines) == 1, out
    text = lines[0].removeprefix("RESULT_JSON=")
    payload = json.loads(text)
    assert text == json.dumps(payload, sort_keys=True)
    return payload


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("legacy", {"status": "completed", "zeta": 1, "alpha": 2}),
        ("datacite", {"status": "completed", "publications": {}}),
        ("smoke", {"status": "smoke-completed", "counts": {"b": 1, "a": 2}}),
        ("benchmark", {"status": "benchmark-ready", "ready": True}),
    ],
)
def test_main_dispatches_each_mode(monkeypatch, capsys, mode: str, expected: dict) -> None:
    module = load_job_module()
    dispatch = Dispatch(monkeypatch, module)
    monkeypatch.delenv("BENCHMARK_FLAVOR", raising=False)
    module.main([*source_args(), "--mode", mode, "--partitions", "64"])
    assert [name for name, _, _ in dispatch.calls] == [mode]
    config = dispatch.calls[0][1]
    assert config.mode == mode
    assert config.smoke is (mode == "smoke")
    assert config.source_root == ROOT
    assert config.resources.partitions == 64
    assert dispatch.bootstrap == ["packages", "rust"]
    assert result_payload(capsys.readouterr().out) == expected


def test_main_exports_benchmark_flavor_before_dispatch(monkeypatch, capsys) -> None:
    module = load_job_module()
    dispatch = Dispatch(monkeypatch, module)
    monkeypatch.setenv("BENCHMARK_FLAVOR", "stale")
    module.main([*source_args(), "--mode", "benchmark", "--flavor", "cpu-performance"])
    assert dispatch.calls[0][2] == "cpu-performance"
    assert os.environ["BENCHMARK_FLAVOR"] == "cpu-performance"


def test_main_leaves_benchmark_flavor_alone_without_flag(monkeypatch, capsys) -> None:
    module = load_job_module()
    dispatch = Dispatch(monkeypatch, module)
    monkeypatch.delenv("BENCHMARK_FLAVOR", raising=False)
    module.main([*source_args(), "--mode", "benchmark"])
    assert dispatch.calls[0][2] is None
    assert "BENCHMARK_FLAVOR" not in os.environ


def test_main_verifies_source_before_bootstrap(monkeypatch, capsys) -> None:
    module = load_job_module()
    dispatch = Dispatch(monkeypatch, module)
    with pytest.raises(RuntimeError, match="digest"):
        module.main([*source_args(digest="f" * 64), "--mode", "smoke"])
    assert dispatch.bootstrap == []
    assert dispatch.calls == []
    out = capsys.readouterr().out
    assert "RESULT_JSON=" not in out
    assert '"event": "phase_failed"' in out


# --- bootstrap ---


def test_bootstrap_is_noop_when_tools_present(monkeypatch, capsys) -> None:
    module = load_job_module()
    calls: list[list[str]] = []

    def fake_run_checked(arguments):
        calls.append(arguments)
        return SimpleNamespace(stdout=f"{arguments[0]} 1.90.0\n")

    monkeypatch.setattr(module, "_run_checked", fake_run_checked)
    monkeypatch.setattr(
        module.shutil,
        "which",
        lambda name: None if name == "apt-get" else f"/usr/bin/{name}",
    )
    monkeypatch.delenv("CROSSREF_RUSTC_VERSION", raising=False)
    module._ensure_debian_packages()
    assert calls == []
    module._ensure_rust()
    assert calls == [["rustc", "--version"], ["cargo", "--version"]]
    assert os.environ["CROSSREF_RUSTC_VERSION"] == "rustc 1.90.0"
    assert os.environ["CROSSREF_CARGO_VERSION"] == "cargo 1.90.0"
    assert capsys.readouterr().err == ""


def test_debian_bootstrap_skips_without_root(monkeypatch, tmp_path: Path, capsys) -> None:
    module = load_job_module()
    marker = tmp_path / "debian_version"
    marker.write_text("13\n")
    monkeypatch.setattr(module, "DEBIAN_MARKER", marker)
    monkeypatch.setattr(module.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(module.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout=""),
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(module, "_run_checked", lambda arguments: calls.append(arguments))
    module._ensure_debian_packages()
    assert calls == []
    assert "cmake" in capsys.readouterr().err


def test_debian_bootstrap_installs_missing_packages_as_root(
    monkeypatch, tmp_path: Path
) -> None:
    module = load_job_module()
    marker = tmp_path / "debian_version"
    marker.write_text("13\n")
    monkeypatch.setattr(module, "DEBIAN_MARKER", marker)
    monkeypatch.setattr(module.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(module.os, "geteuid", lambda: 0)

    def dpkg(arguments, **kwargs):
        installed = arguments[-1] != "cmake"
        return SimpleNamespace(
            returncode=0 if installed else 1,
            stdout="install ok installed" if installed else "",
        )

    monkeypatch.setattr(module.subprocess, "run", dpkg)
    calls: list[list[str]] = []
    monkeypatch.setattr(module, "_run_checked", lambda arguments: calls.append(arguments))
    module._ensure_debian_packages()
    assert calls == [
        ["apt-get", "update"],
        ["apt-get", "install", "-y", "--no-install-recommends", "cmake"],
    ]


# --- helpers kept from the legacy entry ---


def test_run_checked_returns_completed_process_on_success(capsys) -> None:
    module = load_job_module()
    result = module._run_checked([sys.executable, "-c", "print('hello-stdout')"])
    assert result.returncode == 0
    assert result.stdout.strip() == "hello-stdout"
    captured = capsys.readouterr()
    assert captured.err == ""


def test_run_checked_surfaces_stderr_on_failure(capsys) -> None:
    module = load_job_module()
    marker = "E: Unable to locate package citations-marker"
    script = f"import sys; sys.stderr.write({marker!r} + chr(10)); sys.exit(3)"
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        module._run_checked([sys.executable, "-c", script])
    assert excinfo.value.returncode == 3
    captured = capsys.readouterr()
    assert marker in captured.err
    assert "exit code 3" in captured.err


def test_uv_header_lists_runtime_dependencies() -> None:
    text = (Path(__file__).resolve().parent.parent / "jobs" / "pipeline.py").read_text()
    header = text.split("# ///")[1]
    for dep in ("duckdb", "huggingface-hub", "pyarrow", "requests", "zstandard"):
        assert dep in header
    assert 'requires-python = ">=3.12"' in header
