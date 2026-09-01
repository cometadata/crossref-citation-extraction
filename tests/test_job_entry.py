import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


def load_job_module():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "job_pipeline_entry", root / "jobs" / "pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
