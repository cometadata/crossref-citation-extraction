import importlib.util
from pathlib import Path


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


def test_uv_header_lists_runtime_dependencies() -> None:
    text = (Path(__file__).resolve().parent.parent / "jobs" / "pipeline.py").read_text()
    header = text.split("# ///")[1]
    for dep in ("duckdb", "huggingface-hub", "pyarrow", "requests", "zstandard"):
        assert dep in header
