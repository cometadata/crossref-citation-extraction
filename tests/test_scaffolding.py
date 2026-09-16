import tomllib
from pathlib import Path

from citations_pipeline.paths import runtime_source_files

ROOT = Path(__file__).resolve().parent.parent


def test_lockfiles_are_tracked_inputs() -> None:
    root = Path(__file__).resolve().parent.parent
    assert (root / "Cargo.lock").is_file()
    assert (root / "uv.lock").is_file()
    gitignore = (root / ".gitignore").read_text()
    assert "\nCargo.lock" not in gitignore


def test_runtime_sources_cover_every_mode_module() -> None:
    names = {path.relative_to(ROOT).as_posix() for path in runtime_source_files(ROOT)}
    assert {
        "jobs/pipeline.py",
        "citations_pipeline/__init__.py",
        "citations_pipeline/pipeline.py",
        "citations_pipeline/full_pipeline.py",
        "citations_pipeline/datacite_smoke.py",
        "citations_pipeline/benchmark.py",
        "citations_pipeline/full_state.py",
        "Cargo.toml",
        "Cargo.lock",
    } <= names
    for pattern in ("citations_pipeline/**/*.py", "jobs/**/*.py"):
        for path in ROOT.glob(pattern):
            assert path.relative_to(ROOT).as_posix() in names


def test_runtime_sources_ship_declared_benches() -> None:
    names = {path.relative_to(ROOT).as_posix() for path in runtime_source_files(ROOT)}
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    benches = manifest.get("bench", [])
    assert benches, "Cargo.toml declares no [[bench]] targets"
    for bench in benches:
        target = bench.get("path", f"benches/{bench['name']}.rs")
        assert target in names


def test_runtime_sources_exclude_submitter_settings_and_tests() -> None:
    names = {path.relative_to(ROOT).as_posix() for path in runtime_source_files(ROOT)}
    assert "submit_job.py" not in names
    assert not any(name.endswith(".json") for name in names)
    assert not any(name.startswith("tests/") for name in names)
    assert not any(name.startswith("docs/") for name in names)
