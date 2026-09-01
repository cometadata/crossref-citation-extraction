from pathlib import Path


def test_lockfiles_are_tracked_inputs() -> None:
    root = Path(__file__).resolve().parent.parent
    assert (root / "Cargo.lock").is_file()
    assert (root / "uv.lock").is_file()
    gitignore = (root / ".gitignore").read_text()
    assert "\nCargo.lock" not in gitignore
