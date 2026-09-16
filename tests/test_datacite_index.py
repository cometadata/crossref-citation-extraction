from __future__ import annotations

import hashlib
import json
import re
import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pytest
from conftest import (
    FULL_RELEASE_MONTH,
    csv_path,
    make_full_datacite_release,
    part_path,
    write_full_datacite_release,
)

from citations_pipeline.bucket import LocalBucketStore
from citations_pipeline.datacite_index import (
    BUNDLE_FILES,
    BUNDLE_MARKER_NAME,
    BUNDLE_POINTER_NAME,
    BundleResult,
    DataciteIndexError,
    DataciteRelease,
    discover_release,
    ensure_bundle,
    release_inventory_sha256,
)
from citations_pipeline.extractor import (
    ExtractorBinary,
    ExtractorError,
    run_build_datacite_index,
)
from citations_pipeline.paths import (
    DATACITE_PREFIX_NAME,
    GenerationPathError,
    PipelinePaths,
    bundle_prefix,
)
from citations_pipeline.state import read_json, sha256_file
from citations_pipeline.telemetry import EventLogger

MONTH = FULL_RELEASE_MONTH
# Pinned by the Rust test `inventory_digest_matches_the_cross_language_fixture`
# in tests/datacite_release.rs: part_0000.jsonl.gz = b"x", 2026-07.csv.gz = b"yz".
RUST_FIXTURE_INVENTORY = "eece7282f43778a37b55e1481455c501c7eb72adb5a4bba7f6dbfff2d701621b"
# Pinned by the same Rust test: the fixture above plus a folder dated after the
# release month, part_0000.jsonl.gz = b"later", 2026-08.csv.gz = b"rows".
LATER_FOLDER_FIXTURE_INVENTORY = (
    "f4ce7500bce908603e88efe1e7285ccff4d2ebf628553c86654179bcf26bcb01"
)
LATER_FOLDER_FILES = [
    (part_path(MONTH, 0), b"x"),
    (csv_path(MONTH), b"yz"),
    (part_path("2026-08", 0), b"later"),
    (csv_path("2026-08"), b"rows"),
]


# --- inventory --------------------------------------------------------------


def test_inventory_digest_matches_the_rust_fixture(tmp_path: Path) -> None:
    root = tmp_path / "release"
    write_full_datacite_release(
        root, MONTH, [(part_path(MONTH, 0), b"x"), (csv_path(MONTH), b"yz")]
    )
    assert release_inventory_sha256(root, MONTH) == RUST_FIXTURE_INVENTORY


def test_inventory_serialization_is_compact_sorted_rows(tmp_path: Path) -> None:
    root = tmp_path / "release"
    write_full_datacite_release(
        root, MONTH, [(part_path(MONTH, 0), b"x"), (csv_path(MONTH), b"yz")]
    )
    rows = [
        [csv_path(MONTH), 2, hashlib.sha256(b"yz").hexdigest()],
        [part_path(MONTH, 0), 1, hashlib.sha256(b"x").hexdigest()],
    ]
    expected = hashlib.sha256(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    assert release_inventory_sha256(root, MONTH) == expected == RUST_FIXTURE_INVENTORY


def test_inventory_ignores_non_input_entries_and_manifest_order(tmp_path: Path) -> None:
    root = tmp_path / "release"
    write_full_datacite_release(
        root,
        MONTH,
        [
            (csv_path(MONTH), b"yz"),
            (f"dois/updated_{MONTH}/README.txt", b"notes"),
            ("index.html", b"<html>"),
            (part_path(MONTH, 0), b"x"),
        ],
    )
    assert release_inventory_sha256(root, MONTH) == RUST_FIXTURE_INVENTORY


def test_inventory_includes_folders_dated_after_the_release_month(tmp_path: Path) -> None:
    # The real 2026-07 data file, generated on 1 August, lists
    # `dois/updated_2026-08/` for the DOIs updated before the snapshot. The
    # folder is an ordinary input and the digest agrees with the Rust reader.
    root = tmp_path / "release"
    write_full_datacite_release(root, MONTH, LATER_FOLDER_FILES)
    rows = [
        [path, len(data), hashlib.sha256(data).hexdigest()]
        for path, data in sorted(LATER_FOLDER_FILES)
    ]
    expected = hashlib.sha256(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    assert (
        release_inventory_sha256(root, MONTH)
        == expected
        == LATER_FOLDER_FIXTURE_INVENTORY
    )
    # The inventory depends only on the manifest, never on the release month.
    assert release_inventory_sha256(root, "2026-06") == LATER_FOLDER_FIXTURE_INVENTORY
    assert release_inventory_sha256(root, "2026-08") == LATER_FOLDER_FIXTURE_INVENTORY


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ([(part_path(MONTH, 0), b"x")], "csv"),
        ([(part_path("2026-06", 0), b"x")], "csv"),
        (
            [
                (part_path(MONTH, 0), b"x"),
                (csv_path(MONTH), b"yz"),
                (part_path("2026-08", 0), b"later"),
            ],
            "updated_2026-08 lists JSONL",
        ),
        ([(f"dois/updated_{MONTH}/nested/part.jsonl.gz", b"x")], "updated_"),
        ([(f"other/updated_{MONTH}/{MONTH}.csv.gz", b"yz")], "dois"),
        ([("dois/updated_2026-13/2026-13.csv.gz", b"yz")], "updated_"),
        ([(f"dois/updated_{MONTH}/a\\b.csv.gz", b"yz")], "file name"),
        ([], "no"),
        ([("notes.txt", b"n")], "no"),
    ],
)
def test_inventory_rejects_invalid_manifests(
    tmp_path: Path, files: list[tuple[str, bytes]], message: str
) -> None:
    root = tmp_path / "release"
    write_full_datacite_release(root, MONTH, files)
    with pytest.raises(DataciteIndexError, match=message):
        release_inventory_sha256(root, MONTH)


def test_inventory_rejects_duplicate_entries_and_bad_digests(tmp_path: Path) -> None:
    root = tmp_path / "release"
    write_full_datacite_release(root, MONTH, [(csv_path(MONTH), b"yz")])
    manifest = json.loads((root / "MANIFEST.json").read_text())
    (root / "MANIFEST.json").write_text(json.dumps(manifest + manifest))
    with pytest.raises(DataciteIndexError, match="duplicate"):
        release_inventory_sha256(root, MONTH)
    manifest[0]["sha256"] = "not-base64!"
    (root / "MANIFEST.json").write_text(json.dumps(manifest))
    with pytest.raises(DataciteIndexError, match="sha256"):
        release_inventory_sha256(root, MONTH)
    manifest[0]["sha256"] = "AAAA"  # valid base64, wrong length
    (root / "MANIFEST.json").write_text(json.dumps(manifest))
    with pytest.raises(DataciteIndexError, match="32 bytes"):
        release_inventory_sha256(root, MONTH)
    (root / "MANIFEST.json").write_text("{}")
    with pytest.raises(DataciteIndexError, match="list"):
        release_inventory_sha256(root, MONTH)
    (root / "MANIFEST.json").unlink()
    with pytest.raises(DataciteIndexError, match="MANIFEST.json"):
        release_inventory_sha256(root, MONTH)


# --- discovery --------------------------------------------------------------


def _mirror(path: Path, month: str) -> Path:
    return path / f"datacite-monthly-data-file-{month}"


def test_discover_release_picks_latest_complete(tmp_path: Path) -> None:
    mount = tmp_path / "mnt"
    make_full_datacite_release(_mirror(mount, "2026-06"), "2026-06")
    make_full_datacite_release(_mirror(mount, "2026-07"), "2026-07")
    make_full_datacite_release(
        _mirror(mount, "2026-08"), "2026-08", transfer_complete=False
    )
    found = discover_release(mount)
    assert found == DataciteRelease(
        release="2026-07",
        root=_mirror(mount, "2026-07"),
        inventory_sha256=release_inventory_sha256(_mirror(mount, "2026-07"), "2026-07"),
    )


def test_discover_release_pinned(tmp_path: Path) -> None:
    mount = tmp_path / "mnt"
    make_full_datacite_release(_mirror(mount, "2026-06"), "2026-06")
    make_full_datacite_release(_mirror(mount, "2026-07"), "2026-07")
    make_full_datacite_release(
        _mirror(mount, "2026-08"), "2026-08", transfer_complete=False
    )
    assert discover_release(mount, pinned="2026-06").release == "2026-06"
    with pytest.raises(DataciteIndexError, match="2026-05"):
        discover_release(mount, pinned="2026-05")
    with pytest.raises(DataciteIndexError, match="2026-08"):
        discover_release(mount, pinned="2026-08")


def test_discover_release_empty(tmp_path: Path) -> None:
    with pytest.raises(DataciteIndexError, match="no complete"):
        discover_release(tmp_path / "mnt")
    (tmp_path / "mnt").mkdir()
    (tmp_path / "mnt" / "datacite-monthly-data-file-2026-07").write_text("file")
    with pytest.raises(DataciteIndexError, match="no complete"):
        discover_release(tmp_path / "mnt")


def test_discover_release_requires_complete_status_without_falling_back(
    tmp_path: Path, full_datacite_release: Path
) -> None:
    mount = tmp_path / "mnt"
    make_full_datacite_release(_mirror(mount, "2026-06"), "2026-06")
    status = full_datacite_release / "STATUS.json"
    status.write_text(
        json.dumps({"month": MONTH, "datetime": "x", "status": "Generating"})
    )
    with pytest.raises(DataciteIndexError, match="Complete"):
        discover_release(mount)
    status.write_text(
        json.dumps({"month": "2026-06", "datetime": "x", "status": "Complete"})
    )
    with pytest.raises(DataciteIndexError, match="2026-06"):
        discover_release(mount)
    status.unlink()
    with pytest.raises(DataciteIndexError, match="STATUS.json"):
        discover_release(mount)


def test_discover_release_fails_without_csv_state_report(tmp_path: Path) -> None:
    mount = tmp_path / "mnt"
    write_full_datacite_release(_mirror(mount, MONTH), MONTH, [(part_path(MONTH, 0), b"x")])
    with pytest.raises(DataciteIndexError, match="csv"):
        discover_release(mount)


def test_discover_release_accepts_a_folder_dated_after_the_release_month(
    tmp_path: Path,
) -> None:
    # The real layout: a 2026-07 mirror entry whose manifest lists both
    # `updated_2026-07/` and `updated_2026-08/`, each with JSONL and CSV.
    mount = tmp_path / "mnt"
    write_full_datacite_release(_mirror(mount, MONTH), MONTH, LATER_FOLDER_FILES)
    assert discover_release(mount) == DataciteRelease(
        release=MONTH,
        root=_mirror(mount, MONTH),
        inventory_sha256=LATER_FOLDER_FIXTURE_INVENTORY,
    )


# --- paths ------------------------------------------------------------------


def test_bundle_prefix_layout() -> None:
    digest = "f" * 64
    assert bundle_prefix("2026-07", digest) == Path("indexes") / f"2026-07-v2-{digest}"
    for release, inventory in (
        ("2026-7", digest),
        ("../x", digest),
        ("2026-07", "F" * 64),
        ("2026-07", "f" * 63),
        ("2026-07", "../" + "f" * 61),
    ):
        with pytest.raises(GenerationPathError):
            bundle_prefix(release, inventory)


def test_pipeline_paths_prefix_name_selects_only_the_owned_prefix(tmp_path: Path) -> None:
    legacy = PipelinePaths.from_roots(tmp_path / "mnt", tmp_path / "work")
    full = PipelinePaths.from_roots(
        tmp_path / "mnt", tmp_path / "work", prefix_name=DATACITE_PREFIX_NAME
    )
    assert legacy.prefix == tmp_path / "mnt" / "crossref-arxiv-citations-latest"
    assert full.prefix == tmp_path / "mnt" / "crossref-datacite-citations-latest"
    assert full.affiliations_prefix == legacy.affiliations_prefix
    assert full.extraction_marker == full.prefix / "_EXTRACTION_COMPLETE.json"
    assert full.pipeline_marker == full.prefix / "_PIPELINE_COMPLETE.json"
    assert full.transfer_marker == full.prefix / "_TRANSFER_COMPLETE.json"
    assert full.indexes == full.prefix / "indexes"
    assert legacy.indexes == legacy.prefix / "indexes"
    assert full.bundle_local == tmp_path / "work" / "bundle"
    assert full.identity_local == tmp_path / "work" / "run-identity.json"
    assert full.new_raw_generation("2026-07", "0" * 32).root.is_relative_to(full.prefix)
    assert full.new_derived_generation("2026-07", "0" * 32).root.is_relative_to(
        full.prefix
    )
    assert full.snapshot_local == legacy.snapshot_local


# --- bundle cache -----------------------------------------------------------


def write_bundle(
    directory: Path,
    release: DataciteRelease,
    contents: dict[str, bytes] | None = None,
    manifest_overrides: dict[str, Any] | None = None,
) -> str:
    """Write a bundle the way the Rust builder does (files, then the pretty
    JSON marker with a trailing newline) and return the marker digest."""
    if contents is None:
        contents = {name: f"{name}-bytes".encode() for name in BUNDLE_FILES}
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, data in contents.items():
        (directory / name).write_bytes(data)
        files[name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    manifest = {
        "kind": "datacite-index",
        "format_version": 2,
        "normalization_version": 1,
        "release": release.release,
        "source_inventory_sha256": release.inventory_sha256,
        "files": files,
        "counts": {"dois": 1, "urls": 1, "missing_urls": 0, "shared_urls": 0},
        **(manifest_overrides or {}),
    }
    return write_marker(directory, manifest)


def write_marker(directory: Path, manifest: dict[str, Any]) -> str:
    marker = (json.dumps(manifest, indent=2) + "\n").encode("utf-8")
    (directory / BUNDLE_MARKER_NAME).write_bytes(marker)
    return hashlib.sha256(marker).hexdigest()


class Builder:
    """A fake `build_index` that writes a plausible bundle at `output`."""

    def __init__(
        self,
        release: DataciteRelease,
        contents: dict[str, bytes] | None = None,
        *,
        corrupt: str | None = None,
        manifest_overrides: dict[str, Any] | None = None,
    ) -> None:
        self.release = release
        self.contents = contents
        self.corrupt = corrupt
        self.manifest_overrides = manifest_overrides
        self.calls: list[tuple[Path, Path]] = []

    def __call__(self, records: Path, output: Path) -> None:
        # The Rust builder refuses an existing, non-empty output directory.
        assert not output.exists() or not any(output.iterdir())
        self.calls.append((records, output))
        write_bundle(output, self.release, self.contents, self.manifest_overrides)
        if self.corrupt is not None:
            target = output / self.corrupt
            original = target.read_bytes()
            target.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))


def fail_build(records: Path, output: Path) -> None:
    raise AssertionError("must not rebuild when the cache is valid")


@dataclass
class Case:
    mount: Path
    paths: PipelinePaths
    release: DataciteRelease
    lines: list[str] = field(default_factory=list)

    @property
    def cache_root(self) -> Path:
        return self.paths.prefix / bundle_prefix(
            self.release.release, self.release.inventory_sha256
        )

    def pointer(self) -> dict[str, Any]:
        return read_json(self.cache_root / BUNDLE_POINTER_NAME)

    def generations(self) -> list[Path]:
        root = self.cache_root / "generations"
        return sorted(root.iterdir()) if root.is_dir() else []

    def ensure(
        self,
        build_index: Callable[[Path, Path], None],
        *,
        force: bool = False,
        with_store: bool = True,
    ) -> BundleResult:
        self.lines = []
        return ensure_bundle(
            paths=self.paths,
            release=self.release,
            build_index=build_index,
            store=LocalBucketStore(mount_root=self.mount) if with_store else None,
            events=EventLogger(output=self.lines.append),
            force=force,
        )

    def events(self) -> list[dict[str, Any]]:
        return [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in self.lines]

    def names(self) -> list[str]:
        return [event["event"] for event in self.events()]

    def emitted(self, name: str) -> list[dict[str, Any]]:
        return [event for event in self.events() if event["event"] == name]


@pytest.fixture
def case(tmp_path: Path, full_datacite_release: Path) -> Case:
    mount = tmp_path / "mnt"
    paths = PipelinePaths.from_roots(
        mount, tmp_path / "work", prefix_name=DATACITE_PREFIX_NAME
    )
    return Case(mount=mount, paths=paths, release=discover_release(mount))


def seed_cache(
    case: Case,
    contents: dict[str, bytes] | None = None,
    *,
    pointer_overrides: dict[str, Any] | None = None,
    manifest_overrides: dict[str, Any] | None = None,
) -> Path:
    """Write a cached generation plus a consistent `_BUNDLE_CURRENT.json`."""
    generation_name = uuid.uuid4().hex
    generation = case.cache_root / "generations" / generation_name
    digest = write_bundle(generation, case.release, contents, manifest_overrides)
    pointer = {
        "kind": "datacite-bundle-cache",
        "schema_version": 2,
        "release": case.release.release,
        "source_inventory_sha256": case.release.inventory_sha256,
        "relative_generation": f"generations/{generation_name}",
        "generation_manifest_sha256": digest,
        **(pointer_overrides or {}),
    }
    (case.cache_root / BUNDLE_POINTER_NAME).write_text(json.dumps(pointer))
    return generation


def snapshot(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(directory.iterdir())}


def bundle_names() -> list[str]:
    return sorted([*BUNDLE_FILES, BUNDLE_MARKER_NAME])


def test_ensure_bundle_builds_verifies_and_persists_a_new_generation(case: Case) -> None:
    builder = Builder(case.release)
    result = case.ensure(builder)
    assert result.reused is False
    assert (result.release, result.inventory_sha256) == (
        case.release.release,
        case.release.inventory_sha256,
    )
    [(records, output)] = builder.calls
    assert records == case.release.root
    assert output.is_relative_to(case.paths.bundle_local)
    # The verified local copy holds exactly the bundle.
    assert result.directory.is_relative_to(case.paths.bundle_local)
    assert sorted(snapshot(result.directory)) == bundle_names()
    assert result.manifest_sha256 == sha256_file(result.directory / BUNDLE_MARKER_NAME)
    # A new immutable generation and a pointer to it were uploaded.
    assert case.cache_root.relative_to(case.paths.prefix) == (
        Path("indexes") / f"2026-07-v2-{case.release.inventory_sha256}"
    )
    pointer = case.pointer()
    assert re.fullmatch(r"generations/[0-9a-f]{32}", pointer["relative_generation"])
    assert pointer == {
        "kind": "datacite-bundle-cache",
        "schema_version": 2,
        "release": "2026-07",
        "source_inventory_sha256": case.release.inventory_sha256,
        "relative_generation": pointer["relative_generation"],
        "generation_manifest_sha256": result.manifest_sha256,
    }
    assert case.generations() == [case.cache_root / pointer["relative_generation"]]
    assert snapshot(case.generations()[0]) == snapshot(result.directory)
    assert "bundle_cache_missing" in case.names()
    assert "bundle_built" in case.names()
    assert "bundle_persisted" in case.names()
    assert "bundle_cache_reused" not in case.names()


def test_ensure_bundle_reuses_a_verified_cache_without_calling_the_builder(
    case: Case,
) -> None:
    first = case.ensure(Builder(case.release))
    pointer_before = case.pointer()
    second = case.ensure(fail_build)
    assert second.reused is True
    assert second.manifest_sha256 == first.manifest_sha256
    assert second.directory.is_relative_to(case.paths.bundle_local)
    assert snapshot(second.directory) == snapshot(first.directory)
    assert case.pointer() == pointer_before
    assert len(case.generations()) == 1
    [reused] = case.emitted("bundle_cache_reused")
    assert reused["relative_generation"] == pointer_before["relative_generation"]
    assert "bundle_built" not in case.names()


class FlakyCopy:
    """`shutil.copyfile` that raises a transient `OSError` for `name` read
    from the bucket cache (under `cache`) the first `failures` times."""

    def __init__(self, real: Callable[..., Any], name: str, failures: int, cache: Path) -> None:
        self.real = real
        self.name = name
        self.failures = failures
        self.cache = cache
        self.attempts = 0

    def __call__(self, source: Any, target: Any, *args: Any, **kwargs: Any) -> Any:
        if Path(source).name == self.name and Path(source).is_relative_to(self.cache):
            self.attempts += 1
            if self.attempts <= self.failures:
                raise OSError(5, "Input/output error", str(source))
        return self.real(source, target, *args, **kwargs)


def test_a_transient_copy_error_is_retried_before_the_cache_is_rejected(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    import citations_pipeline.datacite_index as datacite_index

    first = case.ensure(Builder(case.release))
    flaky = FlakyCopy(
        datacite_index.shutil.copyfile, "url-targets.bin", failures=1, cache=case.cache_root
    )
    sleeps: list[float] = []
    monkeypatch.setattr(datacite_index.shutil, "copyfile", flaky)
    monkeypatch.setattr(datacite_index, "_sleep", sleeps.append)
    second = case.ensure(fail_build)
    assert second.reused is True
    assert second.manifest_sha256 == first.manifest_sha256
    assert flaky.attempts == 2
    assert sleeps == [datacite_index.COPY_RETRY_DELAYS[0]]
    [retry] = case.emitted("bundle_cache_copy_retry")
    assert retry["file"] == "url-targets.bin" and retry["attempt"] == 1
    assert "bundle_cache_invalid" not in case.names()


def test_a_persistent_copy_error_falls_through_to_a_rebuild_after_bounded_retries(
    case: Case, monkeypatch: pytest.MonkeyPatch
) -> None:
    import citations_pipeline.datacite_index as datacite_index

    case.ensure(Builder(case.release))
    flaky = FlakyCopy(
        datacite_index.shutil.copyfile, "dois.fst", failures=10**6, cache=case.cache_root
    )
    sleeps: list[float] = []
    monkeypatch.setattr(datacite_index.shutil, "copyfile", flaky)
    monkeypatch.setattr(datacite_index, "_sleep", sleeps.append)
    builder = Builder(case.release)
    result = case.ensure(builder)
    assert result.reused is False
    assert len(builder.calls) == 1
    assert flaky.attempts == len(datacite_index.COPY_RETRY_DELAYS) + 1 == 3
    assert sleeps == list(datacite_index.COPY_RETRY_DELAYS)
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "dois.fst" in invalid["reason"]


def test_a_missing_cached_file_is_not_retried(case: Case, monkeypatch: pytest.MonkeyPatch) -> None:
    import citations_pipeline.datacite_index as datacite_index

    generation = seed_cache(case)
    (generation / "hosts.fst").unlink()
    sleeps: list[float] = []
    monkeypatch.setattr(datacite_index, "_sleep", sleeps.append)
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    assert sleeps == []
    assert "bundle_cache_copy_retry" not in case.names()


def test_same_size_sidecar_mutation_forces_a_rebuild_and_keeps_the_old_generation(
    case: Case,
) -> None:
    case.ensure(Builder(case.release))
    old_generation = case.cache_root / case.pointer()["relative_generation"]
    sidecar = old_generation / "url-targets.bin"
    original = sidecar.read_bytes()
    mutated = original[:-1] + bytes([original[-1] ^ 1])
    sidecar.write_bytes(mutated)
    mutated_snapshot = snapshot(old_generation)

    second = case.ensure(Builder(case.release))
    assert second.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "url-targets.bin" in invalid["reason"]
    assert "checksum" in invalid["reason"]
    # The verified local copy is the fresh build, not the mutated cache.
    assert (second.directory / "url-targets.bin").read_bytes() == original
    # The existing generation is never modified or deleted; a new one is added.
    assert snapshot(old_generation) == mutated_snapshot
    assert len(case.generations()) == 2
    new_generation = case.cache_root / case.pointer()["relative_generation"]
    assert new_generation != old_generation
    assert snapshot(new_generation) == snapshot(second.directory)


@pytest.mark.parametrize(
    ("manifest_overrides", "pointer_overrides", "expected_reason"),
    [
        ({"normalization_version": 2}, {}, "normalization_version"),
        ({"format_version": 1}, {}, "format_version"),
        ({"kind": "arxiv-fst"}, {}, "kind"),
        ({"release": "2026-06"}, {}, "release"),
        ({"source_inventory_sha256": "0" * 64}, {}, "source_inventory_sha256"),
        ({"counts": {"dois": "1"}}, {}, "counts"),
        ({}, {"generation_manifest_sha256": "0" * 64}, "_INDEX_COMPLETE.json"),
        ({}, {"relative_generation": "generations/../escape"}, "relative_generation"),
        ({}, {"relative_generation": "other/" + "0" * 32}, "relative_generation"),
        ({}, {"relative_generation": "generations/" + "0" * 31}, "relative_generation"),
        ({}, {"schema_version": 1}, "schema_version"),
        ({}, {"kind": "fst-cache"}, "kind"),
        ({}, {"release": "2026-06"}, "release"),
        ({}, {"source_inventory_sha256": "1" * 64}, "source_inventory_sha256"),
        ({}, {"extra": True}, "extra"),
    ],
)
def test_invalid_cache_metadata_forces_a_rebuild_and_leaves_the_cache_untouched(
    case: Case,
    manifest_overrides: dict[str, Any],
    pointer_overrides: dict[str, Any],
    expected_reason: str,
) -> None:
    seeded = seed_cache(
        case, pointer_overrides=pointer_overrides, manifest_overrides=manifest_overrides
    )
    seeded_snapshot = snapshot(seeded)
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert expected_reason in invalid["reason"]
    assert snapshot(seeded) == seeded_snapshot
    assert len(case.generations()) == 2
    assert case.cache_root / case.pointer()["relative_generation"] != seeded


def test_marker_listing_unsafe_or_missing_names_forces_a_rebuild(case: Case) -> None:
    seeded = seed_cache(case)
    manifest = read_json(seeded / BUNDLE_MARKER_NAME)
    manifest["files"]["../escape"] = {"size": 1, "sha256": "0" * 64}
    pointer = case.pointer()
    pointer["generation_manifest_sha256"] = write_marker(seeded, manifest)
    (case.cache_root / BUNDLE_POINTER_NAME).write_text(json.dumps(pointer))
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "../escape" in invalid["reason"]
    assert not (case.cache_root / "escape").exists()

    del manifest["files"]["../escape"]
    del manifest["files"]["hosts.fst"]
    pointer["generation_manifest_sha256"] = write_marker(seeded, manifest)
    (case.cache_root / BUNDLE_POINTER_NAME).write_text(json.dumps(pointer))
    case.ensure(Builder(case.release))
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "hosts.fst" in invalid["reason"]


def test_incomplete_cache_forces_a_rebuild(case: Case) -> None:
    seeded = seed_cache(case)
    (seeded / "urls.fst").unlink()
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "urls.fst" in invalid["reason"]
    assert (result.directory / "urls.fst").is_file()


def test_pointer_to_an_unfinished_generation_forces_a_rebuild(case: Case) -> None:
    seeded = seed_cache(case)
    (seeded / BUNDLE_MARKER_NAME).unlink()
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert BUNDLE_MARKER_NAME in invalid["reason"]


def test_generation_without_a_pointer_is_not_a_cache_hit(case: Case) -> None:
    seed_cache(case)
    (case.cache_root / BUNDLE_POINTER_NAME).unlink()
    result = case.ensure(Builder(case.release))
    assert result.reused is False
    assert "bundle_cache_missing" in case.names()
    assert "bundle_cache_invalid" not in case.names()
    assert len(case.generations()) == 2  # the orphaned generation is left alone


def test_wrong_size_in_cache_forces_a_rebuild(case: Case) -> None:
    seeded = seed_cache(case)
    (seeded / "dois.fst").write_bytes(b"longer than the manifest says")
    case.ensure(Builder(case.release))
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "dois.fst" in invalid["reason"]
    assert "size" in invalid["reason"]


def test_corrupt_cached_marker_is_rejected_before_any_copy(case: Case) -> None:
    seeded = seed_cache(case)
    marker = seeded / BUNDLE_MARKER_NAME
    marker.write_bytes(marker.read_bytes().replace(b"\n}", b" \n}"))
    case.ensure(Builder(case.release))
    [invalid] = case.emitted("bundle_cache_invalid")
    assert BUNDLE_MARKER_NAME in invalid["reason"]
    assert not any(
        path.name.startswith(".stage") for path in case.paths.bundle_local.rglob("*")
    )


def test_force_rebuilds_a_valid_cache_but_still_verifies(case: Case) -> None:
    case.ensure(Builder(case.release))
    old_generation = case.cache_root / case.pointer()["relative_generation"]
    old_snapshot = snapshot(old_generation)

    result = case.ensure(Builder(case.release), force=True)
    assert result.reused is False
    assert "bundle_cache_forced" in case.names()
    assert "bundle_cache_reused" not in case.names()
    assert len(case.generations()) == 2
    assert case.cache_root / case.pointer()["relative_generation"] != old_generation
    assert snapshot(old_generation) == old_snapshot

    # A forced build that fails verification is rejected and persists nothing.
    pointer_before = case.pointer()
    with pytest.raises(DataciteIndexError, match="checksum mismatch: dois.fst"):
        case.ensure(Builder(case.release, corrupt="dois.fst"), force=True)
    assert case.pointer() == pointer_before
    assert len(case.generations()) == 2


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"normalization_version": 2}, "normalization_version"),
        ({"format_version": 3}, "format_version"),
        ({"kind": "datacite-extraction"}, "kind"),
        ({"release": "2026-06"}, "release"),
        ({"source_inventory_sha256": "0" * 64}, "source_inventory_sha256"),
    ],
)
def test_fresh_build_with_wrong_identity_is_rejected(
    case: Case, overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(DataciteIndexError, match=message):
        case.ensure(Builder(case.release, manifest_overrides=overrides))
    assert not case.cache_root.exists()


def test_fresh_build_missing_a_required_file_is_rejected(case: Case) -> None:
    contents = {name: b"data" for name in BUNDLE_FILES if name != "hosts.fst"}
    with pytest.raises(DataciteIndexError, match="hosts.fst"):
        case.ensure(Builder(case.release, contents))
    assert not case.cache_root.exists()


def test_ensure_bundle_without_store_verifies_locally_and_uploads_nothing(
    case: Case,
) -> None:
    result = case.ensure(Builder(case.release), with_store=False)
    assert result.reused is False
    assert result.directory.is_relative_to(case.paths.bundle_local)
    assert sorted(snapshot(result.directory)) == bundle_names()
    assert not case.cache_root.exists()
    assert "bundle_persisted" not in case.names()


def test_reuse_always_verifies_the_cache_even_when_a_local_copy_exists(
    case: Case,
) -> None:
    first = case.ensure(Builder(case.release))
    assert (first.directory / "hosts.fst").is_file()  # a verified local copy exists
    generation = case.cache_root / case.pointer()["relative_generation"]
    (generation / "hosts.fst").write_bytes(b"hosts.fst-bytes"[::-1])  # same size
    second = case.ensure(Builder(case.release))
    assert second.reused is False
    [invalid] = case.emitted("bundle_cache_invalid")
    assert "hosts.fst" in invalid["reason"]
    assert (second.directory / "hosts.fst").read_bytes() == b"hosts.fst-bytes"


# --- extractor wrapper ------------------------------------------------------


class FakeRunner:
    def __init__(self, returncode: int = 0) -> None:
        self.calls: list[dict[str, Any]] = []
        self.returncode = returncode

    def __call__(self, arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        self.calls.append({"arguments": list(arguments), **kwargs})
        return subprocess.CompletedProcess(arguments, self.returncode)


def test_run_build_datacite_index_composes_command(tmp_path: Path) -> None:
    binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
    runner = FakeRunner()
    run_build_datacite_index(
        binary, tmp_path / "release", tmp_path / "bundle" / "out", 4096, runner=runner
    )
    assert runner.calls[0]["arguments"] == [
        str(tmp_path / "bin"),
        "build-datacite-index",
        "--input",
        str(tmp_path / "release"),
        "--output",
        str(tmp_path / "bundle" / "out"),
        "--sort-memory-mb",
        "4096",
    ]
    # The parent exists for the Rust builder's staging; the output itself is
    # left for the builder to create (it refuses a non-empty directory).
    assert (tmp_path / "bundle").is_dir()
    assert not (tmp_path / "bundle" / "out").exists()


def test_run_build_datacite_index_rejects_failure_and_nonpositive_memory(
    tmp_path: Path,
) -> None:
    binary = ExtractorBinary(path=tmp_path / "bin", version="2.1.0")
    runner = FakeRunner()
    with pytest.raises(ExtractorError, match="sort_memory_mb"):
        run_build_datacite_index(binary, tmp_path / "r", tmp_path / "o", 0, runner=runner)
    assert runner.calls == []
    with pytest.raises(ExtractorError, match="exit code 1"):
        run_build_datacite_index(
            binary, tmp_path / "r", tmp_path / "o", 4096, runner=FakeRunner(returncode=1)
        )
