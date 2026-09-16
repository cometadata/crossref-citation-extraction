"""Representative benchmark: streaming sampler, measurement helpers,
extrapolation, readiness ladder, pricing, settings emission, and the
orchestration with faked acquisition, extractor and legacy runner.

No real benchmark runs here: the orchestration test fakes the mirror, the
Rust binary and both extraction paths, and exercises the real sampler,
estimator, readiness evaluation, report writer and `LocalBucketStore`.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import itertools
import json
import os
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from citations_pipeline.benchmark import (
    DOCUMENTED_PRICING,
    HARDWARE,
    READINESS_LADDER,
    SETTINGS_KEYS,
    BenchmarkDependencies,
    MeasuredRunner,
    MemoryReading,
    PhaseMeter,
    ResourceWatermark,
    _run_stress,
    compare_arxiv_projection,
    cost_estimate,
    create_synthetic_stress,
    evaluate_readiness,
    extrapolate,
    fetch_pricing,
    maxrss_to_bytes,
    read_cgroup_peak,
    read_cgroup_resident,
    run_benchmark,
    sample_crossref_archive,
    select_configuration,
    timing_summary,
    tree_bytes,
    write_production_settings,
)
from citations_pipeline.bucket import LocalBucketStore
from citations_pipeline.datacite_index import (
    BUNDLE_MARKER_NAME,
    BundleResult,
    DataciteRelease,
    discover_release,
    ensure_bundle,
)
from citations_pipeline.datacite_publish import transform_datacite_outputs
from citations_pipeline.datacite_smoke import SMOKE_RESOURCES
from citations_pipeline.extractor import (
    ExtractionOutputs,
    ExtractorBinary,
    FullExtractionOutputs,
    build_extractor,
    run_datacite_extraction,
)
from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.mirror import ResolvedSnapshot
from citations_pipeline.paths import DATACITE_PREFIX_NAME, PipelinePaths, runtime_source_digest
from citations_pipeline.pipeline import PipelineConfig
from citations_pipeline.pipeline_io import PipelineError
from citations_pipeline.state import utc_now
from citations_pipeline.telemetry import EventLogger
from test_full_pipeline import (
    RUN,
    TOKEN,
    fixture_ambiguity_rows,
    fixture_records,
    make_source_tree,
    write_fake_bundle,
    write_jsonl,
)

# --- the brief's verbatim tests ------------------------------------------------


def test_sample_preserves_separated_whole_members(tmp_path):
    source, sample = tmp_path / "source.tar.gz", tmp_path / "sample.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        for number in range(12):
            content = f'{{"items":[],"number":{number}}}'.encode()
            member = tarfile.TarInfo(f"{number}.json")
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    sample_crossref_archive(source, sample, stride=5, window=2)
    with tarfile.open(sample, "r:gz") as archive:
        actual = {member.name: archive.extractfile(member).read()
                  for member in archive.getmembers()}
    assert actual == {f"{n}.json": f'{{"items":[],"number":{n}}}'.encode()
                      for n in [0, 1, 5, 6, 10, 11]}

def test_readiness_requires_memory_disk_and_time_headroom():
    report = {"peak_memory_bytes": 76, "projected_peak_disk_bytes": 60,
              "projected_total_seconds": 70}
    result = evaluate_readiness(report, memory_bytes=100, disk_bytes=100, timeout_seconds=100)
    assert result["ready"] is False
    assert "memory" in result["failed_limits"]
    report["peak_memory_bytes"] = 75
    report["projected_total_seconds"] = 81
    assert evaluate_readiness(report, memory_bytes=100, disk_bytes=100,
                              timeout_seconds=100)["failed_limits"] == ["time"]


# --- sampler ----------------------------------------------------------------------


def write_members(path: Path, members: list[tuple[str, bytes]]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, content in members:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))


def member_content(number: int, *, dois: int = 0, urls: int = 0) -> bytes:
    references = (
        [{"DOI": f"10.1/{number}-{i}"} for i in range(dois)]
        + [{"URL": f"https://example.org/{number}-{i}"} for i in range(urls)]
    )
    return json.dumps({"items": [{"DOI": f"10.9/{number}", "reference": references}]}).encode()


def read_members(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, "r:gz") as archive:
        return {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isreg()}


def test_sampler_reports_coverage_and_flags_tiny_sources(tmp_path):
    source = tmp_path / "source.tar.gz"
    members = [(f"{n}.json", member_content(n, dois=n)) for n in range(7)]
    write_members(source, members)
    sample = tmp_path / "sample.tar.gz"
    first = sample_crossref_archive(source, sample, stride=5, window=2)
    assert first["representative"] is False
    assert first["total_members"] == 7 and first["selected_members"] == 7
    assert first["members"] == [name for name, _ in members]
    assert first["total_bytes"] == sum(len(c) for _, c in members)
    assert first["selected_bytes"] == first["total_bytes"]
    assert first["source_bytes"] == source.stat().st_size
    assert first["sample_bytes"] == sample.stat().st_size
    assert first["sample_sha256"] == hashlib.sha256(sample.read_bytes()).hexdigest()
    assert first["skipped_entries"] == 0
    for key in ("elapsed_seconds", "read_seconds", "write_seconds"):
        assert first[key] >= 0.0
    assert read_members(sample) == dict(members)
    # Windows are consecutive chunks of `window` selected members.
    assert [w["members"] for w in first["windows"]] == [
        ["0.json", "1.json"], ["2.json", "3.json"], ["4.json", "5.json"], ["6.json"]
    ]
    # Deterministic bytes: the same source yields the same sample.
    again = tmp_path / "again.tar.gz"
    second = sample_crossref_archive(source, again, stride=5, window=2)
    assert again.read_bytes() == sample.read_bytes()
    assert second["sample_sha256"] == first["sample_sha256"]
    with gzip.open(sample, "rb") as handle:
        handle.read(1)
        assert handle.mtime == 0


def test_sampler_skips_non_json_entries_and_counts_them(tmp_path):
    source = tmp_path / "source.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        directory = tarfile.TarInfo("snapshot")
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        for n in range(12):
            content = member_content(n)
            info = tarfile.TarInfo(f"snapshot/{n}.json")
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
        readme = tarfile.TarInfo("snapshot/README.txt")
        readme.size = 5
        archive.addfile(readme, io.BytesIO(b"hello"))
    result = sample_crossref_archive(source, tmp_path / "sample.tar.gz", stride=5, window=2)
    assert result["total_members"] == 12
    assert result["skipped_entries"] == 2
    assert result["members"] == [f"snapshot/{n}.json" for n in (0, 1, 5, 6, 10, 11)]
    assert result["representative"] is True


def test_density_sub_samples_select_top_windows(tmp_path):
    source = tmp_path / "source.tar.gz"
    # Twelve members, stride 4, window 2: windows {0,1}, {4,5}, {8,9}.
    plan = {0: (1, 0), 1: (1, 0), 4: (0, 5), 5: (0, 5), 8: (9, 1), 9: (9, 1)}
    members = [
        (f"{n}.json", member_content(n, dois=plan.get(n, (0, 0))[0], urls=plan.get(n, (0, 0))[1]))
        for n in range(12)
    ]
    write_members(source, members)
    windows_dir = tmp_path / "windows"
    result = sample_crossref_archive(
        source, tmp_path / "sample.tar.gz", stride=4, window=2, windows_dir=windows_dir
    )
    windows = result["windows"]
    assert [w["index"] for w in windows] == [0, 1, 2]
    assert [w["members"] for w in windows] == [["0.json", "1.json"], ["4.json", "5.json"], ["8.json", "9.json"]]
    # Every member carries its own top-level "DOI" key plus the reference keys.
    assert [w["doi_keys"] for w in windows] == [4, 2, 20]
    assert [w["url_keys"] for w in windows] == [0, 10, 2]
    for window in windows:
        assert window["bytes"] == sum(len(dict(members)[m]) for m in window["members"])
        assert window["doi_density"] == pytest.approx(window["doi_keys"] / window["bytes"])
        assert window["url_density"] == pytest.approx(window["url_keys"] / window["bytes"])
    assert result["doi_heavy_window"] == 2
    assert result["url_heavy_window"] == 1
    # Per-window archives hold exactly their members, in order.
    for window in windows:
        archive = Path(window["archive"])
        assert archive == windows_dir / f"window-{window['index']:02d}" / "all.json.tar.gz"
        assert list(read_members(archive)) == window["members"]


# --- measurement helpers ------------------------------------------------------------


def test_maxrss_unit_conversion():
    assert maxrss_to_bytes(1024, platform="linux") == 1024 * 1024
    assert maxrss_to_bytes(1024, platform="darwin") == 1024
    assert maxrss_to_bytes(7, platform="freebsd") == 7 * 1024


def test_cgroup_peak_reader_prefers_v2_then_v1(tmp_path):
    assert read_cgroup_peak(tmp_path) == MemoryReading(None, "unavailable")
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "memory.max_usage_in_bytes").write_text("4096\n")
    assert read_cgroup_peak(tmp_path) == MemoryReading(4096, "cgroup-v1")
    (tmp_path / "memory.peak").write_text("8192\n")
    assert read_cgroup_peak(tmp_path) == MemoryReading(8192, "cgroup-v2")
    (tmp_path / "memory.peak").write_text("not a number\n")
    assert read_cgroup_peak(tmp_path) == MemoryReading(4096, "cgroup-v1")


def test_cgroup_resident_excludes_page_cache_and_tracks_a_maximum(tmp_path):
    assert read_cgroup_resident(tmp_path) == MemoryReading(None, "unavailable")
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "memory.stat").write_text(
        "cache 900000\nrss 4096\ntotal_cache 950000\ntotal_rss 5120\n"
    )
    assert read_cgroup_resident(tmp_path) == MemoryReading(5120, "cgroup-v1-stat")
    # v2: anon + kernel, never the file-backed page cache that dominates
    # memory.current/memory.peak while the archive is staged and streamed.
    (tmp_path / "memory.stat").write_text(
        "anon 1000\nfile 50000000\nkernel 200\nfile_mapped 40000000\nshmem 0\n"
    )
    assert read_cgroup_resident(tmp_path) == MemoryReading(1200, "cgroup-v2-stat")
    (tmp_path / "memory.peak").write_text("50001200\n")
    assert read_cgroup_peak(tmp_path).peak_bytes == 50001200

    readings = iter([1000, 5000, 2000, 700, 900])
    watermark = ResourceWatermark(
        tmp_path / "work",
        interval=3600.0,
        memory_sampler=lambda: MemoryReading(next(readings), "cgroup-v2-stat"),
    )
    watermark.work_root.mkdir()
    for _ in range(3):
        watermark.poll()
    assert watermark.memory_peak == 5000 and watermark.phase_memory_peak == 5000
    assert watermark.memory_source == "cgroup-v2-stat"
    watermark.mark_phase()
    watermark.poll()
    # The phase mark resets the per-phase maximum; the run-wide one persists.
    assert watermark.phase_memory_peak == 900 and watermark.memory_peak == 5000
    unavailable = ResourceWatermark(
        tmp_path / "work",
        interval=3600.0,
        memory_sampler=lambda: MemoryReading(None, "unavailable"),
    )
    unavailable.poll()
    assert unavailable.memory_peak is None and unavailable.phase_memory_peak is None


NODE_DISK = SimpleNamespace(total=2 * 10**12, used=987 * 10**9, free=13 * 10**9)


def test_watermark_measures_the_owned_work_tree_not_the_node_filesystem(tmp_path, monkeypatch):
    # The node's filesystem reports every tenant's files; only the trees the
    # job owns may feed a disk projection.
    monkeypatch.setattr("citations_pipeline.benchmark.shutil.disk_usage", lambda path: NODE_DISK)
    work = tmp_path / "work"
    (work / "snapshot").mkdir(parents=True)
    (work / "snapshot" / "all.json.tar.gz").write_bytes(b"a" * 1000)
    (work / "bundle").mkdir()
    (work / "bundle" / "dois.fst").write_bytes(b"b" * 300)
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "other-tenant.bin").write_bytes(b"c" * 5000)
    watermark = ResourceWatermark(work, interval=3600.0)
    assert watermark.poll() == 1300
    assert watermark.peak_owned == 1300
    assert watermark.filesystem_used_peak == NODE_DISK.used
    before = watermark.mark_phase(watch=work / "partitions")
    (work / "partitions").mkdir()
    (work / "partitions" / "segment_0000.parquet").write_bytes(b"d" * 700)
    watermark.poll()
    (work / "partitions" / "segment_0000.parquet").unlink()
    after = watermark.poll()
    assert (before, after) == (1300, 1300)
    assert watermark.phase_peak_owned == 2000
    assert watermark.watch_peak == 700
    assert watermark.peak_owned == 2000
    assert tree_bytes(work) == 1300


def test_measured_runner_forwards_output_and_reports_its_own_child(capsys):
    script = (
        "print('2026-09-16 16:31:43 INFO  [x] event=phase phase=index_load status=completed "
        "release=2026-07 elapsed_s=21.300')\n"
        "print('event=phase phase=aggregation status=started partitions=256')\n"
        "print('event=phase phase=aggregation status=completed valid=1 elapsed_s=0.250')\n"
        "block = bytearray(64 * 1024 * 1024)\n"
        "block[::4096] = b'x' * len(range(0, len(block), 4096))\n"
    )
    runner = MeasuredRunner()
    result = runner([sys.executable, "-c", script])
    assert result.returncode == 0
    run = runner.runs[-1]
    assert run.phase_seconds == {"index_load": 21.3, "aggregation": 0.25}
    assert run.peak_rss_bytes >= 64 * 1024 * 1024
    assert run.elapsed_seconds > 0
    out = capsys.readouterr().out
    assert "event=phase phase=index_load status=completed" in out
    assert "phase=aggregation status=started" in out
    failed = runner([sys.executable, "-c", "import sys; print('partial'); sys.exit(3)"])
    assert failed.returncode == 3
    assert runner.runs[-1].returncode == 3 and runner.runs[-1].phase_seconds == {}


def test_phase_meter_growth_and_child_peaks_come_from_the_job_itself(tmp_path, monkeypatch):
    monkeypatch.setattr("citations_pipeline.benchmark.shutil.disk_usage", lambda path: NODE_DISK)
    work = tmp_path / "work"
    (work / "bundle").mkdir(parents=True)
    (work / "bundle" / "dois.fst").write_bytes(b"b" * 100)
    lines: list[str] = []
    unavailable = lambda: MemoryReading(None, "unavailable")  # noqa: E731
    runner = MeasuredRunner()
    watermark = ResourceWatermark(work, interval=3600.0, memory_sampler=unavailable)
    meter = PhaseMeter(
        EventLogger(output=lines.append), memory_probe=unavailable,
        watermark=watermark, work_root=work, runner=runner,
    )
    with meter.phase("transform_sample", watch=work / "transform") as measured:
        (work / "transform").mkdir()
        (work / "transform" / "flat.parquet").write_bytes(b"x" * 4096)
        runner([sys.executable, "-c", "print('event=phase phase=aggregation status=completed elapsed_s=0.5')"])
    disk = measured["disk"]
    assert disk["owned_before_bytes"] == 100
    assert disk["owned_after_bytes"] == 4196
    assert disk["phase_growth_bytes"] == 4096
    assert disk["watched_tree_peak_bytes"] == 4096
    # Reported for context only.
    assert disk["filesystem_used_after_bytes"] == NODE_DISK.used
    assert [child["phase_seconds"] for child in measured["children"]] == [{"aggregation": 0.5}]
    assert measured["memory"]["child_peak_rss_bytes"] == measured["children"][0]["peak_rss_bytes"]
    event = json.loads(lines[-1].removeprefix("PIPELINE_EVENT="))
    assert event["event"] == "phase_measured" and event["phase_peak_owned_bytes"] == 4196


def test_timing_summary_separates_per_run_fixed_costs():
    child = SimpleNamespace(phase_seconds={"index_load": 21.3, "aggregation": 0.2})
    summary = timing_summary(23.16, 8, child)
    assert summary["index_load_seconds"] == 21.3
    assert summary["aggregation_seconds"] == 0.2
    assert summary["fixed_seconds"] == pytest.approx(21.5)
    assert summary["variable_seconds"] == pytest.approx(1.66)
    assert summary["variable_seconds_per_member"] == pytest.approx(1.66 / 8)
    # A run that logged nothing keeps its whole duration variable; a fixed
    # cost larger than the measured phase never goes negative.
    assert timing_summary(10.0, 4, None)["variable_seconds_per_member"] == 2.5
    assert timing_summary(1.0, 4, child)["variable_seconds"] == 0.0


# --- extrapolation ----------------------------------------------------------------------


def measured_numbers() -> dict[str, Any]:
    return {
        "total_members": 1000,
        "sampled_members": 10,
        "window_seconds_per_member": [1.5, 2.0, 1.0],
        # The same windows once each run's index load and aggregation are
        # subtracted; the slowest variable cost is 1.5 s per member.
        "window_variable_seconds_per_member": [1.0, 1.5, 0.5],
        "index_load_seconds": 20.0,
        "aggregation_seconds_max": 4.0,
        "sample_aggregation_seconds": 0.1,
        # Three windows of differing density; the sample mean is 5 matches
        # and 10 raw bytes per member, the densest window 8 and 20.
        "window_confirmed_matches_per_member": [4.0, 8.0, 3.0],
        "window_raw_bytes_per_member": [8.0, 20.0, 6.0],
        "sample_confirmed_matches": 50,
        "sample_ambiguous_urls": 5,
        "source_transfer_seconds": 100.0,
        "index_preparation_seconds": 200.0,
        "transform_seconds": 4.0,
        "compression_seconds": 2.0,
        "persistence_seconds": 1.0,
        "persisted_bytes": 1000,
        "archive_bytes": 5000,
        "bundle_bytes": 300,
        "fixed_work_bytes": 700,
        "segment_bytes": 40,
        "raw_output_bytes": 100,
        "transform_scratch_bytes": 60,
        "derived_bytes": 30,
        "compressed_output_bytes": 20,
        "extraction_child_peak_bytes": 1_000,
        "legacy_child_peak_bytes": 900,
        "transform_self_peak_bytes": 500,
        "extraction_budget_bytes": 1_200,
        "transform_budget_bytes": 600,
        "production_resident_peak_bytes": None,
        "observed_peak_owned_bytes": 0,
    }


def test_extrapolation_formulas_on_synthetic_numbers():
    result = extrapolate(measured_numbers())
    components = result["components_seconds"]
    # Slowest representative window's variable cost x total members; the
    # per-run fixed costs are paid once: the index load as measured, the
    # aggregation from the sample scaled by rows (never below the slowest run).
    assert components["extraction"] == pytest.approx(1.5 * 1000)
    assert components["index_load"] == pytest.approx(20.0)
    assert components["aggregation"] == pytest.approx(max(4.0, 0.1 * 160))
    # Rows and bytes follow the densest window (8 matches, 20 raw bytes per
    # member), not the sample mean (5 and 10): 160x rows, 200x bytes.
    assert result["densest_window_matches_per_member"] == 8.0
    assert result["densest_window_raw_bytes_per_member"] == 20.0
    assert result["projected_confirmed_matches"] == pytest.approx(8.0 * 1000)
    assert result["projected_raw_output_bytes"] == pytest.approx(20.0 * 1000)
    assert result["row_ratio"] == pytest.approx(160.0)
    assert result["byte_ratio"] == pytest.approx(200.0)
    assert result["projected_ambiguous_urls"] == pytest.approx(5 * 160)
    # Fixed costs are measured at full scale; the transform scales by rows,
    # compression and persistence by bytes.
    assert components["source_transfer"] == 100.0
    assert components["index_preparation"] == 200.0
    assert components["transform"] == pytest.approx(4.0 * 160)
    assert components["compression"] == pytest.approx(2.0 * 200)
    assert components["persistence"] == pytest.approx(1.0 * 200)
    # Publication uploads the projected derived bytes at the persistence rate.
    assert components["publication"] == pytest.approx((30 * 200) / (1000 / 1.0))
    point = sum(components.values())
    assert result["projected_total_seconds_point"] == pytest.approx(point)
    assert result["projected_total_seconds"] == pytest.approx(point * 1.25)
    assert result["projected_total_seconds_low"] == pytest.approx(point * 0.75)
    # Disk: before deletion = fixed + archive + segments + 2x segments + raw;
    # after deletion drops the archive but adds scratch, derived, compressed;
    # every byte term scales by the densest window's byte ratio (200x).
    segments = 100 * 200  # raw bytes are the floor for segments (40 < 100), scaled
    before = 700 + 5000 + segments + 2 * segments + 100 * 200
    after = 700 + 100 * 200 + 60 * 200 + 30 * 200 + 20 * 200
    assert result["disk"]["before_release_bytes"] == pytest.approx(before)
    assert result["disk"]["after_release_bytes"] == pytest.approx(after)
    assert result["projected_peak_disk_bytes_point"] == pytest.approx(max(before, after))
    assert result["projected_peak_disk_bytes"] == pytest.approx(max(before, after) * 1.5)
    # Memory: child peaks scaled for group-size effects, never below the budgets.
    assert result["memory"]["extraction_bytes"] == pytest.approx(max(1000 * 1.5, 1200))
    assert result["memory"]["transform_bytes"] == pytest.approx(max(500 * 1.5, 600))
    assert result["peak_memory_bytes"] == pytest.approx(1500)
    assert set(result["formulas"]) >= {"extraction", "transform", "disk", "memory", "total"}


def test_extrapolation_follows_the_densest_window():
    uniform = measured_numbers()
    uniform["window_confirmed_matches_per_member"] = [5.0, 5.0, 5.0]
    uniform["window_raw_bytes_per_member"] = [10.0, 10.0, 10.0]
    flat = extrapolate(uniform)
    # Uniform windows reproduce the sample mean x member scale (100x).
    assert flat["projected_confirmed_matches"] == pytest.approx(50 * 100)
    assert flat["projected_raw_output_bytes"] == pytest.approx(100 * 100)
    assert flat["row_ratio"] == pytest.approx(100.0)
    assert flat["byte_ratio"] == pytest.approx(100.0)
    skewed = measured_numbers()
    skewed["window_confirmed_matches_per_member"] = [5.0, 15.0, 5.0]
    skewed["window_raw_bytes_per_member"] = [10.0, 30.0, 10.0]
    dense = extrapolate(skewed)
    # One dense window triples every projected row and byte term, before
    # the uncertainty band is applied on top.
    assert dense["projected_confirmed_matches"] == pytest.approx(3 * 50 * 100)
    assert dense["projected_raw_output_bytes"] == pytest.approx(3 * 100 * 100)
    assert dense["disk"]["raw_output_bytes"] == pytest.approx(3 * flat["disk"]["raw_output_bytes"])
    assert dense["disk"]["segments_bytes"] == pytest.approx(3 * flat["disk"]["segments_bytes"])
    assert dense["disk"]["derived_bytes"] == pytest.approx(3 * flat["disk"]["derived_bytes"])
    assert dense["components_seconds"]["transform"] == pytest.approx(3 * flat["components_seconds"]["transform"])
    assert dense["components_seconds"]["compression"] == pytest.approx(3 * flat["components_seconds"]["compression"])
    assert dense["components_seconds"]["extraction"] == flat["components_seconds"]["extraction"]
    assert dense["projected_peak_disk_bytes"] == pytest.approx(
        dense["projected_peak_disk_bytes_point"] * 1.5
    )
    assert dense["projected_peak_disk_bytes"] > 3 * flat["projected_peak_disk_bytes_point"]
    # A sample that matched nothing falls back to the member scale.
    empty = measured_numbers()
    empty["sample_confirmed_matches"] = 0
    empty["window_confirmed_matches_per_member"] = [0.0, 0.0, 0.0]
    assert extrapolate(empty)["row_ratio"] == pytest.approx(100.0)


def test_extrapolation_uses_the_production_resident_peak_when_larger():
    numbers = measured_numbers()
    numbers["production_resident_peak_bytes"] = 9_000
    assert extrapolate(numbers)["peak_memory_bytes"] == 9_000


def test_fixed_costs_are_not_multiplied_by_total_members():
    # Attempt 4's real windows: 8 members each, a cold 21.3 s index load in
    # the first window and a warm 3.8 s one later, over 37,011 members.
    numbers = measured_numbers()
    windows = [
        timing_summary(23.16, 8, SimpleNamespace(phase_seconds={"index_load": 21.3, "aggregation": 0.2})),
        timing_summary(11.32, 8, SimpleNamespace(phase_seconds={"index_load": 3.8, "aggregation": 4.0})),
    ]
    numbers.update(
        total_members=37_011,
        window_seconds_per_member=[23.16 / 8, 11.32 / 8],
        window_variable_seconds_per_member=[w["variable_seconds_per_member"] for w in windows],
        index_load_seconds=21.3,
        aggregation_seconds_max=4.0,
    )
    result = extrapolate(numbers)
    components = result["components_seconds"]
    slowest_variable = (11.32 - 3.8 - 4.0) / 8
    assert components["extraction"] == pytest.approx(slowest_variable * 37_011)
    assert components["index_load"] == pytest.approx(21.3)
    # The old projection (slowest whole window x members) was ~30 h.
    assert components["extraction"] < (23.16 / 8) * 37_011 / 5
    # Raising a per-run fixed cost moves the total by that cost, not by
    # that cost x members.
    slower_load = extrapolate({**numbers, "index_load_seconds": 121.3})
    assert slower_load["projected_total_seconds_point"] - result[
        "projected_total_seconds_point"
    ] == pytest.approx(100.0)
    assert slower_load["components_seconds"]["extraction"] == components["extraction"]


def test_memory_projection_ignores_legacy_and_build_peaks():
    numbers = measured_numbers()
    numbers["legacy_child_peak_bytes"] = 20 * 10**9
    result = extrapolate(numbers)
    assert result["peak_memory_bytes"] == pytest.approx(1500)
    assert result["memory"]["legacy_bytes"] == 20 * 10**9


def test_disk_projection_never_falls_below_the_observed_owned_peak():
    numbers = measured_numbers()
    numbers["observed_peak_owned_bytes"] = 10**9
    result = extrapolate(numbers)
    assert result["projected_peak_disk_bytes_point"] == 10**9
    assert result["projected_peak_disk_bytes"] == pytest.approx(1.5 * 10**9)


# --- readiness ladder and settings --------------------------------------------------------


def report_for(*, seconds: float, memory: int, disk: int) -> dict[str, Any]:
    return {
        "peak_memory_bytes": memory,
        "projected_peak_disk_bytes": disk,
        "projected_total_seconds": seconds,
        "components_seconds": {
            "source_transfer": 0.0, "index_preparation": 0.0, "extraction": seconds / 1.25,
            "transform": 0.0, "compression": 0.0, "persistence": 0.0, "publication": 0.0,
        },
        "measured_on": "cpu-xl",
    }


def test_readiness_ladder_order_and_scaling():
    ready_now = select_configuration(report_for(seconds=1000, memory=1, disk=1))
    assert ready_now["selected"] == {"flavor": "cpu-xl", "timeout": "24h"}
    assert [(e["flavor"], e["timeout"]) for e in ready_now["evaluations"]] == [
        ("cpu-xl", "24h")
    ]
    # 100 000 s exceeds 80% of 24 h (69 120 s) but fits 48 h -> second rung.
    day_too_short = select_configuration(report_for(seconds=100_000, memory=1, disk=1))
    assert day_too_short["selected"] == {"flavor": "cpu-xl", "timeout": "48h"}
    assert day_too_short["evaluations"][0]["failed_limits"] == ["time"]
    assert day_too_short["evaluations"][1]["ready"] is True
    # Memory beyond cpu-xl's 75% but within cpu-performance's; CPU-bound
    # time is scaled by 0.6 on the larger machine, memory is not.
    big = select_configuration(report_for(seconds=100_000, memory=100 * 10**9, disk=1))
    assert big["selected"] == {"flavor": "cpu-performance", "timeout": "24h"}
    evaluations = {(e["flavor"], e["timeout"]): e for e in big["evaluations"]}
    assert evaluations[("cpu-xl", "24h")]["failed_limits"] == ["memory", "time"]
    assert evaluations[("cpu-xl", "48h")]["failed_limits"] == ["memory"]
    performance = evaluations[("cpu-performance", "24h")]
    assert performance["projected_total_seconds"] == pytest.approx(100_000 * 0.6)
    assert performance["peak_memory_bytes"] == 100 * 10**9
    assert performance["memory_bytes"] == HARDWARE["cpu-performance"].memory_bytes
    # Nothing fits: every rung is retained and nothing is selected.
    hopeless = select_configuration(report_for(seconds=10**7, memory=1, disk=10**13))
    assert hopeless["selected"] is None
    assert [(e["flavor"], e["timeout"]) for e in hopeless["evaluations"]] == list(READINESS_LADDER)
    assert all(e["failed_limits"] == ["disk", "time"] for e in hopeless["evaluations"])


def test_settings_file_has_exactly_the_contract_keys(tmp_path):
    report_path = tmp_path / "benchmark-report.json"
    report_path.write_bytes(b'{"kind": "datacite-benchmark-report"}\n')
    settings_path = tmp_path / "production-settings.json"
    resources = ResourceSettings()
    write_production_settings(
        settings_path,
        report_path=report_path,
        source_tree_sha256="a" * 64,
        snapshot_id="2026-07",
        datacite_release="2026-07",
        flavor="cpu-xl",
        timeout="24h",
        resources=resources,
    )
    settings = json.loads(settings_path.read_text())
    assert set(settings) == set(SETTINGS_KEYS) == {
        "source_tree_sha256", "snapshot_id", "datacite_release", "flavor",
        "timeout", "resources", "benchmark_report_sha256",
    }
    assert settings["benchmark_report_sha256"] == hashlib.sha256(
        report_path.read_bytes()
    ).hexdigest()
    assert settings["resources"] == {
        "partitions": 256, "writer_memory_mb": 4096, "result_batch_mb": 16,
        "queue_batches": 8, "aggregation_memory_mb": 16384, "merge_fan_in": 32,
        "sort_workers": 4, "lookup_memory_mb": 64, "max_entry_mb": 512,
        "index_sort_memory_mb": 4096, "duckdb_memory_mb": 32768,
    }
    assert settings["timeout"] == "24h" and settings["flavor"] == "cpu-xl"


# --- pricing and costs --------------------------------------------------------------------


class Hardware:
    def __init__(self, name: str, unit_cost_usd: float, unit_label: str = "minute") -> None:
        self.name = name
        self.unit_cost_usd = unit_cost_usd
        self.unit_label = unit_label


def test_pricing_is_fetched_per_minute_and_falls_back_when_unavailable():
    listing = [
        Hardware("cpu-basic", 0.000167),
        Hardware("cpu-upgrade", 0.0005),
        Hardware("cpu-xl", 0.02),
        Hardware("cpu-performance", 0.04, "minute"),
        Hardware("a10g-small", 0.05),
    ]
    fetched = fetch_pricing(lambda: listing)
    assert fetched["pricing_source"] == "hub-api"
    assert fetched["hourly_usd"] == {
        "cpu-basic": pytest.approx(0.01002), "cpu-upgrade": pytest.approx(0.03),
        "cpu-xl": pytest.approx(1.2), "cpu-performance": pytest.approx(2.4),
    }

    def failing() -> list[Any]:
        raise RuntimeError("no network")

    fallback = fetch_pricing(failing)
    assert fallback["pricing_source"] == "documented-fallback"
    assert fallback["hourly_usd"] == DOCUMENTED_PRICING
    assert fallback["error"] == "RuntimeError: no network"
    # A listing that lacks one of the flavors we plan for is unusable.
    partial = fetch_pricing(lambda: listing[:2])
    assert partial["pricing_source"] == "documented-fallback"
    assert "cpu-xl" in partial["error"]
    # Unknown billing units fall back too.
    odd = fetch_pricing(lambda: [Hardware(f.name, 1.0, "fortnight") for f in listing])
    assert odd["pricing_source"] == "documented-fallback"


def test_cost_estimates_bill_whole_minutes():
    estimate = cost_estimate(seconds=61, hourly_usd=1.0, flavor="cpu-xl")
    assert estimate == {
        "flavor": "cpu-xl", "seconds": 61, "billed_minutes": 2,
        "hourly_usd": 1.0, "usd": pytest.approx(2 / 60),
    }
    assert cost_estimate(seconds=0, hourly_usd=1.0, flavor="cpu-xl")["billed_minutes"] == 0
    assert cost_estimate(seconds=3600, hourly_usd=1.9, flavor="cpu-performance")["usd"] == pytest.approx(1.9)


# --- synthetic stress fixtures ----------------------------------------------------------------


def test_synthetic_stress_fixtures_have_the_designed_shape(tmp_path):
    cases = create_synthetic_stress(tmp_path, degree=30)
    assert set(cases) == {"high_degree", "shared_url"}
    high = cases["high_degree"]
    works = [json.loads(c) for c in read_members(high.archive).values()]
    items = [item for member in works for item in member["items"]]
    assert len(items) == 30
    assert {ref["DOI"] for item in items for ref in item["reference"]} == {high.target}
    assert len({item["DOI"] for item in items}) == 30
    shared = cases["shared_url"]
    items = [item for member in [json.loads(c) for c in read_members(shared.archive).values()] for item in member["items"]]
    assert len(items) == 30
    assert all(set(ref) == {"key", "URL", "unstructured"} for item in items for ref in item["reference"])
    assert {ref["URL"] for item in items for ref in item["reference"]} == {shared.url}
    # One release serves both cases and is discoverable by the real selector.
    assert high.release_root == shared.release_root
    release = discover_release(high.release_root.parent, None)
    assert release.release == "2026-07"
    metadata = gzip.decompress(
        (release.root / "dois" / "updated_2026-07" / "part_0000.jsonl.gz").read_bytes()
    ).decode().splitlines()
    urls = {json.loads(line)["attributes"]["url"] for line in metadata}
    assert shared.url in urls
    assert sum(json.loads(line)["attributes"]["url"] == shared.url for line in metadata) == 2
    # Generation is deterministic.
    again = create_synthetic_stress(tmp_path / "again", degree=30)
    assert again["high_degree"].archive.read_bytes() == high.archive.read_bytes()


# --- semantic comparison ------------------------------------------------------------------


def test_semantic_comparison_reports_pair_sets(tmp_path):
    legacy = tmp_path / "legacy_valid.jsonl"
    write_jsonl(legacy, [
        {"doi": "10.48550/arXiv.2403.03542", "citation_count": 2,
         "cited_by": [{"doi": "10.9999/w1"}, {"doi": "10.9999/w2"}]},
        {"doi": "10.48550/arXiv.1409.1556", "citation_count": 1,
         "cited_by": [{"doi": "10.9999/w3"}]},
    ])
    table = pa.table({
        "arxiv_doi": ["10.48550/arXiv.2403.03542", "10.48550/arXiv.2501.00001"],
        "arxiv_id": ["2403.03542", "2501.00001"],
        "citation_count": [2, 1],
        "cited_by": [
            [{"doi": "10.9999/w1", "provenance": "mined"}, {"doi": "10.9999/w2", "provenance": "crossref"}],
            [{"doi": "10.9999/w9", "provenance": "mined"}],
        ],
        "reference_count": [2, 1],
    })
    projection = tmp_path / "all.parquet"
    pq.write_table(table, projection)
    result = compare_arxiv_projection(legacy, [projection])
    assert result["legacy_pairs"] == 3 and result["full_pairs"] == 3
    assert result["shared"] == 2
    assert result["legacy_only"] == 1 and result["full_only"] == 1
    assert result["consistent"] is False
    assert result["legacy_only_examples"] == [["10.9999/w3", "10.48550/arxiv.1409.1556"]]
    assert result["full_only_examples"] == [["10.9999/w9", "10.48550/arxiv.2501.00001"]]


# --- orchestration -------------------------------------------------------------------------------


ARCHIVE_MEMBERS = 24
# Larger than every configured budget, so only a legacy leak could reach it.
LEGACY_RESIDENT = 100 * 10**9


def crossref_archive_bytes(count: int = ARCHIVE_MEMBERS) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz, tarfile.open(fileobj=gz, mode="w|") as tar:
        for n in range(count):
            content = member_content(n, dois=n % 3, urls=n % 2)
            info = tarfile.TarInfo(f"{n}.json")
            info.size = len(content)
            info.mtime = 0
            tar.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


class Case:
    """Fakes for everything outside this task: the mirror, the Rust binary,
    both extraction paths, the pricing call and the memory probe. The
    sampler, estimator, readiness, report and settings writers are real."""

    def __init__(self, tmp_path: Path, *, members: int = ARCHIVE_MEMBERS) -> None:
        self.mount = tmp_path / "mnt"
        self.work = tmp_path / "work"
        self.source_root = tmp_path / "sources"
        self.mount.mkdir()
        self.digest = make_source_tree(self.source_root)
        self.paths = PipelinePaths.from_roots(self.mount, self.work, prefix_name=DATACITE_PREFIX_NAME)
        self.lines: list[str] = []
        self.calls: list[str] = []
        self.extraction_archives: list[Path] = []
        self.legacy_archives: list[Path] = []
        self.archive_bytes = crossref_archive_bytes(members)
        mirror_archive = (
            self.mount / "crossref-affiliations-latest" / f"raw-generations/2026-07-{'7' * 32}" / "all.json.tar.gz"
        )
        mirror_archive.parent.mkdir(parents=True)
        mirror_archive.write_bytes(self.archive_bytes)
        self.snapshot = ResolvedSnapshot(
            snapshot_id="2026-07", provenance="mirror",
            archive_sha256=hashlib.sha256(self.archive_bytes).hexdigest(),
            archive_size=len(self.archive_bytes), archive_path=mirror_archive,
            archive_is_local=False,
            source={"kind": "affiliations-mirror", "prefix": "crossref-affiliations-latest",
                    "raw_prefix": f"raw-generations/2026-07-{'7' * 32}"},
        )
        self.release = DataciteRelease(release="2026-07", root=self.mount / "datacite-monthly-data-file-2026-07", inventory_sha256="d" * 64)
        (self.release.root / "dois").mkdir(parents=True)
        self.config = PipelineConfig(
            source_root=self.source_root, source_digest=self.digest, work_root=self.work,
            bucket_root=self.mount, smoke=False, force=False, datacite_release=None,
            hf_token=TOKEN, crossref_token=None, run_id=RUN, mode="benchmark",
            resources=ResourceSettings(duckdb_memory_mb=256),
        )
        self.memory_readings = itertools.count(1_000_000, 1_000_000)
        self.resident_readings = itertools.count(500_000, 250_000)
        # Set while a non-production child (the legacy index build) holds
        # memory, so the report can prove it never reaches readiness.
        self.resident_override: int | None = None
        self.child_phase_lines = (
            "event=phase phase=index_load status=completed release=2026-07 elapsed_s=2.000",
            "event=phase phase=aggregation status=completed valid=2 elapsed_s=0.500",
        )

    def deps(self) -> BenchmarkDependencies:
        def refuse(name: str):
            def _refuse(*args: Any, **kwargs: Any) -> None:
                raise AssertionError(f"{name} must never be called by the benchmark")
            return _refuse

        return BenchmarkDependencies(
            events=EventLogger(output=self.lines.append),
            api_factory=lambda token: f"api:{token}",
            store_factory=lambda **kwargs: LocalBucketStore(mount_root=kwargs["mount_root"]),
            resolve_mirror=self._resolve_mirror,
            fallback_transfer=refuse("fallback_transfer"),
            build_extractor=lambda source_root, target, events: ExtractorBinary(path=self.work / "bin", version="2.1.0"),
            discover_release=lambda bucket_root, pinned: self.release,
            ensure_bundle=self._ensure_bundle,
            run_extraction=self._run_extraction,
            transform=transform_datacite_outputs,
            build_legacy_index=self._build_legacy_index,
            run_legacy_extraction=self._run_legacy_extraction,
            list_hardware=lambda: [Hardware("cpu-basic", 0.000167), Hardware("cpu-upgrade", 0.0005), Hardware("cpu-xl", 1 / 60), Hardware("cpu-performance", 1.9 / 60)],
            memory_probe=lambda: MemoryReading(next(self.memory_readings), "cgroup-v2"),
            resident_probe=self._resident,
            prepare_dataset=refuse("prepare_dataset"),
            publish_pair=refuse("publish_pair"),
            publish_prepared=refuse("publish_prepared"),
            verify_current=refuse("verify_current"),
            stride=5,
            window=2,
            synthetic_degree=12,
            benchmark_flavor="cpu-xl",
            smoke_seconds=90.0,
        )

    def events(self) -> list[dict[str, Any]]:
        return [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in self.lines]

    def _resident(self) -> MemoryReading:
        reading = next(self.resident_readings)
        if self.resident_override is not None:
            reading = self.resident_override
        return MemoryReading(reading, "cgroup-v2-stat")

    def _resolve_mirror(self, bucket_root: Path) -> ResolvedSnapshot:
        self.calls.append("resolve_mirror")
        return self.snapshot

    def _ensure_bundle(self, **kwargs: Any) -> BundleResult:
        self.calls.append("ensure_bundle")
        release: DataciteRelease = kwargs["release"]
        directory = kwargs["paths"].bundle_local / f"{release.release}-v2-{release.inventory_sha256}" / "generations" / ("c" * 32)
        if directory.is_dir():
            manifest_sha256 = hashlib.sha256((directory / BUNDLE_MARKER_NAME).read_bytes()).hexdigest()
        else:
            manifest_sha256 = write_fake_bundle(directory, release)
        return BundleResult(directory=directory, release=release.release, inventory_sha256=release.inventory_sha256, manifest_sha256=manifest_sha256, reused=kwargs["store"] is not None)

    def _run_extraction(self, binary: ExtractorBinary, **kwargs: Any) -> FullExtractionOutputs:
        self.calls.append("run_extraction")
        archive: Path = kwargs["archive"]
        assert archive.is_file()
        self.extraction_archives.append(archive)
        identity = json.loads(kwargs["identity_path"].read_text())
        assert identity["bundle_manifest_sha256"] == hashlib.sha256((kwargs["bundle"] / BUNDLE_MARKER_NAME).read_bytes()).hexdigest()
        output_dir: Path = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_dir: Path = kwargs["temp_dir"]
        (temp_dir / "partitions").mkdir(parents=True, exist_ok=True)
        (temp_dir / "partitions" / "segment_0000.parquet").write_bytes(b"x" * 4096)
        with tarfile.open(archive, "r:gz") as tar:
            members = [m for m in tar.getmembers() if m.isreg()]
        # The extractor logs its per-run fixed costs; the benchmark reads
        # them through the measured runner.
        script = "".join(f"print({line!r})\n" for line in self.child_phase_lines)
        assert kwargs["runner"]([sys.executable, "-c", script]).returncode == 0
        names = {kind: output_dir / f"all.json_{kind}.jsonl" for kind in ("valid", "failed", "publisher", "crossref", "mined", "ambiguous_urls")}
        # The shared-URL stress archive confirms nothing by design.
        records = [] if "shared-url" in archive.parts else fixture_records()
        write_jsonl(names["valid"], records)
        write_jsonl(names["failed"], [])
        for provenance in ("publisher", "crossref", "mined"):
            write_jsonl(names[provenance], [
                {**r, "cited_by": [c for c in r["cited_by"] if c["provenance"] == provenance]}
                for r in records if any(c["provenance"] == provenance for c in r["cited_by"])
            ])
        write_jsonl(names["ambiguous_urls"], fixture_ambiguity_rows())
        stats = {
            "items": 2 * len(members), "references": 3 * len(members), "confirmed_matches": 3,
            "doi_matches": 1, "url_matches": 1, "arxiv_matches": 1, "ambiguous_urls": 1,
            "invalid_arxiv": 0, "unmatched_dois": 0, "unmatched_urls": 0,
            "malformed_references": 0, "malformed_fields": 0,
            "filtered_self_citations": 0, "filtered_provenance": 0,
        }
        report = {
            "kind": "datacite-extraction-report", "schema_version": 2,
            "identity_sha256": hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "completed": True, "files_processed": len(members), "fatal_source_errors": 0,
            "stats": stats,
            "aggregation": {"partitions_processed": 1, "valid_count": 2, "failed_count": 0, "total_citations": 3, "publisher_citations": 1, "crossref_citations": 0, "mined_citations": 2, "in_memory_partitions": 1, "external_partitions": 0, "reused_partitions": 0},
        }
        report_path = output_dir / "extraction-report.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        return FullExtractionOutputs(**names, report=report_path)

    def _build_legacy_index(self, binary: ExtractorBinary, records: Path, output: Path, **kwargs: Any) -> None:
        self.calls.append("build_legacy_index")
        assert records == self.release.root / "dois"
        assert isinstance(kwargs["runner"], MeasuredRunner)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"fst")
        self.resident_override = LEGACY_RESIDENT

    def _run_legacy_extraction(self, binary: ExtractorBinary, **kwargs: Any) -> ExtractionOutputs:
        self.calls.append("run_legacy_extraction")
        self.resident_override = None
        assert kwargs["fst"].is_file()
        self.legacy_archives.append(kwargs["archive"])
        out: Path = kwargs["output_dir"]
        out.mkdir(parents=True, exist_ok=True)
        paths = {kind: out / f"all.json_{kind}.jsonl" for kind in ("valid", "failed", "publisher", "crossref", "mined")}
        write_jsonl(paths["valid"], [
            {"doi": "10.48550/arXiv.2403.03542", "citation_count": 1, "cited_by": [{"doi": "10.9999/w2"}]},
        ])
        for kind in ("failed", "publisher", "crossref", "mined"):
            paths[kind].touch()
        return ExtractionOutputs(**paths)


def write_legacy_fst_cache(case: Case, content: bytes, *, digest: str | None = None) -> Path:
    cache = PipelinePaths.from_roots(case.mount, case.work).fst_prefix("2026-07")
    cache.root.mkdir(parents=True)
    cache.fst.write_bytes(content)
    cache.marker.write_text(json.dumps({
        "schema_version": 1, "fst_schema_version": 1, "datacite_release": "2026-07",
        "fst_sha256": digest or hashlib.sha256(content).hexdigest(), "fst_size": len(content),
        "extractor_version": "2.1.0", "completed_at": utc_now(),
    }))
    return cache.fst


def test_run_benchmark_measures_reports_and_never_publishes(tmp_path, monkeypatch):
    # The node filesystem claims to be almost full; only the job's own trees
    # may decide readiness.
    monkeypatch.setattr("citations_pipeline.benchmark.shutil.disk_usage", lambda path: NODE_DISK)
    case = Case(tmp_path)
    result = run_benchmark(case.config, case.deps())

    assert result["status"] == "benchmark-ready"
    assert result["ready"] is True and result["failed_limits"] == []
    assert result["semantic_consistent"] is True
    assert result["representative"] is True
    assert result["snapshot_id"] == "2026-07" and result["datacite_release"] == "2026-07"
    assert result["source_tree_sha256"] == case.digest
    assert result["selected"] == {"flavor": "cpu-xl", "timeout": "24h"}
    assert result["costs"]["pricing_source"] == "hub-api"
    assert set(result["costs"]) >= {"smoke", "benchmark", "production", "production_with_headroom"}
    assert result["costs"]["smoke"]["flavor"] == "cpu-basic"
    assert result["costs"]["smoke"]["billed_minutes"] == 2
    assert result["projected"]["projected_total_seconds"] > 0

    local = case.work / "benchmarks" / RUN
    remote = case.paths.prefix / "benchmarks" / RUN
    assert Path(result["report_path"]) == local / "benchmark-report.json"
    assert Path(result["settings_path"]) == local / "production-settings.json"
    for root in (local, remote):
        assert (root / "benchmark-report.json").is_file()
        assert (root / "production-settings.json").is_file()
    assert (local / "benchmark-report.json").read_bytes() == (remote / "benchmark-report.json").read_bytes()
    assert (remote / "sample-outputs" / "outputs" / "valid.jsonl.zst").is_file()
    assert (remote / "sample-outputs" / "datacite" / "data" / "all" / "part-00000.parquet").is_file()
    assert (remote / "sample-outputs" / "arxiv" / "data" / "all.parquet").is_file()

    report = json.loads((local / "benchmark-report.json").read_text())
    settings = json.loads((local / "production-settings.json").read_text())
    assert set(settings) == set(SETTINGS_KEYS)
    assert settings["benchmark_report_sha256"] == hashlib.sha256((local / "benchmark-report.json").read_bytes()).hexdigest()
    assert settings["resources"] == {"partitions": 256, "writer_memory_mb": 4096, "result_batch_mb": 16, "queue_batches": 8, "aggregation_memory_mb": 16384, "merge_fan_in": 32, "sort_workers": 4, "lookup_memory_mb": 64, "max_entry_mb": 512, "index_sort_memory_mb": 4096, "duckdb_memory_mb": 256}
    assert settings["snapshot_id"] == "2026-07" and settings["datacite_release"] == "2026-07"
    assert settings["source_tree_sha256"] == case.digest

    assert report["kind"] == "datacite-benchmark-report"
    assert report["identities"]["archive_sha256"] == case.snapshot.archive_sha256
    assert report["identities"]["bundle_manifest_sha256"]
    assert report["sampling"]["total_members"] == ARCHIVE_MEMBERS
    assert report["sampling"]["selected_members"] == 10
    assert report["sampling"]["representative"] is True
    assert len(report["extraction"]["windows"]) == 5
    assert report["samples"]["doi_heavy"]["window"] == report["sampling"]["doi_heavy_window"]
    assert report["samples"]["url_heavy"]["window"] == report["sampling"]["url_heavy_window"]
    assert report["extraction"]["sample"]["files_processed"] == 10
    assert report["extraction"]["sample"]["stats"]["confirmed_matches"] == 3
    assert report["extraction"]["sample"]["match_rate"] == pytest.approx(3 / 30)
    assert report["legacy"]["pairs"] == 1
    assert report["semantic"]["consistent"] is True and report["semantic"]["legacy_only"] == 0
    assert report["semantic"]["full_pairs"] == 1
    assert set(report["synthetic"]) == {"high_degree", "shared_url"}
    for stress in report["synthetic"].values():
        assert stress["extraction"]["elapsed_seconds"] >= 0
        assert stress["transform"]["elapsed_seconds"] >= 0
        assert stress["output_bytes"]["raw"] > 0
    assert report["synthetic"]["high_degree"]["transform"]["confirmed_citations"] > 0
    shared = report["synthetic"]["shared_url"]
    assert shared["transform"]["confirmed_citations"] == 0
    assert shared["transform"]["no_confirmed_citations"] is True
    assert shared["transform"]["counts"]["datacite"]["all"] == {"works": 0, "citations": 0, "references": 0}
    assert shared["output_bytes"]["derived"] == 0
    assert "high_degree" not in json.dumps(report["extrapolation"])
    # The non-reclaimable high-water mark of the phases production runs
    # feeds the projection; the legacy index build's resident memory and
    # memory.peak (page cache included) are reported for information only.
    assert report["measurement"]["memory_source"] == "cgroup-v2-stat"
    assert report["measurement"]["memory_peak_source"] == "cgroup-v2"
    assert report["measurement"]["cgroup_resident_peak_bytes"] == LEGACY_RESIDENT
    production_resident = report["extrapolation"]["inputs"]["production_resident_peak_bytes"]
    assert production_resident == max(
        phase["memory"]["cgroup_resident_peak_bytes"]
        for name, phase in report["phases"].items()
        if name in {"prepare_index", "source_transfer", "extract_sample", "transform_sample",
                    "compress_outputs", "persist_sample_outputs"}
        or name.startswith("extract_window_")
    )
    assert 500_000 <= production_resident < LEGACY_RESIDENT
    assert report["phases"]["prepare_legacy_fst"]["memory"]["cgroup_resident_peak_bytes"] == LEGACY_RESIDENT
    memory = report["extrapolation"]["memory"]
    assert report["peak_memory_bytes"] == max(
        memory["extraction_bytes"], memory["transform_bytes"], production_resident
    )
    assert report["peak_memory_bytes"] < LEGACY_RESIDENT
    assert "cgroup_peak_bytes" not in report["extrapolation"]["inputs"]
    assert report["measurement"]["cgroup_peak_bytes"] >= 1_000_000
    assert report["extrapolation"]["memory"]["production_resident_bytes"] == production_resident
    # Per-run fixed costs come from the extractor's own phase log lines.
    for window in report["extraction"]["windows"]:
        assert window["index_load_seconds"] == 2.0
        assert window["aggregation_seconds"] == 0.5
        assert window["fixed_seconds"] == 2.5
        assert window["child_peak_rss_bytes"] > 0
    assert report["extrapolation"]["components_seconds"]["index_load"] == 2.0
    # Disk comes from the job's own trees, never the node filesystem.
    assert report["measurement"]["disk_source"] == "owned-tree-sizes"
    assert report["measurement"]["filesystem_used_peak_bytes"] == NODE_DISK.used
    assert report["projected_peak_disk_bytes"] < NODE_DISK.used
    assert report["legacy"]["fst_source"] == "built"
    # Rows and bytes are projected from the densest window's per-member rates.
    windows = report["extraction"]["windows"]
    assert all(w["member_count"] == len(w["members"]) for w in windows)
    assert report["extrapolation"]["inputs"]["window_raw_bytes_per_member"] == [
        w["raw_bytes_per_member"] for w in windows
    ]
    assert report["extrapolation"]["projected_raw_output_bytes"] == pytest.approx(
        max(w["raw_bytes_per_member"] for w in windows) * ARCHIVE_MEMBERS
    )
    assert report["extrapolation"]["projected_confirmed_matches"] == pytest.approx(
        max(w["confirmed_matches_per_member"] for w in windows) * ARCHIVE_MEMBERS
    )
    assert isinstance(report["samples"]["doi_heavy"]["members"], list)
    assert set(report["phases"]) >= {
        "prepare_index", "source_transfer", "sample_archive", "extract_window_00",
        "extract_sample", "prepare_legacy_fst", "extract_legacy", "transform_sample",
        "compress_outputs", "persist_sample_outputs",
    }
    assert report["phases"]["extract_sample"]["memory"]["children_maxrss_after_bytes"] >= 0
    assert report["readiness"]["selected"] == {"flavor": "cpu-xl", "timeout": "24h"}
    assert report["pricing"]["pricing_source"] == "hub-api"
    assert report["extrapolation"]["formulas"]
    assert report["peak_memory_bytes"] == report["extrapolation"]["peak_memory_bytes"]
    assert report["projected_total_seconds"] == report["extrapolation"]["projected_total_seconds"]

    # Every window, the whole sample and the two synthetic archives were
    # extracted; the legacy path ran once on the whole sample.
    assert len(case.extraction_archives) == 5 + 1 + 2
    assert case.legacy_archives == [case.work / "benchmarks" / RUN / "sample" / "all.json.tar.gz"]
    assert case.calls.count("run_legacy_extraction") == 1
    assert not any(call.startswith("publish") for call in case.calls)
    names = [e["event"] for e in case.events()]
    assert "benchmark_completed" in names and "archive_released" in names
    assert not any(n.startswith("publish") or n == "pipeline_activated" for n in names)
    assert not (case.work / "snapshot" / "all.json.tar.gz").exists()
    assert not (case.paths.prefix / "_PIPELINE_COMPLETE.json").exists()
    assert not (case.paths.prefix / "_EXTRACTION_COMPLETE.json").exists()
    assert TOKEN not in json.dumps(report) and TOKEN not in "".join(case.lines)


def test_run_benchmark_reuses_a_verified_legacy_fst_from_the_arxiv_cache(tmp_path):
    case = Case(tmp_path)
    cached = write_legacy_fst_cache(case, b"cached-legacy-fst")
    result = run_benchmark(case.config, case.deps())
    assert result["status"] == "benchmark-ready"
    assert "build_legacy_index" not in case.calls
    report = json.loads(Path(result["report_path"]).read_text())
    assert report["legacy"]["fst_source"] == "cache"
    assert report["legacy"]["fst_sha256"] == hashlib.sha256(b"cached-legacy-fst").hexdigest()
    assert "legacy_fst_prepared" in [e["event"] for e in case.events()]
    # Read-only: the arXiv-owned cache is exactly as it was.
    assert cached.read_bytes() == b"cached-legacy-fst"


def test_run_benchmark_builds_the_legacy_fst_when_no_cache_verifies(tmp_path):
    case = Case(tmp_path)
    cached = write_legacy_fst_cache(case, b"cached-legacy-fst", digest="0" * 64)
    result = run_benchmark(case.config, case.deps())
    assert case.calls.count("build_legacy_index") == 1
    report = json.loads(Path(result["report_path"]).read_text())
    assert report["legacy"]["fst_source"] == "built"
    # Nothing is written back to the arXiv-owned cache.
    assert cached.read_bytes() == b"cached-legacy-fst"
    assert json.loads((cached.parent / "_FST_COMPLETE.json").read_text())["fst_sha256"] == "0" * 64


def test_shared_url_stress_case_records_zero_citations_with_the_real_extractor(tmp_path):
    root = Path(__file__).resolve().parents[1]
    lines: list[str] = []
    events = EventLogger(output=lines.append)
    target = Path(os.environ.get("CARGO_TARGET_DIR") or tmp_path / "cargo-target")
    binary = build_extractor(root, target, events)
    work = tmp_path / "work"
    config = PipelineConfig(
        source_root=root, source_digest=runtime_source_digest(root), work_root=work,
        bucket_root=tmp_path / "mnt", smoke=False, force=False, datacite_release=None,
        hf_token=TOKEN, crossref_token=None, run_id=RUN, mode="benchmark",
        resources=SMOKE_RESOURCES,
    )
    deps = BenchmarkDependencies(
        events=events, api_factory=None, store_factory=None, resolve_mirror=None,
        fallback_transfer=None, build_extractor=None, discover_release=discover_release,
        ensure_bundle=ensure_bundle, run_extraction=run_datacite_extraction,
        transform=transform_datacite_outputs, build_legacy_index=None,
        run_legacy_extraction=None, list_hardware=None,
        memory_probe=lambda: MemoryReading(None, "unavailable"), prepare_dataset=None,
        publish_pair=None, publish_prepared=None, verify_current=None,
    )
    runner = MeasuredRunner()
    watermark = ResourceWatermark(work, interval=3600.0)
    meter = PhaseMeter(
        events, memory_probe=deps.memory_probe, watermark=watermark, work_root=work, runner=runner
    )
    cases = create_synthetic_stress(tmp_path / "synthetic", degree=12)
    result = _run_stress(config, deps, binary, meter, runner, cases["shared_url"], tmp_path / "synthetic")
    stats = result["extraction"]["stats"]
    assert stats["confirmed_matches"] == 0
    assert stats["ambiguous_urls"] == 12
    assert result["transform"]["confirmed_citations"] == 0
    assert result["transform"]["no_confirmed_citations"] is True
    for kind in ("datacite", "arxiv"):
        for config_name in ("all", "asserted", "mined"):
            assert result["transform"]["counts"][kind][config_name] == {
                "works": 0, "citations": 0, "references": 0,
            }
    assert result["transform"]["counts"]["ambiguous_urls"] == 12
    assert result["extraction"]["index_load_seconds"] is not None
    assert result["extraction"]["aggregation_seconds"] is not None


def test_run_benchmark_withholds_settings_for_a_tiny_source(tmp_path):
    case = Case(tmp_path, members=6)
    result = run_benchmark(case.config, case.deps())
    assert result["status"] == "benchmark-not-ready"
    assert result["representative"] is False
    assert result["settings_path"] is None
    local = case.work / "benchmarks" / RUN
    assert (local / "benchmark-report.json").is_file()
    assert not (local / "production-settings.json").exists()
    assert not (case.paths.prefix / "benchmarks" / RUN / "production-settings.json").exists()
    report = json.loads((local / "benchmark-report.json").read_text())
    assert report["sampling"]["selected_members"] == 6
    assert report["readiness"]["selected"] is not None
    assert report["readiness"]["withheld_reason"] == "sample is not representative"


def test_run_benchmark_refuses_other_modes_and_weak_budgets(tmp_path):
    case = Case(tmp_path)
    with pytest.raises(PipelineError, match="mode"):
        run_benchmark(PipelineConfig(**{**case.config.__dict__, "mode": "datacite"}), case.deps())
    weak = PipelineConfig(**{**case.config.__dict__, "resources": ResourceSettings(writer_memory_mb=64, aggregation_memory_mb=100, duckdb_memory_mb=256)})
    with pytest.raises(PipelineError, match="aggregation_memory_mb"):
        run_benchmark(weak, case.deps())
    assert not (case.work / "benchmarks").exists()


def test_run_benchmark_raises_on_measurement_failure(tmp_path):
    case = Case(tmp_path)
    deps = case.deps()

    def broken(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("extractor crashed")

    with pytest.raises(RuntimeError, match="extractor crashed"):
        run_benchmark(case.config, BenchmarkDependencies(**{**deps.__dict__, "run_extraction": broken}))
    assert not (case.work / "benchmarks" / RUN / "benchmark-report.json").exists()
