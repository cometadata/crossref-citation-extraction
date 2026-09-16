"""Representative benchmark of the full DataCite workload and the measured
production job settings it justifies.

`run_benchmark` reuses the full pipeline's acquisition, bundle, extraction
and transform phases against the real bucket, but never prepares or
publishes a dataset. It stages the real Crossref snapshot once, streams it
to select a strided-window sample of whole members (recording the members,
the traversal count and the decompression time), extracts every window and
the whole sample with the full DataCite bundle, runs the legacy arXiv
extraction on the same sample for a semantic comparison (reusing the legacy
pipeline's verified arXiv FST cache when one matches the release), transforms,
compresses and persists the sample outputs, runs two synthetic stress
archives (a high-degree target and a shared landing page), and extrapolates
a complete run from the slowest representative window's variable cost plus
the per-run fixed costs paid once. The extrapolation, the readiness ladder
and the pricing feed `benchmark-report.json`; `production-settings.json` is
written only for the first configuration with the required headroom, and
only when the sample is representative and the semantic comparison holds.

Every extractor child runs through `MeasuredRunner`, which forwards its log,
reads the index-load and aggregation durations it reports, and reads that
child's own peak resident set size from `os.wait4`. Memory readiness uses
those child peaks and the cgroup non-reclaimable high-water mark of the
phases production runs; the legacy comparison, the synthetic stress cases
and the Rust build are reported but never feed readiness. Disk is the summed
size of the trees the job owns under the work root, sampled by a thread and
at phase boundaries; `shutil.disk_usage` describes the node's shared
filesystem and is recorded for context only. Nothing is ever inferred from
serialized output size alone.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import math
import os
import re
import resource
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow.parquet as pq

from .arxiv_index import DataciteRelease as LegacyRelease
from .arxiv_index import ensure_fst
from .datacite_index import BundleResult, DataciteRelease, discover_release
from .datacite_publish import DatasetTransform, NoConfirmedCitationsError
from .extractor import (
    FULL_OUTPUT_KINDS,
    ExtractionReport,
    ExtractorBinary,
    FullExtractionOutputs,
    load_extraction_report,
    run_build_datacite_index,
    run_build_index,
    run_extraction,
)
from .full_pipeline import (
    PUBLICATION_KINDS,
    build_run_identity,
    counts_from_transforms,
    default_full_dependencies,
    full_paths,
    prepare_bundle,
    release_archive,
    resolve_snapshot,
    select_release,
    stage_archive,
    verify_source,
)
from .full_state import ResourceSettings, RunIdentity
from .paths import ARCHIVE_NAME, CONFIG_NAMES, DATACITE_PREFIX_NAME, PipelinePaths
from .pipeline import PipelineConfig
from .pipeline_io import PipelineError, compress_file
from .state import sha256_file, utc_now, write_atomic_json

BENCHMARK_MODE = "benchmark"
BENCHMARKS_DIR = "benchmarks"
REPORT_NAME = "benchmark-report.json"
SETTINGS_NAME = "production-settings.json"
REPORT_KIND = "datacite-benchmark-report"
REPORT_SCHEMA_VERSION = 1
SETTINGS_KEYS = (
    "source_tree_sha256",
    "snapshot_id",
    "datacite_release",
    "flavor",
    "timeout",
    "resources",
    "benchmark_report_sha256",
)

DEFAULT_STRIDE = 5000
DEFAULT_WINDOW = 8
SYNTHETIC_DEGREE = 200_000
SYNTHETIC_WORKS_PER_MEMBER = 5000
SYNTHETIC_RELEASE = "2026-07"
SYNTHETIC_TARGET = "10.5555/target"
SYNTHETIC_SHARED_URL = "https://example.org/shared"
SYNTHETIC_SHARED_DOIS = ("10.5555/shared-a", "10.5555/shared-b")
SAMPLE_COMPRESSION_LEVEL = 6

_GB = 10**9
_MIB = 1024 * 1024


@dataclass(frozen=True)
class Flavor:
    name: str
    vcpus: int
    memory_bytes: int
    disk_bytes: int


# Hugging Face Jobs hardware, as documented on 2026-09-15. Sizes use decimal
# gigabytes, which understates a binary reading and is therefore the
# conservative choice for headroom.
HARDWARE = {
    "cpu-basic": Flavor("cpu-basic", 2, 16 * _GB, 50 * _GB),
    "cpu-upgrade": Flavor("cpu-upgrade", 8, 32 * _GB, 50 * _GB),
    "cpu-xl": Flavor("cpu-xl", 16, 124 * _GB, 1000 * _GB),
    "cpu-performance": Flavor("cpu-performance", 32, 256 * _GB, 1024 * _GB),
}
DOCUMENTED_PRICING = {
    "cpu-basic": 0.01,
    "cpu-upgrade": 0.03,
    "cpu-xl": 1.00,
    "cpu-performance": 1.90,
}
PRICING_CHECKED = "2026-09-15"
PRICING_URL = "https://huggingface.co/docs/hub/jobs-pricing"
SMOKE_TIMEOUT_SECONDS = 30 * 60
TIMEOUT_SECONDS = {"24h": 86400, "48h": 172800}
READINESS_LADDER = (
    ("cpu-xl", "24h"),
    ("cpu-xl", "48h"),
    ("cpu-performance", "24h"),
    ("cpu-performance", "48h"),
)
# CPU-bound phases scale conservatively with the vCPU ratio (16 -> 32 is
# 0.6, not 0.5); memory and disk never scale.
CPU_SCALE = {("cpu-xl", "cpu-performance"): 0.6, ("cpu-performance", "cpu-xl"): 2.0}
CPU_BOUND_COMPONENTS = (
    "index_preparation",
    "extraction",
    "aggregation",
    "transform",
    "compression",
)
# The phases a production run also executes. Their memory feeds readiness;
# the benchmark-only phases (sampling, the legacy comparison, the synthetic
# stress cases) are reported but never do.
PRODUCTION_PHASES = (
    "prepare_index",
    "source_transfer",
    "extract_sample",
    "transform_sample",
    "compress_outputs",
    "persist_sample_outputs",
)
WINDOW_PHASE_PREFIX = "extract_window_"
TIME_UNCERTAINTY = 0.25
DISK_UNCERTAINTY = 1.5
GROUP_SCALE = 1.5
PYARROW_HEADROOM = 0.25
AGGREGATION_TEMP_FACTOR = 2
TIMEOUT_HEADROOM = 1.2
EXAMPLE_PAIRS = 20


# --- sampling ---


class _ArchiveWriter:
    """A deterministic gzip tar: zero gzip mtime, no gzip filename, streaming
    tar with fixed member metadata (the smoke fixture's discipline)."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._raw = path.open("wb")
        self._gzip = gzip.GzipFile(
            filename="",
            fileobj=self._raw,
            mode="wb",
            compresslevel=SAMPLE_COMPRESSION_LEVEL,
            mtime=0,
        )
        self._tar = tarfile.open(fileobj=self._gzip, mode="w|")

    def add(self, name: str, data: bytes) -> None:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mtime = 0
        info.mode = 0o644
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        self._tar.addfile(info, io.BytesIO(data))

    def close(self) -> None:
        self._tar.close()
        self._gzip.close()
        self._raw.close()


def _is_member(entry: tarfile.TarInfo) -> bool:
    # Exactly what the Rust reader counts: regular `*.json` entries.
    return entry.isreg() and entry.name.endswith(".json")


def _stream_select(
    source: Path,
    destination: Path,
    *,
    selected: Callable[[int], bool],
    window: int,
    windows_dir: Path | None,
) -> dict[str, Any]:
    total = chosen = skipped = 0
    total_bytes = selected_bytes = 0
    members: list[str] = []
    windows: list[dict[str, Any]] = []
    read_seconds = write_seconds = 0.0
    window_writer: _ArchiveWriter | None = None
    started = time.perf_counter()
    with source.open("rb") as raw, tarfile.open(fileobj=raw, mode="r|gz") as archive:
        writer = _ArchiveWriter(destination)
        try:
            entries: Iterator[tarfile.TarInfo] = iter(archive)
            while True:
                clock = time.perf_counter()
                try:
                    entry = next(entries)
                except StopIteration:
                    read_seconds += time.perf_counter() - clock
                    break
                read_seconds += time.perf_counter() - clock
                if not _is_member(entry):
                    skipped += 1
                    continue
                index = total
                total += 1
                total_bytes += entry.size
                if not selected(index):
                    continue
                clock = time.perf_counter()
                handle = archive.extractfile(entry)
                data = handle.read() if handle is not None else b""
                read_seconds += time.perf_counter() - clock
                position = chosen
                chosen += 1
                selected_bytes += len(data)
                members.append(entry.name)
                if position % window == 0:
                    if window_writer is not None:
                        window_writer.close()
                        window_writer = None
                    record: dict[str, Any] = {
                        "index": len(windows),
                        "members": [],
                        "bytes": 0,
                        "doi_keys": 0,
                        "url_keys": 0,
                        "archive": None,
                    }
                    if windows_dir is not None:
                        path = (
                            windows_dir / f"window-{record['index']:02d}" / ARCHIVE_NAME
                        )
                        window_writer = _ArchiveWriter(path)
                        record["archive"] = str(path)
                    windows.append(record)
                current = windows[-1]
                current["members"].append(entry.name)
                current["bytes"] += len(data)
                current["doi_keys"] += data.count(b'"DOI"')
                current["url_keys"] += data.count(b'"URL"')
                clock = time.perf_counter()
                writer.add(entry.name, data)
                if window_writer is not None:
                    window_writer.add(entry.name, data)
                write_seconds += time.perf_counter() - clock
        finally:
            clock = time.perf_counter()
            writer.close()
            if window_writer is not None:
                window_writer.close()
            write_seconds += time.perf_counter() - clock
    for record in windows:
        size = record["bytes"] or 1
        record["doi_density"] = record["doi_keys"] / size
        record["url_density"] = record["url_keys"] / size
    return {
        "total_members": total,
        "selected_members": chosen,
        "skipped_entries": skipped,
        "total_bytes": total_bytes,
        "selected_bytes": selected_bytes,
        "members": members,
        "windows": windows,
        "elapsed_seconds": time.perf_counter() - started,
        "read_seconds": read_seconds,
        "write_seconds": write_seconds,
    }


def _densest(windows: list[dict[str, Any]], key: str) -> int | None:
    if not windows:
        return None
    return max(windows, key=lambda record: (record[key], -record["index"]))["index"]


def sample_crossref_archive(
    source: Path,
    destination: Path,
    stride: int = DEFAULT_STRIDE,
    window: int = DEFAULT_WINDOW,
    *,
    windows_dir: Path | None = None,
) -> dict[str, Any]:
    """Stream `source` once and write the members whose index modulo
    `stride` is below `window` to `destination`, whole and in order.

    The source is decompressed in stream mode, so the traversal itself is a
    measured cost (`read_seconds`, separate from `write_seconds`). Selected
    members are grouped into consecutive windows of `window` members; the
    DOI-heavy and URL-heavy windows are the ones with the most `"DOI"` and
    `"URL"` keys per decompressed byte, scored in the same pass. With
    `windows_dir` every window is also written as its own archive.

    A source with fewer than `2 * stride` members cannot span enough windows
    to extrapolate from: it is re-streamed once selecting every member and
    flagged `representative=False`. That second pass only ever happens for
    sources that are small by definition.
    """
    if stride <= 0 or window <= 0 or window > stride:
        raise ValueError("stride and window must be positive with window <= stride")
    result = _stream_select(
        source,
        destination,
        selected=lambda index: index % stride < window,
        window=window,
        windows_dir=windows_dir,
    )
    passes = 1
    representative = result["total_members"] >= 2 * stride
    if not representative and result["selected_members"] != result["total_members"]:
        if windows_dir is not None and windows_dir.exists():
            shutil.rmtree(windows_dir)
        result = _stream_select(
            source,
            destination,
            selected=lambda index: True,
            window=window,
            windows_dir=windows_dir,
        )
        passes = 2
    result.update(
        {
            "stride": stride,
            "window": window,
            "passes": passes,
            "representative": representative,
            "source_bytes": source.stat().st_size,
            "sample_bytes": destination.stat().st_size,
            "sample_sha256": sha256_file(destination),
            "doi_heavy_window": _densest(result["windows"], "doi_density"),
            "url_heavy_window": _densest(result["windows"], "url_density"),
        }
    )
    return result


# --- measurement ---


class MemoryReading(NamedTuple):
    peak_bytes: int | None
    source: str


def maxrss_to_bytes(value: int, platform: str = sys.platform) -> int:
    """`ru_maxrss` is bytes on macOS and kibibytes everywhere else."""
    return value if platform.startswith("darwin") else value * 1024


def read_cgroup_peak(root: Path = Path("/sys/fs/cgroup")) -> MemoryReading:
    """The job's cgroup memory peak: v2 `memory.peak`, else v1
    `memory/memory.max_usage_in_bytes`, else unavailable."""
    for relative, source in (
        ("memory.peak", "cgroup-v2"),
        ("memory/memory.max_usage_in_bytes", "cgroup-v1"),
    ):
        try:
            return MemoryReading(int((root / relative).read_text().strip()), source)
        except (OSError, ValueError):
            continue
    return MemoryReading(None, "unavailable")


def _stat_counters(path: Path) -> dict[str, int]:
    counters: dict[str, int] = {}
    for line in path.read_text().splitlines():
        name, _, value = line.partition(" ")
        if name and value.strip().isdigit():
            counters[name] = int(value)
    return counters


def read_cgroup_resident(root: Path = Path("/sys/fs/cgroup")) -> MemoryReading:
    """The job's cgroup non-reclaimable memory right now: v2 `memory.stat`
    `anon + kernel`, else v1 `memory/memory.stat` `total_rss` (or `rss`),
    else unavailable. Unlike `memory.peak`/`memory.current`, this excludes
    the file-backed page cache that staging and streaming the archive fill,
    so a high-water mark over samples reflects the process footprint."""
    try:
        counters = _stat_counters(root / "memory.stat")
        if "anon" in counters:
            return MemoryReading(
                counters["anon"] + counters.get("kernel", 0), "cgroup-v2-stat"
            )
    except OSError:
        pass
    try:
        counters = _stat_counters(root / "memory" / "memory.stat")
        for name in ("total_rss", "rss"):
            if name in counters:
                return MemoryReading(counters[name], "cgroup-v1-stat")
    except OSError:
        pass
    return MemoryReading(None, "unavailable")


def rusage_children_bytes() -> int:
    return maxrss_to_bytes(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)


def rusage_self_bytes() -> int:
    return maxrss_to_bytes(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def is_production_phase(name: str) -> bool:
    return name in PRODUCTION_PHASES or name.startswith(WINDOW_PHASE_PREFIX)


def tree_bytes(root: Path) -> int:
    """Explicit `du`-style size of every regular file under `root` (symlinks
    are not followed). Files that disappear during the walk are skipped."""
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _filesystem_used(path: Path) -> int | None:
    try:
        return shutil.disk_usage(path).used
    except OSError:
        return None


# The extractor's phase completion lines, e.g.
# `event=phase phase=index_load status=completed ... elapsed_s=3.812`.
_PHASE_COMPLETED = re.compile(
    r"event=phase phase=(?P<phase>\w+) status=completed\b.*?"
    r"\belapsed_s=(?P<seconds>\d+(?:\.\d+)?)"
)


@dataclass(frozen=True)
class ChildRun:
    """One extractor invocation: its subcommand, exit status, wall time, its
    own peak resident set size, and the phase durations it logged."""

    command: str
    returncode: int
    elapsed_seconds: float
    peak_rss_bytes: int | None
    phase_seconds: dict[str, float]


class MeasuredRunner:
    """A `subprocess.run` stand-in for the extractor invocations.

    The child's stdout is forwarded line by line (the job log keeps every
    extractor event) and scanned for `event=phase ... status=completed ...
    elapsed_s=` lines. The child is reaped with `os.wait4`, whose rusage
    describes that process alone, so a phase's memory is the memory of the
    process production runs rather than `RUSAGE_CHILDREN`'s running maximum
    over every child ever started (the Rust build, the legacy index build).
    """

    def __init__(self, output: Any | None = None) -> None:
        self._output = output
        self.runs: list[ChildRun] = []

    def __call__(
        self, arguments: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess:
        output = self._output if self._output is not None else sys.stdout
        phases: dict[str, float] = {}
        started = time.perf_counter()
        process = subprocess.Popen(
            arguments,
            stdout=subprocess.PIPE,
            text=True,
            errors="replace",
            **kwargs,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                output.write(line)
                output.flush()
                match = _PHASE_COMPLETED.search(line)
                if match is not None:
                    phases[match.group("phase")] = float(match.group("seconds"))
        except BaseException:
            process.kill()
            raise
        finally:
            process.stdout.close()
            _pid, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        run = ChildRun(
            command=arguments[1] if len(arguments) > 1 else Path(arguments[0]).name,
            returncode=process.returncode,
            elapsed_seconds=time.perf_counter() - started,
            peak_rss_bytes=maxrss_to_bytes(usage.ru_maxrss) if usage.ru_maxrss else None,
            phase_seconds=phases,
        )
        self.runs.append(run)
        return subprocess.CompletedProcess(arguments, process.returncode)


def timing_summary(
    elapsed_seconds: float, members: int, child: Any | None
) -> dict[str, Any]:
    """Split one extraction run into the per-run fixed costs the extractor
    logged (index load, aggregation) and the variable remainder.

    A run that logged neither keeps its whole duration variable, which only
    overstates the per-member cost."""
    phases = getattr(child, "phase_seconds", None) or {}
    index_load = phases.get("index_load")
    aggregation = phases.get("aggregation")
    fixed = (index_load or 0.0) + (aggregation or 0.0)
    variable = max(elapsed_seconds - fixed, 0.0)
    return {
        "index_load_seconds": index_load,
        "aggregation_seconds": aggregation,
        "fixed_seconds": fixed,
        "variable_seconds": variable,
        "variable_seconds_per_member": variable / max(members, 1),
    }


class ResourceWatermark:
    """A sampler thread polling every `interval` seconds: the job's own disk
    usage, the summed size of the trees it owns (`owned_roots`, by default
    the work root that holds the staged archive, the bundle copy, the
    partition and temp directories, the outputs and the transform scratch),
    as run-wide and per-phase high-water marks plus an optional watched tree;
    and, through `memory_sampler`, the cgroup's non-reclaimable memory, kept
    as a high-water mark over samples (run-wide and per phase).

    `shutil.disk_usage` describes the node's shared filesystem, other
    tenants included; it is recorded for context and never feeds readiness.
    The owned roots must not nest."""

    def __init__(
        self,
        work_root: Path,
        *,
        owned_roots: Iterable[Path] | None = None,
        interval: float = 1.0,
        memory_sampler: Callable[[], MemoryReading] | None = None,
    ) -> None:
        self.work_root = work_root
        self.owned_roots = tuple(owned_roots) if owned_roots is not None else (work_root,)
        self.interval = interval
        self.memory_sampler = memory_sampler
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._watch: Path | None = None
        self.baseline_owned = 0
        self.peak_owned = 0
        self.phase_peak_owned = 0
        self.watch_peak = 0
        self.filesystem_used: int | None = None
        self.filesystem_used_peak: int | None = None
        self.samples = 0
        self.memory_peak: int | None = None
        self.phase_memory_peak: int | None = None
        self.memory_source = "unavailable"

    def start(self) -> None:
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.baseline_owned = self.owned_bytes()
        self.poll()
        self._thread = threading.Thread(
            target=self._run, name="resource-watermark", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        self.poll()

    def owned_bytes(self) -> int:
        return sum(tree_bytes(root) for root in self.owned_roots)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self.poll()

    def poll(self) -> int:
        """Sample once; returns the owned bytes right now."""
        owned = self.owned_bytes()
        watch = self._watch
        watched = tree_bytes(watch) if watch is not None else 0
        filesystem = _filesystem_used(self.work_root)
        memory = self.memory_sampler() if self.memory_sampler is not None else None
        with self._lock:
            self.samples += 1
            self.peak_owned = max(self.peak_owned, owned)
            self.phase_peak_owned = max(self.phase_peak_owned, owned)
            self.watch_peak = max(self.watch_peak, watched)
            if filesystem is not None:
                self.filesystem_used = filesystem
                self.filesystem_used_peak = max(self.filesystem_used_peak or 0, filesystem)
            if memory is not None and memory.peak_bytes is not None:
                self.memory_source = memory.source
                self.memory_peak = max(self.memory_peak or 0, memory.peak_bytes)
                self.phase_memory_peak = max(
                    self.phase_memory_peak or 0, memory.peak_bytes
                )
        return owned

    def mark_phase(self, watch: Path | None = None) -> int:
        with self._lock:
            self._watch = watch
            self.watch_peak = 0
            self.phase_peak_owned = 0
            self.phase_memory_peak = None
        return self.poll()

    def unwatch(self) -> None:
        with self._lock:
            self._watch = None


class PhaseMeter:
    """Wraps `events.phase` and records per phase: wall-clock; the peak
    resident set size and logged phase durations of every extractor child
    `runner` ran during the phase; the cgroup non-reclaimable high-water mark;
    the owned-tree disk marks; and, for context only, `getrusage` running
    maxima, the cgroup peak (page cache included) and the node filesystem's
    usage."""

    def __init__(
        self,
        events: Any,
        *,
        memory_probe: Callable[[], MemoryReading],
        watermark: ResourceWatermark,
        work_root: Path,
        runner: MeasuredRunner | None = None,
    ) -> None:
        self.events = events
        self.memory_probe = memory_probe
        self.disk = watermark
        self.work_root = work_root
        self.runner = runner
        self.phases: dict[str, dict[str, Any]] = {}
        self.memory_source = "rusage"

    @contextmanager
    def phase(
        self, name: str, *, watch: Path | None = None
    ) -> Iterator[dict[str, Any]]:
        filesystem_before = _filesystem_used(self.work_root)
        owned_before = self.disk.mark_phase(watch)
        children_before = rusage_children_bytes()
        cgroup_before = self.memory_probe()
        runs_before = len(self.runner.runs) if self.runner is not None else 0
        record: dict[str, Any] = {}
        started = time.perf_counter()
        with self.events.phase(name):
            yield record
        elapsed = time.perf_counter() - started
        owned_after = self.disk.poll()
        self.disk.unwatch()
        children_after = rusage_children_bytes()
        cgroup_after = self.memory_probe()
        resident_peak = self.disk.phase_memory_peak
        if resident_peak is not None:
            self.memory_source = self.disk.memory_source
        children = self.runner.runs[runs_before:] if self.runner is not None else []
        child_peaks = [run.peak_rss_bytes for run in children if run.peak_rss_bytes]
        record.update(
            {
                "elapsed_seconds": elapsed,
                "production_phase": is_production_phase(name),
                "children": [asdict(run) for run in children],
                "memory": {
                    "child_peak_rss_bytes": max(child_peaks) if child_peaks else None,
                    "children_maxrss_before_bytes": children_before,
                    "children_maxrss_after_bytes": children_after,
                    "children_maxrss_delta_bytes": children_after - children_before,
                    "self_maxrss_after_bytes": rusage_self_bytes(),
                    "cgroup_peak_before_bytes": cgroup_before.peak_bytes,
                    "cgroup_peak_after_bytes": cgroup_after.peak_bytes,
                    "cgroup_source": cgroup_after.source,
                    "cgroup_resident_peak_bytes": resident_peak,
                    "cgroup_resident_source": self.disk.memory_source,
                },
                "disk": {
                    "owned_before_bytes": owned_before,
                    "owned_after_bytes": owned_after,
                    "phase_peak_owned_bytes": self.disk.phase_peak_owned,
                    "phase_growth_bytes": max(
                        self.disk.phase_peak_owned - owned_before, 0
                    ),
                    "watched_tree_peak_bytes": self.disk.watch_peak if watch else None,
                    "filesystem_used_before_bytes": filesystem_before,
                    "filesystem_used_after_bytes": _filesystem_used(self.work_root),
                },
            }
        )
        self.phases[name] = record
        self.events.emit(
            "phase_measured",
            phase=name,
            elapsed_seconds=round(elapsed, 3),
            child_peak_rss_bytes=record["memory"]["child_peak_rss_bytes"],
            cgroup_peak_after_bytes=cgroup_after.peak_bytes,
            cgroup_resident_peak_bytes=resident_peak,
            phase_peak_owned_bytes=self.disk.phase_peak_owned,
        )


def extraction_budget_bytes(
    resources: ResourceSettings, bundle_bytes: int, workers: int
) -> int:
    """The extractor's configured upper bound: writer and aggregation
    budgets, the result queue, per-worker lookup budgets, the largest single
    entry, and the memory-mapped bundle."""
    return (
        resources.writer_memory_mb
        + resources.aggregation_memory_mb
        + resources.result_batch_mb * resources.queue_batches
        + resources.lookup_memory_mb * workers
        + resources.max_entry_mb
    ) * _MIB + bundle_bytes


def transform_budget_bytes(resources: ResourceSettings) -> int:
    """DuckDB's limit plus the pyarrow merge buffers that live outside it."""
    return int(resources.duckdb_memory_mb * _MIB * (1 + PYARROW_HEADROOM))


# --- extrapolation ---


def extrapolate(numbers: dict[str, Any]) -> dict[str, Any]:
    """Project a complete run from the measured numbers, printing every
    formula.

    Extraction time is the slowest representative window's variable cost
    per member (its duration minus the index load and aggregation that
    extractor run logged) x total members; each window run pays those fixed
    costs once, and so does the full run: the index load as the slowest
    measured load, the aggregation as the sample's aggregation scaled by the
    row ratio, never below the slowest measured aggregation. Matched rows and
    output bytes follow the densest representative window (per-member rates
    x total members), so every scaled term follows the worst window rather
    than the sample mean. Memory takes the extractor children's own peaks
    scaled for larger cited-work groups, never below the configured budgets,
    and the cgroup non-reclaimable high-water mark of the phases production
    runs; the legacy comparison and the Rust build never count. Disk comes
    from the trees the job owns, never the node's filesystem."""
    total = numbers["total_members"]
    sampled = max(numbers["sampled_members"], 1)
    member_scale = total / sampled
    slowest = max(numbers["window_seconds_per_member"] or [0.0])
    slowest_variable = max(numbers["window_variable_seconds_per_member"] or [0.0])
    sample_matches = numbers["sample_confirmed_matches"]
    sample_raw = numbers["raw_output_bytes"]
    # Densest-window rates; the sample mean is the floor when a run had no
    # windows (never the case after sampling, but keeps the formula total).
    matches_per_member = max(
        numbers["window_confirmed_matches_per_member"] or [sample_matches / sampled]
    )
    raw_per_member = max(
        numbers["window_raw_bytes_per_member"] or [sample_raw / sampled]
    )
    projected_matches = matches_per_member * total
    projected_raw = raw_per_member * total
    row_ratio = projected_matches / sample_matches if sample_matches else member_scale
    byte_ratio = projected_raw / sample_raw if sample_raw else member_scale
    projected_ambiguous = numbers["sample_ambiguous_urls"] * row_ratio

    persistence_seconds = numbers["persistence_seconds"]
    persisted = numbers["persisted_bytes"]
    derived_projected = numbers["derived_bytes"] * byte_ratio
    persistence_projected = persistence_seconds * byte_ratio
    if persistence_seconds > 0 and persisted > 0:
        publication = derived_projected / (persisted / persistence_seconds)
    else:
        publication = persistence_projected
    aggregation = max(
        float(numbers["aggregation_seconds_max"]),
        numbers["sample_aggregation_seconds"] * row_ratio,
    )
    components = {
        "source_transfer": float(numbers["source_transfer_seconds"]),
        "index_preparation": float(numbers["index_preparation_seconds"]),
        "extraction": slowest_variable * total,
        "index_load": float(numbers["index_load_seconds"]),
        "aggregation": aggregation,
        "transform": numbers["transform_seconds"] * row_ratio,
        "compression": numbers["compression_seconds"] * byte_ratio,
        "persistence": persistence_projected,
        "publication": publication,
    }
    point = sum(components.values())

    segments = max(numbers["segment_bytes"], sample_raw) * byte_ratio
    raw = sample_raw * byte_ratio
    fixed = numbers["fixed_work_bytes"]
    before = (
        fixed
        + numbers["archive_bytes"]
        + segments
        + AGGREGATION_TEMP_FACTOR * segments
        + raw
    )
    after = (
        fixed
        + raw
        + numbers["transform_scratch_bytes"] * byte_ratio
        + derived_projected
        + numbers["compressed_output_bytes"] * byte_ratio
    )
    observed = float(numbers["observed_peak_owned_bytes"])
    disk_point = max(before, after, observed)

    extraction_memory = max(
        numbers["extraction_child_peak_bytes"] * GROUP_SCALE,
        numbers["extraction_budget_bytes"],
    )
    transform_memory = max(
        numbers["transform_self_peak_bytes"] * GROUP_SCALE,
        numbers["transform_budget_bytes"],
    )
    candidates = [extraction_memory, transform_memory]
    resident = numbers.get("production_resident_peak_bytes")
    if resident is not None:
        candidates.append(float(resident))
    peak_memory = max(candidates)

    formulas = {
        "extraction": (
            "slowest representative window variable seconds per member x total "
            "members, where a window's variable seconds are its measured duration "
            "minus the index load and aggregation seconds its extractor run logged "
            "(never negative)"
        ),
        "index_load": (
            "slowest index load any benchmark extraction logged, paid once by the "
            "full run (never multiplied by members)"
        ),
        "aggregation": (
            "max(slowest aggregation any benchmark extraction logged, sample "
            "aggregation seconds x row ratio), paid once by the full run"
        ),
        "member_scale": "total members / sampled members (informational)",
        "projected_confirmed_matches": (
            "max over representative windows of (confirmed matches / members) "
            "x total members (the densest window, not the sample mean)"
        ),
        "projected_raw_output_bytes": (
            "max over representative windows of (raw output bytes / members) "
            "x total members"
        ),
        "row_ratio": (
            "projected confirmed matches / sample confirmed matches; scales the "
            "row-driven terms (aggregation, transform seconds, ambiguity rows); "
            "member scale when the sample matched nothing"
        ),
        "byte_ratio": (
            "projected raw output bytes / sample raw output bytes; scales every "
            "byte-driven term (raw, segments, scratch, derived, compressed, "
            "compression and persistence seconds); member scale when the sample "
            "produced no output"
        ),
        "transform": "measured transform seconds x row ratio",
        "compression": "measured compression seconds x byte ratio",
        "persistence": "measured persistence seconds x byte ratio",
        "publication": (
            "projected derived bytes / measured persistence bytes per second "
            "(assumes Hub uploads run at the bucket persistence rate)"
        ),
        "source_transfer": "measured staging of the full archive (already full scale)",
        "index_preparation": "measured bundle build or verified reuse (already full scale)",
        "total": (
            "sum of components; reported value = sum x (1 + TIME_UNCERTAINTY), "
            "low band = sum x (1 - TIME_UNCERTAINTY)"
        ),
        "disk": (
            "sizes of the trees the job owns under the work root, never the node "
            "filesystem: before release = fixed work tree + staged archive + segments + "
            "AGGREGATION_TEMP_FACTOR x segments + raw outputs; after release = fixed work "
            "tree + raw outputs + transform scratch + derived + compressed outputs; "
            "segments = max(observed temp peak, raw output bytes) x byte ratio; every "
            "output term scales by byte ratio; peak = max(before, after, observed "
            "owned-tree high-water mark) x DISK_UNCERTAINTY"
        ),
        "memory": (
            "max(extraction child peak x GROUP_SCALE, configured extraction budget), "
            "max(transform self peak x GROUP_SCALE, DuckDB limit x (1 + PYARROW_HEADROOM)), "
            "cgroup non-reclaimable high-water mark (anon + kernel, page cache excluded) "
            "of the phases production runs; the largest wins. Child peaks are each "
            "extractor process's own ru_maxrss (os.wait4); the Rust build, the legacy "
            "index build and extraction, and the synthetic stress cases are excluded"
        ),
    }
    return {
        "inputs": dict(numbers),
        "member_scale": member_scale,
        "row_ratio": row_ratio,
        "byte_ratio": byte_ratio,
        "densest_window_matches_per_member": matches_per_member,
        "densest_window_raw_bytes_per_member": raw_per_member,
        "slowest_window_seconds_per_member": slowest,
        "slowest_window_variable_seconds_per_member": slowest_variable,
        "projected_confirmed_matches": projected_matches,
        "projected_raw_output_bytes": projected_raw,
        "projected_ambiguous_urls": projected_ambiguous,
        "components_seconds": components,
        "projected_total_seconds_point": point,
        "projected_total_seconds_low": point * (1 - TIME_UNCERTAINTY),
        "projected_total_seconds": point * (1 + TIME_UNCERTAINTY),
        "disk": {
            "segments_bytes": segments,
            "raw_output_bytes": raw,
            "derived_bytes": derived_projected,
            "before_release_bytes": before,
            "after_release_bytes": after,
            "observed_peak_owned_bytes": observed,
        },
        "projected_peak_disk_bytes_point": disk_point,
        "projected_peak_disk_bytes": disk_point * DISK_UNCERTAINTY,
        "memory": {
            "extraction_bytes": extraction_memory,
            "transform_bytes": transform_memory,
            "production_resident_bytes": resident,
            "legacy_bytes": numbers.get("legacy_child_peak_bytes"),
        },
        "peak_memory_bytes": peak_memory,
        "uncertainty": {
            "time": TIME_UNCERTAINTY,
            "disk": DISK_UNCERTAINTY,
            "group_scale": GROUP_SCALE,
            "pyarrow_headroom": PYARROW_HEADROOM,
        },
        "formulas": formulas,
    }


# --- readiness ---


def evaluate_readiness(
    report: dict[str, Any], *, memory_bytes: int, disk_bytes: int, timeout_seconds: int
) -> dict[str, Any]:
    failed = []
    if report["peak_memory_bytes"] > memory_bytes * 0.75:
        failed.append("memory")
    if report["projected_peak_disk_bytes"] > disk_bytes * 0.75:
        failed.append("disk")
    if report["projected_total_seconds"] > timeout_seconds * 0.80:
        failed.append("time")
    return {"ready": not failed, "failed_limits": failed}


def scale_total_seconds(report: dict[str, Any], flavor: str) -> float:
    """The projected total on `flavor`: unchanged on the measured flavor,
    otherwise CPU-bound components scaled by `CPU_SCALE`."""
    measured = report.get("measured_on", "cpu-xl")
    if flavor == measured:
        return float(report["projected_total_seconds"])
    factor = CPU_SCALE.get((measured, flavor), 1.0)
    point = sum(
        seconds * (factor if name in CPU_BOUND_COMPONENTS else 1.0)
        for name, seconds in report["components_seconds"].items()
    )
    return point * (1 + TIME_UNCERTAINTY)


def select_configuration(report: dict[str, Any]) -> dict[str, Any]:
    """Walk `READINESS_LADDER` and stop at the first ready configuration,
    retaining every evaluation made on the way."""
    evaluations: list[dict[str, Any]] = []
    selected: dict[str, str] | None = None
    for flavor_name, timeout in READINESS_LADDER:
        flavor = HARDWARE[flavor_name]
        scaled = {
            "peak_memory_bytes": report["peak_memory_bytes"],
            "projected_peak_disk_bytes": report["projected_peak_disk_bytes"],
            "projected_total_seconds": scale_total_seconds(report, flavor_name),
        }
        verdict = evaluate_readiness(
            scaled,
            memory_bytes=flavor.memory_bytes,
            disk_bytes=flavor.disk_bytes,
            timeout_seconds=TIMEOUT_SECONDS[timeout],
        )
        evaluations.append(
            {
                "flavor": flavor_name,
                "timeout": timeout,
                "memory_bytes": flavor.memory_bytes,
                "disk_bytes": flavor.disk_bytes,
                "timeout_seconds": TIMEOUT_SECONDS[timeout],
                **scaled,
                **verdict,
            }
        )
        if verdict["ready"]:
            selected = {"flavor": flavor_name, "timeout": timeout}
            break
    return {"evaluations": evaluations, "selected": selected}


# --- pricing ---


def _hourly(entry: Any) -> float:
    unit = getattr(entry, "unit_label", None)
    cost = float(getattr(entry, "unit_cost_usd"))
    per_hour = {"minute": 60.0, "hour": 1.0, "second": 3600.0}.get(unit)
    if per_hour is None:
        raise ValueError(
            f"unknown billing unit {unit!r} for {getattr(entry, 'name', '?')}"
        )
    return round(cost * per_hour, 6)


def fetch_pricing(
    list_hardware: Callable[[], Iterable[Any]] | None = None,
) -> dict[str, Any]:
    """Hourly USD per planned flavor, fetched at execution time; the
    documented table stands in when the call fails or is incomplete."""
    fetched_at = utc_now()
    try:
        if list_hardware is None:
            from huggingface_hub import HfApi

            listing = list(HfApi().list_jobs_hardware())
        else:
            listing = list(list_hardware())
        by_name = {getattr(entry, "name", None): entry for entry in listing}
        hourly: dict[str, float] = {}
        for name in DOCUMENTED_PRICING:
            if name not in by_name:
                raise ValueError(f"hardware listing does not include {name}")
            hourly[name] = _hourly(by_name[name])
        return {
            "pricing_source": "hub-api",
            "hourly_usd": hourly,
            "fetched_at": fetched_at,
            "error": None,
            "documented": dict(DOCUMENTED_PRICING),
            "documented_checked": PRICING_CHECKED,
            "documented_url": PRICING_URL,
        }
    except Exception as error:
        return {
            "pricing_source": "documented-fallback",
            "hourly_usd": dict(DOCUMENTED_PRICING),
            "fetched_at": fetched_at,
            "error": f"{type(error).__name__}: {error}",
            "documented": dict(DOCUMENTED_PRICING),
            "documented_checked": PRICING_CHECKED,
            "documented_url": PRICING_URL,
        }


def cost_estimate(*, seconds: float, hourly_usd: float, flavor: str) -> dict[str, Any]:
    """Per-minute billing while Starting/Running, rounded up to whole minutes."""
    minutes = math.ceil(max(seconds, 0) / 60)
    return {
        "flavor": flavor,
        "seconds": seconds,
        "billed_minutes": minutes,
        "hourly_usd": hourly_usd,
        "usd": minutes * hourly_usd / 60,
    }


# --- settings and synthetic fixtures ---


def write_production_settings(
    path: Path,
    *,
    report_path: Path,
    source_tree_sha256: str,
    snapshot_id: str,
    datacite_release: str,
    flavor: str,
    timeout: str,
    resources: ResourceSettings,
) -> None:
    """Exactly the contract keys; the digest is of the report bytes as written."""
    write_atomic_json(
        path,
        {
            "source_tree_sha256": source_tree_sha256,
            "snapshot_id": snapshot_id,
            "datacite_release": datacite_release,
            "flavor": flavor,
            "timeout": timeout,
            "resources": asdict(resources),
            "benchmark_report_sha256": hashlib.sha256(
                report_path.read_bytes()
            ).hexdigest(),
        },
    )


@dataclass(frozen=True)
class SyntheticCase:
    name: str
    archive: Path
    release_root: Path
    degree: int
    target: str | None = None
    url: str | None = None


def write_crossref_archive(
    path: Path, members: Iterable[tuple[str, list[dict[str, Any]]]]
) -> int:
    """Write `{"items": works}` members deterministically; returns the count."""
    writer = _ArchiveWriter(path)
    count = 0
    try:
        for name, works in members:
            payload = (json.dumps({"items": works}, ensure_ascii=False) + "\n").encode(
                "utf-8"
            )
            writer.add(name, payload)
            count += 1
    finally:
        writer.close()
    return count


def write_datacite_release(
    root: Path, registry: Iterable[tuple[str, str]], release: str
) -> None:
    """A complete single-month release for `(doi, landing url)` pairs, in the
    mirror layout the real `discover_release` accepts."""
    month_dir = f"dois/updated_{release}"
    pairs = list(registry)
    metadata = "".join(
        json.dumps(
            {
                "id": doi,
                "type": "dois",
                "attributes": {"doi": doi, "url": url, "state": "findable"},
            },
            separators=(",", ":"),
        )
        + "\n"
        for doi, url in pairs
    ).encode("utf-8")
    states = (
        "doi,state,client_id,updated\n"
        + "".join(
            f"{doi},findable,example.bench,{release}-15T00:00:00.000Z\n"
            for doi, _url in pairs
        )
    ).encode("utf-8")
    entries = []
    for relative, data in (
        (f"{month_dir}/part_0000.jsonl.gz", gzip.compress(metadata, mtime=0)),
        (f"{month_dir}/{release}.csv.gz", gzip.compress(states, mtime=0)),
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        entries.append(
            {
                "filename": relative,
                "size": len(data),
                "sha256": base64.b64encode(hashlib.sha256(data).digest()).decode(
                    "ascii"
                ),
            }
        )
    (root / "MANIFEST.json").write_text(json.dumps(entries, indent=2) + "\n")
    (root / "STATUS.json").write_text(
        json.dumps(
            {
                "month": release,
                "datetime": "2026-08-01T00:00:00+00:00",
                "status": "Complete",
            },
            indent=2,
        )
        + "\n"
    )
    (root / "_TRANSFER_COMPLETE.json").write_text(
        json.dumps({"release": release, "source": "benchmark synthetic fixture"}) + "\n"
    )


def _chunked(
    works: Iterable[dict[str, Any]], size: int
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    batch: list[dict[str, Any]] = []
    index = 0
    for work in works:
        batch.append(work)
        if len(batch) == size:
            yield f"{index}.json", batch
            batch = []
            index += 1
    if batch:
        yield f"{index}.json", batch


def create_synthetic_stress(
    root: Path, *, degree: int = SYNTHETIC_DEGREE
) -> dict[str, SyntheticCase]:
    """Two deterministic stress archives and the small release they cite:
    `high_degree` (one registered DOI cited by `degree` works through a
    publisher-asserted structured DOI) and `shared_url` (one landing page
    registered to two DOIs, cited by `degree` works by URL alone)."""
    release_root = root / "datacite" / f"datacite-monthly-data-file-{SYNTHETIC_RELEASE}"
    write_datacite_release(
        release_root,
        [
            (SYNTHETIC_TARGET, "https://example.org/target"),
            *((doi, SYNTHETIC_SHARED_URL) for doi in SYNTHETIC_SHARED_DOIS),
        ],
        SYNTHETIC_RELEASE,
    )
    high_archive = root / "high-degree" / "crossref" / ARCHIVE_NAME
    write_crossref_archive(
        high_archive,
        _chunked(
            (
                {
                    "DOI": f"10.9999/hd{i}",
                    "reference": [
                        {
                            "key": "r0",
                            "DOI": SYNTHETIC_TARGET,
                            "doi-asserted-by": "publisher",
                            "unstructured": f"Dataset {SYNTHETIC_TARGET} cited by work {i}",
                        }
                    ],
                }
                for i in range(degree)
            ),
            SYNTHETIC_WORKS_PER_MEMBER,
        ),
    )
    shared_archive = root / "shared-url" / "crossref" / ARCHIVE_NAME
    write_crossref_archive(
        shared_archive,
        _chunked(
            (
                {
                    "DOI": f"10.9999/su{i}",
                    "reference": [
                        {
                            "key": "r0",
                            "URL": SYNTHETIC_SHARED_URL,
                            "unstructured": f"Shared landing page cited by work {i}",
                        }
                    ],
                }
                for i in range(degree)
            ),
            SYNTHETIC_WORKS_PER_MEMBER,
        ),
    )
    return {
        "high_degree": SyntheticCase(
            "high_degree", high_archive, release_root, degree, target=SYNTHETIC_TARGET
        ),
        "shared_url": SyntheticCase(
            "shared_url", shared_archive, release_root, degree, url=SYNTHETIC_SHARED_URL
        ),
    }


# --- semantic comparison ---


def _legacy_pairs(valid: Path) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    with valid.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as error:
                raise PipelineError(f"{valid.name}:{number}: malformed JSON") from error
            target = str(row["doi"]).lower()
            for entry in row.get("cited_by", []):
                pairs.add((str(entry["doi"]), target))
    return pairs


def _projection_pairs(files: Iterable[Path]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for path in files:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(columns=["arxiv_doi", "cited_by"]):
            for target, citers in zip(
                batch.column("arxiv_doi").to_pylist(),
                batch.column("cited_by").to_pylist(),
                strict=True,
            ):
                pairs.update((citer["doi"], str(target).lower()) for citer in citers)
    return pairs


def compare_arxiv_projection(
    legacy_valid: Path, projection_files: Iterable[Path]
) -> dict[str, Any]:
    """Legacy `(citing, arxiv_doi)` pairs versus the full-mode arXiv `all`
    projection on the same input; consistent when the projection contains
    every legacy pair."""
    legacy = _legacy_pairs(legacy_valid)
    full = _projection_pairs(projection_files)
    legacy_only = sorted(legacy - full)
    full_only = sorted(full - legacy)
    return {
        "legacy_pairs": len(legacy),
        "full_pairs": len(full),
        "shared": len(legacy & full),
        "legacy_only": len(legacy_only),
        "full_only": len(full_only),
        "consistent": not legacy_only,
        "legacy_only_examples": [list(pair) for pair in legacy_only[:EXAMPLE_PAIRS]],
        "full_only_examples": [list(pair) for pair in full_only[:EXAMPLE_PAIRS]],
        "legacy_works": len({target for _citer, target in legacy}),
        "full_works": len({target for _citer, target in full}),
    }


# --- dependencies ---


@dataclass(frozen=True)
class BenchmarkDependencies:
    """`FullDependencies` plus the legacy path, pricing and the memory probes
    (`memory_probe` reads the cgroup peak for information; `resident_probe`
    reads the non-reclaimable footprint the watermark thread samples).
    `ensure_legacy_fst` is the legacy pipeline's verified FST cache, used
    read-only. The publication callables are present only so the
    full-pipeline phases duck-type; the benchmark never calls them."""

    events: Any
    api_factory: Callable[..., Any]
    store_factory: Callable[..., Any]
    resolve_mirror: Callable[..., Any]
    fallback_transfer: Callable[..., Any]
    build_extractor: Callable[..., Any]
    discover_release: Callable[..., Any]
    ensure_bundle: Callable[..., Any]
    run_extraction: Callable[..., Any]
    transform: Callable[..., Any]
    build_legacy_index: Callable[..., Any]
    run_legacy_extraction: Callable[..., Any]
    list_hardware: Callable[[], Iterable[Any]]
    memory_probe: Callable[[], MemoryReading]
    prepare_dataset: Callable[..., Any]
    publish_pair: Callable[..., Any]
    publish_prepared: Callable[..., Any]
    verify_current: Callable[..., Any]
    resident_probe: Callable[[], MemoryReading] = read_cgroup_resident
    ensure_legacy_fst: Callable[..., Any] = ensure_fst
    stride: int = DEFAULT_STRIDE
    window: int = DEFAULT_WINDOW
    synthetic_degree: int = SYNTHETIC_DEGREE
    benchmark_flavor: str = "cpu-xl"
    smoke_seconds: float | None = None


def _default_list_hardware() -> list[Any]:
    from huggingface_hub import HfApi

    return list(HfApi().list_jobs_hardware())


def default_benchmark_dependencies() -> BenchmarkDependencies:
    full = default_full_dependencies()
    smoke_seconds = os.environ.get("BENCHMARK_SMOKE_SECONDS")
    return BenchmarkDependencies(
        events=full.events,
        api_factory=full.api_factory,
        store_factory=full.store_factory,
        resolve_mirror=full.resolve_mirror,
        fallback_transfer=full.fallback_transfer,
        build_extractor=full.build_extractor,
        discover_release=full.discover_release,
        ensure_bundle=full.ensure_bundle,
        run_extraction=full.run_extraction,
        transform=full.transform,
        build_legacy_index=run_build_index,
        run_legacy_extraction=run_extraction,
        list_hardware=_default_list_hardware,
        memory_probe=read_cgroup_peak,
        prepare_dataset=full.prepare_dataset,
        publish_pair=full.publish_pair,
        publish_prepared=full.publish_prepared,
        verify_current=full.verify_current,
        benchmark_flavor=os.environ.get("BENCHMARK_FLAVOR", "cpu-xl"),
        smoke_seconds=float(smoke_seconds) if smoke_seconds else None,
    )


# --- orchestration ---


@dataclass(frozen=True)
class ExtractionRun:
    label: str
    identity: RunIdentity
    outputs: FullExtractionOutputs
    report: ExtractionReport
    child: ChildRun | None


def check_benchmark_config(config: PipelineConfig) -> None:
    if config.mode != BENCHMARK_MODE:
        raise PipelineError(
            f"run_benchmark handles mode {BENCHMARK_MODE!r}, not {config.mode!r}"
        )
    if config.bucket_root is None:
        raise PipelineError("the benchmark requires --bucket-root (the mounted bucket)")
    if not config.hf_token:
        raise PipelineError("the benchmark requires the HF_TOKEN secret")
    try:
        config.resources.validate()
    except ValueError as error:
        raise PipelineError(f"invalid resource settings: {error}") from error
    resources = config.resources
    if resources.aggregation_memory_mb < 2 * resources.writer_memory_mb:
        raise PipelineError(
            "aggregation_memory_mb must be at least twice writer_memory_mb: segment "
            "estimates include an abandoned dictionary page, so equal budgets cannot "
            f"take the external path ({resources.aggregation_memory_mb} < "
            f"{2 * resources.writer_memory_mb})"
        )


def _cargo_target_dir(config: PipelineConfig) -> Path:
    override = os.environ.get("CARGO_TARGET_DIR")
    return Path(override) if override else config.work_root / "cargo-target"


def _physical_memory_bytes() -> int | None:
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None


def _extract(
    config: PipelineConfig,
    deps: BenchmarkDependencies,
    binary: ExtractorBinary,
    runner: MeasuredRunner,
    *,
    label: str,
    archive: Path,
    release: DataciteRelease,
    bundle: BundleResult,
    run_dir: Path,
) -> ExtractionRun:
    identity = build_run_identity(
        config, archive_sha256=sha256_file(archive), release=release, bundle=bundle
    )
    identity_path = run_dir / "run-identity.json"
    write_atomic_json(identity_path, identity.to_dict())
    runs_before = len(runner.runs)
    outputs = deps.run_extraction(
        binary,
        archive=archive,
        bundle=bundle.directory,
        identity_path=identity_path,
        output_dir=run_dir / "outputs",
        temp_dir=run_dir / "temp",
        resources=config.resources,
        events=deps.events,
        runner=runner,
    )
    child = runner.runs[-1] if len(runner.runs) > runs_before else None
    report = load_extraction_report(
        outputs.report, identity_sha256=identity.fingerprint()
    )
    deps.events.emit(
        "extraction_completed",
        label=label,
        files_processed=report.files_processed,
        stats=report.stats,
        aggregation=report.aggregation,
    )
    return ExtractionRun(
        label=label, identity=identity, outputs=outputs, report=report, child=child
    )


def _raw_output_bytes(outputs: FullExtractionOutputs) -> int:
    return sum(getattr(outputs, kind).stat().st_size for kind in FULL_OUTPUT_KINDS)


def _rate(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


def _run_summary(
    run: ExtractionRun, measured: dict[str, Any], members: int
) -> dict[str, Any]:
    stats = run.report.stats
    elapsed = measured["elapsed_seconds"]
    references = stats["references"]
    raw_bytes = _raw_output_bytes(run.outputs)
    return {
        "files_processed": run.report.files_processed,
        "member_count": members,
        "elapsed_seconds": elapsed,
        "seconds_per_member": elapsed / max(members, 1),
        **timing_summary(elapsed, members, run.child),
        "child_peak_rss_bytes": run.child.peak_rss_bytes if run.child else None,
        "confirmed_matches_per_member": stats["confirmed_matches"] / max(members, 1),
        "raw_bytes_per_member": raw_bytes / max(members, 1),
        "references_per_second": _rate(references, elapsed),
        "members_per_second": _rate(members, elapsed),
        "match_rate": _rate(stats["confirmed_matches"], references),
        "ambiguity_rate": _rate(stats["ambiguous_urls"], references),
        "arxiv_rate": _rate(stats["arxiv_matches"], references),
        "unmatched_rate": _rate(
            stats["unmatched_dois"] + stats["unmatched_urls"], references
        ),
        "malformed_rate": _rate(
            stats["malformed_references"] + stats["malformed_fields"], references
        ),
        "stats": dict(stats),
        "aggregation": dict(run.report.aggregation),
        "raw_output_bytes": raw_bytes,
        "identity_sha256": run.identity.fingerprint(),
        "children_maxrss_after_bytes": measured["memory"][
            "children_maxrss_after_bytes"
        ],
        "cgroup_peak_after_bytes": measured["memory"]["cgroup_peak_after_bytes"],
        "temp_peak_bytes": measured["disk"]["watched_tree_peak_bytes"],
        "disk_growth_bytes": measured["disk"]["phase_growth_bytes"],
    }


def _copy_transform_files(transforms: dict[str, DatasetTransform], local: Path) -> None:
    for kind in PUBLICATION_KINDS:
        for name, path in transforms[kind].files.items():
            target = local / kind / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)


def _zero_counts(ambiguous_urls: int) -> dict[str, Any]:
    empty = {"works": 0, "citations": 0, "references": 0}
    counts: dict[str, Any] = {
        kind: {name: dict(empty) for name in CONFIG_NAMES} for kind in PUBLICATION_KINDS
    }
    counts["ambiguous_urls"] = ambiguous_urls
    return counts


def _run_stress(
    config: PipelineConfig,
    deps: BenchmarkDependencies,
    binary: ExtractorBinary,
    meter: PhaseMeter,
    runner: MeasuredRunner,
    case: SyntheticCase,
    root: Path,
) -> dict[str, Any]:
    """Extract and transform one synthetic archive with its own locally
    verified bundle; reported under `synthetic` only.

    The shared-URL case confirms nothing by design, so its transform has no
    citations to publish: that is recorded as a zero-citation transform,
    with the extraction's measurements and ambiguity counts intact."""
    events = deps.events
    work = root / case.name
    release = discover_release(case.release_root.parent, None)
    paths = PipelinePaths.from_roots(
        work / "bucket", work / "work", prefix_name=DATACITE_PREFIX_NAME
    )

    def build_index(records: Path, output: Path) -> None:
        run_build_datacite_index(
            binary, records, output, config.resources.index_sort_memory_mb
        )

    bundle = deps.ensure_bundle(
        paths=paths,
        release=release,
        build_index=build_index,
        store=None,
        events=events,
        force=True,
    )
    with tarfile.open(case.archive, "r:gz") as archive:
        members = sum(1 for entry in archive if _is_member(entry))
    run_dir = work / "run"
    with meter.phase(
        f"synthetic_{case.name}_extract", watch=run_dir / "temp"
    ) as measured:
        run = _extract(
            config,
            deps,
            binary,
            runner,
            label=f"synthetic_{case.name}",
            archive=case.archive,
            release=release,
            bundle=bundle,
            run_dir=run_dir,
        )
    extraction = _run_summary(run, measured, members)
    transforms: dict[str, DatasetTransform] | None
    with meter.phase(f"synthetic_{case.name}_transform") as measured:
        try:
            transforms = deps.transform(
                run.outputs.valid,
                run.outputs.ambiguous_urls,
                work / "transform",
                events,
                config.resources,
            )
        except NoConfirmedCitationsError:
            transforms = None
    if transforms is None:
        counts = _zero_counts(run.report.stats["ambiguous_urls"])
        derived = 0
        events.emit(
            "synthetic_transform_empty",
            case=case.name,
            ambiguous_urls=counts["ambiguous_urls"],
        )
    else:
        counts = counts_from_transforms(transforms)
        derived = sum(sum(t.sizes.values()) for t in transforms.values())
    return {
        "degree": case.degree,
        "target": case.target,
        "url": case.url,
        "archive_bytes": case.archive.stat().st_size,
        "members": members,
        "extraction": extraction,
        "transform": {
            "elapsed_seconds": measured["elapsed_seconds"],
            "self_maxrss_after_bytes": measured["memory"]["self_maxrss_after_bytes"],
            "cgroup_peak_after_bytes": measured["memory"]["cgroup_peak_after_bytes"],
            "disk_growth_bytes": measured["disk"]["phase_growth_bytes"],
            "no_confirmed_citations": transforms is None,
            "confirmed_citations": counts["datacite"]["all"]["references"],
            "counts": counts,
        },
        "output_bytes": {"raw": extraction["raw_output_bytes"], "derived": derived},
    }


def run_benchmark(
    config: PipelineConfig, deps: BenchmarkDependencies | None = None
) -> dict[str, Any]:
    """Measure the representative workload against the real bucket and
    write `benchmark-report.json` (always) and `production-settings.json`
    (only when a configuration is ready, the sample is representative and
    the semantic comparison holds) under `work_root/benchmarks/<run-id>/`
    and `<prefix>/benchmarks/<run-id>/`. Readiness being false is a valid
    result; measurement failures raise."""
    deps = deps or default_benchmark_dependencies()
    events = deps.events
    check_benchmark_config(config)
    verify_source(config, events)
    paths = full_paths(config)
    root = config.work_root / BENCHMARKS_DIR / config.run_id
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    remote = paths.prefix / BENCHMARKS_DIR / config.run_id
    started_at = utc_now()
    clock = time.perf_counter()
    api = deps.api_factory(config.hf_token)
    store = deps.store_factory(
        api=api, token=config.hf_token, mount_root=config.bucket_root
    )
    # Everything the job writes lives under the work root: the staged
    # archive, the bundle copy, the Rust build, and every run's partitions,
    # outputs and transform scratch.
    disk = ResourceWatermark(
        config.work_root,
        owned_roots=(config.work_root,),
        memory_sampler=deps.resident_probe,
    )
    disk.start()
    runner = MeasuredRunner()
    meter = PhaseMeter(
        events,
        memory_probe=deps.memory_probe,
        watermark=disk,
        work_root=config.work_root,
        runner=runner,
    )

    def progress(payload: dict[str, object]) -> None:
        events.emit("progress", **payload)

    try:
        binary = deps.build_extractor(
            config.source_root, _cargo_target_dir(config), events
        )
        resolved = resolve_snapshot(config, deps, paths, store)
        release = select_release(config, deps)
        with meter.phase("prepare_index"):
            bundle = prepare_bundle(config, deps, paths, binary, release, store)
        identity = build_run_identity(
            config,
            archive_sha256=resolved.archive_sha256,
            release=release,
            bundle=bundle,
        )
        events.emit(
            "run_identity", identity_sha256=identity.fingerprint(), **identity.to_dict()
        )
        bundle_bytes = tree_bytes(bundle.directory)
        fixed_work_bytes = tree_bytes(config.work_root)

        with meter.phase("source_transfer"):
            archive = stage_archive(paths, resolved, events)
        sample_archive = root / "sample" / ARCHIVE_NAME
        with meter.phase("sample_archive"):
            sampling = sample_crossref_archive(
                archive,
                sample_archive,
                stride=deps.stride,
                window=deps.window,
                windows_dir=root / "windows",
            )
        events.emit(
            "archive_sampled",
            total_members=sampling["total_members"],
            selected_members=sampling["selected_members"],
            representative=sampling["representative"],
            windows=len(sampling["windows"]),
        )
        release_archive(archive, config, events)

        windows: list[dict[str, Any]] = []
        for record in sampling["windows"]:
            label = f"extract_window_{record['index']:02d}"
            with meter.phase(label) as measured:
                run = _extract(
                    config,
                    deps,
                    binary,
                    runner,
                    label=label,
                    archive=Path(record["archive"]),
                    release=release,
                    bundle=bundle,
                    run_dir=root / "windows" / f"window-{record['index']:02d}" / "run",
                )
            windows.append(
                {**record, **_run_summary(run, measured, len(record["members"]))}
            )
        sample_dir = root / "sample" / "run"
        with meter.phase("extract_sample", watch=sample_dir / "temp") as measured:
            sample = _extract(
                config,
                deps,
                binary,
                runner,
                label="sample",
                archive=sample_archive,
                release=release,
                bundle=bundle,
                run_dir=sample_dir,
            )
        sample_summary = _run_summary(sample, measured, sampling["selected_members"])

        # The legacy pipeline caches its verified arXiv FST under its own
        # prefix; reuse it read-only (no store) and build only when no cached
        # FST verifies for this release.
        legacy_paths = PipelinePaths.from_roots(config.bucket_root, root / "legacy")
        with meter.phase("prepare_legacy_fst") as fst_measured:
            legacy_fst = deps.ensure_legacy_fst(
                paths=legacy_paths,
                release=LegacyRelease(
                    release=release.release, dois_path=release.root / "dois"
                ),
                build_index=lambda records, output: deps.build_legacy_index(
                    binary, records, output, runner=runner
                ),
                extractor_version=binary.version,
                store=None,
                events=events,
                force=False,
            )
        fst = legacy_fst.local_path
        fst_source = "cache" if legacy_fst.reused else "built"
        events.emit(
            "legacy_fst_prepared",
            source=fst_source,
            release=legacy_fst.release,
            sha256=legacy_fst.sha256,
            size=legacy_fst.size,
        )
        with meter.phase("extract_legacy") as legacy_measured:
            legacy = deps.run_legacy_extraction(
                binary,
                archive=sample_archive,
                fst=fst,
                output_dir=root / "legacy" / "outputs",
                temp_dir=root / "legacy" / "temp",
                events=events,
                runner=runner,
            )
        with meter.phase(
            "transform_sample", watch=root / "sample" / "transform"
        ) as transform_measured:
            transforms = deps.transform(
                sample.outputs.valid,
                sample.outputs.ambiguous_urls,
                root / "sample" / "transform",
                events,
                config.resources,
            )
        arxiv = transforms["arxiv"]
        semantic = compare_arxiv_projection(
            legacy.valid, [arxiv.files[name] for name in arxiv.config_files["all"]]
        )
        events.emit("semantic_comparison", **semantic)

        outputs_local = root / "sample-outputs"
        with meter.phase("compress_outputs") as compress_measured:
            for kind in FULL_OUTPUT_KINDS:
                compress_file(
                    getattr(sample.outputs, kind),
                    outputs_local / "outputs" / f"{kind}.jsonl.zst",
                )
        compressed_bytes = tree_bytes(outputs_local / "outputs")
        _copy_transform_files(transforms, outputs_local)
        persisted_bytes = tree_bytes(outputs_local)
        with meter.phase("persist_sample_outputs") as persist_measured:
            store.put_tree(outputs_local, remote / "sample-outputs", progress=progress)

        cases = create_synthetic_stress(
            root / "synthetic", degree=deps.synthetic_degree
        )
        synthetic = {
            name: _run_stress(
                config, deps, binary, meter, runner, case, root / "synthetic"
            )
            for name, case in cases.items()
        }

        phases = meter.phases
        workers = max((os.cpu_count() or 1) - 2, 1)
        derived_bytes = sum(sum(t.sizes.values()) for t in transforms.values())
        cgroup_peaks = [
            p["memory"]["cgroup_peak_after_bytes"]
            for p in phases.values()
            if p["memory"]["cgroup_peak_after_bytes"] is not None
        ]
        production_residents = [
            p["memory"]["cgroup_resident_peak_bytes"]
            for name, p in phases.items()
            if is_production_phase(name)
            and p["memory"]["cgroup_resident_peak_bytes"] is not None
        ]
        runs = [*windows, sample_summary]
        extraction_peaks = [
            run["child_peak_rss_bytes"] for run in runs if run["child_peak_rss_bytes"]
        ]
        index_loads = [
            run["index_load_seconds"]
            for run in runs
            if run["index_load_seconds"] is not None
        ]
        aggregations = [
            run["aggregation_seconds"]
            for run in runs
            if run["aggregation_seconds"] is not None
        ]
        legacy_peaks = [
            peak
            for measured in (fst_measured, legacy_measured)
            if (peak := measured["memory"]["child_peak_rss_bytes"]) is not None
        ]
        numbers = {
            "total_members": sampling["total_members"],
            "sampled_members": sampling["selected_members"],
            "window_seconds_per_member": [w["seconds_per_member"] for w in windows],
            "window_variable_seconds_per_member": [
                w["variable_seconds_per_member"] for w in windows
            ],
            "index_load_seconds": max(index_loads, default=0.0),
            "aggregation_seconds_max": max(aggregations, default=0.0),
            "sample_aggregation_seconds": sample_summary["aggregation_seconds"] or 0.0,
            "window_confirmed_matches_per_member": [
                w["confirmed_matches_per_member"] for w in windows
            ],
            "window_raw_bytes_per_member": [w["raw_bytes_per_member"] for w in windows],
            "sample_confirmed_matches": sample.report.stats["confirmed_matches"],
            "sample_ambiguous_urls": sample.report.stats["ambiguous_urls"],
            "source_transfer_seconds": phases["source_transfer"]["elapsed_seconds"],
            "index_preparation_seconds": phases["prepare_index"]["elapsed_seconds"],
            "transform_seconds": transform_measured["elapsed_seconds"],
            "compression_seconds": compress_measured["elapsed_seconds"],
            "persistence_seconds": persist_measured["elapsed_seconds"],
            "persisted_bytes": persisted_bytes,
            "archive_bytes": resolved.archive_size,
            "bundle_bytes": bundle_bytes,
            "fixed_work_bytes": fixed_work_bytes,
            "segment_bytes": max(
                sample_summary["temp_peak_bytes"] or 0,
                sample_summary["disk_growth_bytes"],
            ),
            "raw_output_bytes": sample_summary["raw_output_bytes"],
            "transform_scratch_bytes": max(
                transform_measured["disk"]["watched_tree_peak_bytes"] or 0,
                transform_measured["disk"]["phase_growth_bytes"],
            ),
            "derived_bytes": derived_bytes,
            "compressed_output_bytes": compressed_bytes,
            "observed_peak_owned_bytes": disk.peak_owned,
            "extraction_child_peak_bytes": max(extraction_peaks, default=0),
            "legacy_child_peak_bytes": max(legacy_peaks, default=None),
            "transform_self_peak_bytes": transform_measured["memory"][
                "self_maxrss_after_bytes"
            ],
            "extraction_budget_bytes": extraction_budget_bytes(
                config.resources, bundle_bytes, workers
            ),
            "transform_budget_bytes": transform_budget_bytes(config.resources),
            "production_resident_peak_bytes": max(production_residents, default=None),
        }
        extrapolation = extrapolate(numbers)
        pricing = fetch_pricing(deps.list_hardware)
        elapsed = time.perf_counter() - clock

        def window_sample(index: int | None) -> dict[str, Any] | None:
            if index is None:
                return None
            record = windows[index]
            return {
                "window": index,
                "members": record["members"],
                "doi_density": record["doi_density"],
                "url_density": record["url_density"],
                "elapsed_seconds": record["elapsed_seconds"],
                "seconds_per_member": record["seconds_per_member"],
                "references_per_second": record["references_per_second"],
                "match_rate": record["match_rate"],
                "ambiguity_rate": record["ambiguity_rate"],
                "stats": record["stats"],
            }

        report: dict[str, Any] = {
            "kind": REPORT_KIND,
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": config.run_id,
            "started_at": started_at,
            "measured_on": deps.benchmark_flavor,
            "identities": {
                "source_tree_sha256": config.source_digest,
                "snapshot_id": resolved.snapshot_id,
                "snapshot_provenance": resolved.provenance,
                "archive_sha256": resolved.archive_sha256,
                "archive_size": resolved.archive_size,
                "datacite_release": release.release,
                "source_inventory_sha256": release.inventory_sha256,
                "bundle_manifest_sha256": bundle.manifest_sha256,
                "bundle_reused": bundle.reused,
                "identity_sha256": identity.fingerprint(),
                "extractor_version": binary.version,
                "sample_sha256": sampling["sample_sha256"],
            },
            "resources": asdict(config.resources),
            "environment": {
                "platform": sys.platform,
                "cpu_count": os.cpu_count(),
                "extraction_workers": workers,
                "memory_total_bytes": _physical_memory_bytes(),
                "disk_total_bytes": shutil.disk_usage(config.work_root).total,
                "disk_baseline_owned_bytes": disk.baseline_owned,
            },
            "sampling": sampling,
            "samples": {
                "representative": {
                    "archive": str(sample_archive),
                    "members": sampling["selected_members"],
                    "windows": len(windows),
                    "representative": sampling["representative"],
                },
                "doi_heavy": window_sample(sampling["doi_heavy_window"]),
                "url_heavy": window_sample(sampling["url_heavy_window"]),
            },
            "extraction": {"sample": sample_summary, "windows": windows},
            "legacy": {
                "fst_source": fst_source,
                "fst_reused": legacy_fst.reused,
                "fst_prepare_seconds": fst_measured["elapsed_seconds"],
                "fst_sha256": legacy_fst.sha256,
                "fst_bytes": legacy_fst.size,
                "extraction_seconds": legacy_measured["elapsed_seconds"],
                "pairs": semantic["legacy_pairs"],
                "works": semantic["legacy_works"],
                "child_peak_rss_bytes": numbers["legacy_child_peak_bytes"],
                "note": "reported only; never feeds readiness",
            },
            "semantic": semantic,
            "transform": {
                "elapsed_seconds": transform_measured["elapsed_seconds"],
                "counts": counts_from_transforms(transforms),
                "sizes": {
                    kind: dict(transforms[kind].sizes) for kind in PUBLICATION_KINDS
                },
                "derived_bytes": derived_bytes,
                "compressed_output_bytes": compressed_bytes,
                "persisted_bytes": persisted_bytes,
                "self_maxrss_after_bytes": transform_measured["memory"][
                    "self_maxrss_after_bytes"
                ],
            },
            "synthetic": synthetic,
            "phases": phases,
            "measurement": {
                "memory_source": meter.memory_source,
                "cgroup_resident_peak_bytes": disk.memory_peak,
                "production_resident_peak_bytes": numbers[
                    "production_resident_peak_bytes"
                ],
                "resident_note": (
                    "cgroup_resident_peak_bytes is the run-wide high-water mark, "
                    "including the Rust build and the legacy index build, and is "
                    "informational; readiness uses the phases production runs and "
                    "each extractor child's own peak"
                ),
                "memory_peak_source": (
                    meter.phases["extract_sample"]["memory"]["cgroup_source"]
                ),
                "cgroup_peak_bytes": max(cgroup_peaks) if cgroup_peaks else None,
                "cgroup_peak_note": (
                    "memory.peak counts file-backed page cache and is reported for "
                    "information only; peak_memory_bytes uses the non-reclaimable "
                    "high-water mark and the child peaks"
                ),
                "rusage_unit": "bytes"
                if sys.platform.startswith("darwin")
                else "kibibytes",
                "disk_source": "owned-tree-sizes",
                "owned_roots": [str(root_path) for root_path in disk.owned_roots],
                "sample_interval_seconds": disk.interval,
                "samples": disk.samples,
                "disk_peak_owned_bytes": disk.peak_owned,
                "filesystem_used_peak_bytes": disk.filesystem_used_peak,
                "filesystem_note": (
                    "shutil.disk_usage reports the node's shared filesystem, other "
                    "tenants included; context only, never a readiness input"
                ),
            },
            "extrapolation": extrapolation,
            "components_seconds": extrapolation["components_seconds"],
            "peak_memory_bytes": extrapolation["peak_memory_bytes"],
            "projected_peak_disk_bytes": extrapolation["projected_peak_disk_bytes"],
            "projected_total_seconds": extrapolation["projected_total_seconds"],
            "pricing": pricing,
        }
        readiness = select_configuration(report)
        if not sampling["representative"]:
            withheld = "sample is not representative"
        elif not semantic["consistent"]:
            withheld = "legacy arXiv pairs missing from the full-mode projection"
        else:
            withheld = None
        readiness["withheld_reason"] = withheld
        selected = readiness["selected"]
        production_flavor = selected["flavor"] if selected else READINESS_LADDER[0][0]
        production_timeout = selected["timeout"] if selected else READINESS_LADDER[0][1]
        production_seconds = scale_total_seconds(report, production_flavor)
        hourly = pricing["hourly_usd"]
        costs = {
            "pricing_source": pricing["pricing_source"],
            "hourly_usd": hourly,
            "smoke": cost_estimate(
                seconds=deps.smoke_seconds
                if deps.smoke_seconds is not None
                else SMOKE_TIMEOUT_SECONDS,
                hourly_usd=hourly["cpu-basic"],
                flavor="cpu-basic",
            ),
            "smoke_basis": "measured"
            if deps.smoke_seconds is not None
            else "30m-timeout-cap",
            "benchmark": cost_estimate(
                seconds=elapsed,
                hourly_usd=hourly.get(deps.benchmark_flavor, hourly["cpu-xl"]),
                flavor=deps.benchmark_flavor,
            ),
            "production": cost_estimate(
                seconds=production_seconds,
                hourly_usd=hourly[production_flavor],
                flavor=production_flavor,
            ),
            "production_with_headroom": cost_estimate(
                seconds=production_seconds * TIMEOUT_HEADROOM,
                hourly_usd=hourly[production_flavor],
                flavor=production_flavor,
            ),
            "production_at_timeout": cost_estimate(
                seconds=TIMEOUT_SECONDS[production_timeout],
                hourly_usd=hourly[production_flavor],
                flavor=production_flavor,
            ),
        }
        report["readiness"] = readiness
        report["costs"] = costs
        report["completed_at"] = utc_now()
        report["elapsed_seconds"] = elapsed
    finally:
        disk.stop()

    report_path = root / REPORT_NAME
    with events.phase("write_benchmark_report"):
        write_atomic_json(report_path, report)
    settings_path: Path | None = None
    if selected is not None and withheld is None:
        settings_path = root / SETTINGS_NAME
        write_production_settings(
            settings_path,
            report_path=report_path,
            source_tree_sha256=config.source_digest,
            snapshot_id=resolved.snapshot_id,
            datacite_release=release.release,
            flavor=selected["flavor"],
            timeout=selected["timeout"],
            resources=config.resources,
        )
    with events.phase("persist_benchmark_report"):
        store.put_file(report_path, remote / REPORT_NAME)
        if settings_path is not None:
            store.put_file(settings_path, remote / SETTINGS_NAME)

    first = readiness["evaluations"][0]
    ready = settings_path is not None
    result = {
        "status": "benchmark-ready" if ready else "benchmark-not-ready",
        "ready": ready,
        "failed_limits": [] if ready else list(first["failed_limits"]),
        "withheld_reason": withheld,
        "selected": selected,
        "representative": sampling["representative"],
        "semantic_consistent": semantic["consistent"],
        "report_path": str(report_path),
        "settings_path": str(settings_path) if settings_path else None,
        "remote_prefix": f"{BENCHMARKS_DIR}/{config.run_id}",
        "projected": {
            "peak_memory_bytes": extrapolation["peak_memory_bytes"],
            "projected_peak_disk_bytes": extrapolation["projected_peak_disk_bytes"],
            "projected_total_seconds": extrapolation["projected_total_seconds"],
            "projected_total_seconds_point": extrapolation[
                "projected_total_seconds_point"
            ],
            "projected_confirmed_matches": extrapolation["projected_confirmed_matches"],
        },
        "costs": costs,
        "snapshot_id": resolved.snapshot_id,
        "datacite_release": release.release,
        "source_tree_sha256": config.source_digest,
        "identity_sha256": identity.fingerprint(),
        "bundle_manifest_sha256": bundle.manifest_sha256,
        "elapsed_seconds": elapsed,
    }
    events.emit(
        "benchmark_completed",
        status=result["status"],
        selected=selected,
        failed_limits=result["failed_limits"],
        withheld_reason=withheld,
        projected_total_seconds=extrapolation["projected_total_seconds"],
        peak_memory_bytes=extrapolation["peak_memory_bytes"],
        projected_peak_disk_bytes=extrapolation["projected_peak_disk_bytes"],
        remote_prefix=result["remote_prefix"],
    )
    return result
