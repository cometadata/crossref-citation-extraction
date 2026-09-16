"""Deterministic full-chain smoke for the DataCite pipeline.

`create_datacite_smoke` writes a byte-reproducible Crossref archive and a
complete DataCite monthly release; `run_datacite_smoke` drives the real Rust
build, bundle build, extraction and both transforms over that fixture and
checks exact records, counts, and the equivalence of the in-memory and
external aggregation paths. It mounts no bucket, publishes nothing, and
never reads credentials; local and remote smoke share this entry point.

Registry (`SMOKE_REGISTRY`): `10.1234/a` (unique landing page), `10.1234/b`
and `10.1234/c` (shared landing page), the arXiv DOI `10.48550/arxiv.2403.03542`,
and `10.1234/retired` (registered-state CSV row only, no landing page).
Citing works `10.9999/w1..w3` exercise structured and textual DOIs, unique
and shared URLs, DOI-based disambiguation, arXiv forms, an invalid arXiv id,
an unregistered DOI, a boolean `DOI`, a bare-string reference, and a
self-citation from `10.1234/a`.

Eight matched references carry inert filler (`filler_text`) so that the
single partition of the second run holds more decoded bytes than its
aggregation budget while every row and every cited-work group stays well
under it: the first run must aggregate every partition in memory and the
second must take the external path, and their outputs must be logically
identical.

The second run keeps the minimum 1 MiB writer budget but a 2 MiB
aggregation budget. Equal budgets cannot work: the extractor's Parquet
writer abandons dictionary encoding once a large reference lands and the
abandoned dictionary page still counts toward the segment's uncompressed
size, so the aggregator's footer-based estimate of a segment the writer
filled to its budget exceeds an equal aggregation budget, and the external
path refuses segments estimated over its budget.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import shutil
import tarfile
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .datacite_index import (
    BundleResult,
    DataciteRelease,
    discover_release,
    ensure_bundle,
)
from .datacite_publish import DatasetTransform, transform_datacite_outputs
from .extractor import (
    FULL_OUTPUT_KINDS,
    ExtractionReport,
    ExtractorBinary,
    FullExtractionOutputs,
    build_extractor,
    load_extraction_report,
    run_build_datacite_index,
    run_datacite_extraction,
)
from .full_pipeline import build_run_identity, counts_from_transforms, verify_source
from .full_state import ResourceSettings, RunIdentity
from .paths import ARCHIVE_NAME, DATACITE_PREFIX_NAME, PipelinePaths
from .pipeline import PipelineConfig
from .pipeline_io import PipelineError
from .state import sha256_file, write_atomic_json
from .telemetry import EventLogger

SMOKE_MODE = "smoke"
SMOKE_RELEASE = "2026-07"
SMOKE_SNAPSHOT_ID = "smoke"
SMOKE_MEMBER_NAME = "all.json"
SMOKE_STATUS_DATETIME = "2026-08-01T00:00:00+00:00"

SMOKE_COUNTS = {
    "datacite": {
        "all": {"works": 5, "citations": 8, "references": 9},
        "asserted": {"works": 3, "citations": 3, "references": 3},
        "mined": {"works": 5, "citations": 6, "references": 6},
    },
    "arxiv": {
        "all": {"works": 1, "citations": 2, "references": 2},
        "asserted": {"works": 1, "citations": 1, "references": 1},
        "mined": {"works": 1, "citations": 1, "references": 1},
    },
    "ambiguous_urls": 2,
    "failed_arxiv_works": 1,
}

# Bounded settings for the primary run: small but generous budgets, several
# partitions, every partition aggregated in memory.
SMOKE_RESOURCES = ResourceSettings(
    partitions=4,
    writer_memory_mb=64,
    result_batch_mb=1,
    queue_batches=2,
    aggregation_memory_mb=64,
    merge_fan_in=2,
    sort_workers=1,
    lookup_memory_mb=1,
    max_entry_mb=8,
    index_sort_memory_mb=1,
    duckdb_memory_mb=256,
)
# The re-run: one partition, the minimum writer budget, and an aggregation
# budget the partition's decoded bytes exceed (see the module docstring for
# why it must be above the writer budget), so the external path runs.
SMOKE_EXTERNAL_RESOURCES = replace(
    SMOKE_RESOURCES, partitions=1, writer_memory_mb=1, aggregation_memory_mb=2
)

DOI_A = "10.1234/a"
DOI_B = "10.1234/b"
DOI_C = "10.1234/c"
DOI_ARXIV = "10.48550/arxiv.2403.03542"
DOI_RETIRED = "10.1234/retired"
URL_A = "https://example.org/a"
URL_SHARED = "https://example.org/shared"
URL_ARXIV = "https://arxiv.org/abs/2403.03542"
W1, W2, W3 = "10.9999/w1", "10.9999/w2", "10.9999/w3"

# (DOI, landing page, state); a landing page of None means the DOI appears
# only in the state report.
SMOKE_REGISTRY: tuple[tuple[str, str | None, str], ...] = (
    (DOI_A, URL_A, "findable"),
    (DOI_B, URL_SHARED, "findable"),
    (DOI_C, URL_SHARED, "findable"),
    (DOI_ARXIV, URL_ARXIV, "findable"),
    (DOI_RETIRED, None, "registered"),
)

# Filler: 250 000 bytes of plain words per labelled reference. Eight matched
# references (nine partition rows, since w3/4 cites two targets) carry it,
# so the single external-run partition decodes to about 2.5 MB (> 2 MiB)
# while each row stays near 250 KB (< 256 KiB) and each cited-work group
# at or under 500 KB (< 1 MiB, half the aggregation budget).
FILLER_BYTES = 250_000
FILLER_LABELS = (
    "w1 ref 0",
    "w1 ref 2",
    "w2 ref 0",
    "w2 ref 2",
    "w3 ref 0",
    "w3 ref 1",
    "w3 ref 3",
    "w3 ref 4",
)
_FILLER_PHRASE = "plain smoke filler words carrying no identifiers "


def filler_text(label: str) -> str:
    """Inert ASCII filler of exactly `FILLER_BYTES` bytes, distinct per label
    and free of DOI-, URL- and arXiv-like patterns."""
    head = f"smoke filler for {label}: "
    repeats = (FILLER_BYTES - len(head)) // len(_FILLER_PHRASE) + 1
    return (head + _FILLER_PHRASE * repeats)[:FILLER_BYTES]


# Expected extraction rows as (cited DOI, citing DOI, ref_index, provenance,
# match_type, raw_match). Self-citations and the unregistered DOI never
# appear; both arXiv forms resolve to the canonical arXiv DOI.
EXPECTED_VALID_ROWS: frozenset[tuple[str, str, int, str, str, str]] = frozenset(
    {
        (DOI_A, W1, 0, "publisher", "doi", DOI_A),
        (DOI_A, W1, 1, "mined", "url", URL_A),
        (DOI_A, W3, 3, "mined", "url", URL_A),
        (DOI_B, W2, 2, "publisher", "doi", DOI_B),
        (DOI_B, W3, 4, "mined", "doi", DOI_B),
        (DOI_C, W3, 4, "mined", "doi", DOI_C),
        (DOI_RETIRED, W3, 0, "mined", "doi", DOI_RETIRED),
        (DOI_ARXIV, W1, 2, "mined", "arxiv", "arXiv:2403.03542v2"),
        (DOI_ARXIV, W2, 0, "crossref", "doi", "10.48550/arXiv.2403.03542"),
    }
)
EXPECTED_FAILED_ROWS: frozenset[tuple[str, str, int, str, str, str]] = frozenset(
    {("10.48550/arxiv.2499.99999", W3, 1, "mined", "arxiv", "arXiv:2499.99999")}
)
# (citing DOI, ref_index, url, candidate DOIs): the shared landing page stays
# ambiguous without a disambiguating DOI (w2/1) and when both candidates are
# cited independently (w3/4).
EXPECTED_AMBIGUOUS_ROWS: tuple[tuple[str, int, str, tuple[str, ...]], ...] = (
    (W2, 1, URL_SHARED, (DOI_B, DOI_C)),
    (W3, 4, URL_SHARED, (DOI_B, DOI_C)),
)
# Published general records: cited DOI -> [(citer, [(provenance, match_type)])].
EXPECTED_PUBLISHED: dict[str, list[tuple[str, list[tuple[str, str]]]]] = {
    DOI_A: [(W1, [("publisher", "doi"), ("mined", "url")]), (W3, [("mined", "url")])],
    DOI_B: [(W2, [("publisher", "doi")]), (W3, [("mined", "doi")])],
    DOI_C: [(W3, [("mined", "doi")])],
    DOI_RETIRED: [(W3, [("mined", "doi")])],
    DOI_ARXIV: [(W1, [("mined", "arxiv")]), (W2, [("crossref", "doi")])],
}
EXPECTED_ASSERTED_WORKS = frozenset({DOI_A, DOI_B, DOI_ARXIV})
EXPECTED_MINED_WORKS = frozenset({DOI_A, DOI_B, DOI_C, DOI_RETIRED, DOI_ARXIV})
ARXIV_PUBLISHED_DOI = "10.48550/arXiv.2403.03542"
ARXIV_PUBLISHED_ID = "2403.03542"


# --- fixture ----------------------------------------------------------------------


def _smoke_works() -> list[dict[str, Any]]:
    return [
        {
            "DOI": W1,
            "reference": [
                {
                    "key": "w1-0",
                    "DOI": DOI_A,
                    "doi-asserted-by": "publisher",
                    "URL": URL_A,
                    "unstructured": f"Dataset A {DOI_A} " + filler_text("w1 ref 0"),
                },
                {"key": "w1-1", "URL": URL_A},
                {
                    "key": "w1-2",
                    "unstructured": "arXiv:2403.03542v2 " + filler_text("w1 ref 2"),
                },
            ],
        },
        {
            "DOI": W2,
            "reference": [
                {
                    "key": "w2-0",
                    "DOI": "10.48550/arXiv.2403.03542",
                    "doi-asserted-by": "crossref",
                    "unstructured": filler_text("w2 ref 0"),
                },
                {"key": "w2-1", "URL": URL_SHARED},
                {
                    "key": "w2-2",
                    "DOI": DOI_B,
                    "doi-asserted-by": "publisher",
                    "URL": URL_SHARED,
                    "unstructured": filler_text("w2 ref 2"),
                },
            ],
        },
        {
            "DOI": W3,
            "reference": [
                {
                    "key": "w3-0",
                    "DOI": DOI_RETIRED,
                    "unstructured": filler_text("w3 ref 0"),
                },
                {
                    "key": "w3-1",
                    "unstructured": "arXiv:2499.99999 " + filler_text("w3 ref 1"),
                },
                {"key": "w3-2", "DOI": "10.9999/not-in-registry"},
                {
                    "key": "w3-3",
                    "DOI": False,
                    "unstructured": f"{URL_A} " + filler_text("w3 ref 3"),
                },
                {
                    "key": "w3-4",
                    "URL": URL_SHARED,
                    "unstructured": f"Datasets {DOI_B} and {DOI_C} "
                    + filler_text("w3 ref 4"),
                },
                "not a reference object",
            ],
        },
        {
            "DOI": DOI_A,
            "reference": [{"key": "a-0", "DOI": DOI_A, "doi-asserted-by": "publisher"}],
        },
    ]


def _write_crossref_archive(path: Path) -> None:
    payload = (
        json.dumps({"items": _smoke_works()}, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as outer,
        tarfile.open(fileobj=outer, mode="w|") as archive,
    ):
        info = tarfile.TarInfo(SMOKE_MEMBER_NAME)
        info.size = len(payload)
        info.mtime = 0
        info.mode = 0o644
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        archive.addfile(info, io.BytesIO(payload))


def _registry_metadata() -> bytes:
    lines = [
        json.dumps(
            {
                "id": doi,
                "type": "dois",
                "attributes": {"doi": doi, "url": url, "state": state},
            },
            separators=(",", ":"),
        )
        for doi, url, state in SMOKE_REGISTRY
        if url is not None
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _registry_state_report() -> bytes:
    lines = ["doi,state,client_id,updated"]
    lines.extend(
        f"{doi},{state},example.smoke,{SMOKE_RELEASE}-15T00:00:00.000Z"
        for doi, _url, state in SMOKE_REGISTRY
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _write_datacite_release(root: Path) -> None:
    month_dir = f"dois/updated_{SMOKE_RELEASE}"
    files = (
        (
            f"{month_dir}/part_0000.jsonl.gz",
            gzip.compress(_registry_metadata(), mtime=0),
        ),
        (
            f"{month_dir}/{SMOKE_RELEASE}.csv.gz",
            gzip.compress(_registry_state_report(), mtime=0),
        ),
    )
    entries = []
    for relative, data in files:
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
                "month": SMOKE_RELEASE,
                "datetime": SMOKE_STATUS_DATETIME,
                "status": "Complete",
            },
            indent=2,
        )
        + "\n"
    )
    (root / "_TRANSFER_COMPLETE.json").write_text(
        json.dumps({"release": SMOKE_RELEASE, "source": "smoke fixture"}) + "\n"
    )


def create_datacite_smoke(root: Path) -> tuple[Path, Path]:
    """Write the smoke fixture under `root` and return the Crossref archive
    (`root/crossref/all.json.tar.gz`) and the complete DataCite release root
    (`root/datacite/datacite-monthly-data-file-2026-07`). Byte-identical on
    every call."""
    archive = root / "crossref" / ARCHIVE_NAME
    release_root = root / "datacite" / f"datacite-monthly-data-file-{SMOKE_RELEASE}"
    _write_crossref_archive(archive)
    _write_datacite_release(release_root)
    return archive, release_root


# --- checks -----------------------------------------------------------------------


def _jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as error:
                raise PipelineError(f"{path.name}:{number}: malformed JSON") from error
            if not isinstance(row, dict):
                raise PipelineError(f"{path.name}:{number}: row is not an object")
            rows.append(row)
    return rows


def _row_key(kind: str, row: dict[str, Any]) -> tuple[Any, ...]:
    if kind == "ambiguous_urls":
        return (row.get("citing_doi"), row.get("ref_index"), row.get("url"))
    return (row.get("doi"),)


def compare_extraction_outputs(
    left: FullExtractionOutputs, right: FullExtractionOutputs
) -> None:
    """Require the six JSONL outputs of two runs to be logically identical:
    the same rows (parsed, so grouping bytes do not matter) once record
    order, which depends on the partition count, is normalised."""
    for kind in FULL_OUTPUT_KINDS:
        key = partial(_row_key, kind)
        left_rows = sorted(_jsonl_rows(getattr(left, kind)), key=key)
        right_rows = sorted(_jsonl_rows(getattr(right, kind)), key=key)
        if left_rows == right_rows:
            continue
        left_keys = [_row_key(kind, row) for row in left_rows]
        right_keys = [_row_key(kind, row) for row in right_rows]
        if left_keys != right_keys:
            only_left = sorted(set(left_keys) - set(right_keys))
            only_right = sorted(set(right_keys) - set(left_keys))
            raise PipelineError(
                f"smoke {kind} output differs between the in-memory and external "
                f"aggregation runs: rows only in the in-memory run {only_left}, "
                f"only in the external run {only_right}"
            )
        differing = [
            key
            for key, a, b in zip(left_keys, left_rows, right_rows, strict=True)
            if a != b
        ]
        raise PipelineError(
            f"smoke {kind} output differs between the in-memory and external "
            f"aggregation runs for {differing}"
        )


def check_aggregation_paths(
    in_memory: ExtractionReport, external: ExtractionReport
) -> None:
    """Prove from the two reports that the primary run aggregated every
    partition in memory and the re-run took the external path, with no
    partition reused from a previous attempt in either."""
    first = in_memory.aggregation
    if (
        first.get("partitions_processed", 0) < 1
        or first.get("external_partitions") != 0
        or first.get("in_memory_partitions") != first.get("partitions_processed")
    ):
        raise PipelineError(
            "smoke in-memory run did not aggregate every partition in memory: "
            f"{first}"
        )
    second = external.aggregation
    if (
        second.get("partitions_processed") != 1
        or second.get("external_partitions") != 1
        or second.get("in_memory_partitions") != 0
    ):
        raise PipelineError(
            f"smoke external run did not take the external aggregation path: {second}"
        )
    for label, snapshot in (("in-memory", first), ("external", second)):
        if snapshot.get("reused_partitions") != 0:
            raise PipelineError(
                f"smoke {label} run reused partitions from a previous attempt: "
                f"{snapshot}"
            )


def _diff_counts(actual: Any, expected: Any, prefix: str = "") -> list[str]:
    if isinstance(expected, dict) and isinstance(actual, dict):
        differences: list[str] = []
        for key in sorted(set(expected) | set(actual)):
            path = f"{prefix}.{key}" if prefix else key
            if key not in actual:
                differences.append(f"{path} missing")
            elif key not in expected:
                differences.append(f"{path} unexpected")
            else:
                differences.extend(_diff_counts(actual[key], expected[key], path))
        return differences
    if actual != expected:
        return [f"{prefix}: got {actual!r}, expected {expected!r}"]
    return []


def smoke_counts(
    transforms: dict[str, DatasetTransform], failed: Path
) -> dict[str, Any]:
    counts = counts_from_transforms(transforms)
    counts["failed_arxiv_works"] = len(
        {entry["doi"] for row in _jsonl_rows(failed) for entry in row["cited_by"]}
    )
    return counts


def check_counts(counts: dict[str, Any]) -> None:
    differences = _diff_counts(counts, SMOKE_COUNTS)
    if differences:
        raise PipelineError("smoke counts mismatch: " + "; ".join(differences))


def _citation_tuples(path: Path) -> set[tuple[str, str, int, str, str, str]]:
    return {
        (
            row["doi"],
            entry["doi"],
            entry["ref_index"],
            entry["provenance"],
            entry["match_type"],
            entry["raw_match"],
        )
        for row in _jsonl_rows(path)
        for entry in row["cited_by"]
    }


def check_extraction_rows(outputs: FullExtractionOutputs) -> None:
    """The raw extractor outputs carry exactly the expected reference
    evidence, including `ref_index` and the stable raw match."""
    for label, path, expected in (
        ("valid", outputs.valid, EXPECTED_VALID_ROWS),
        ("failed", outputs.failed, EXPECTED_FAILED_ROWS),
    ):
        actual = _citation_tuples(path)
        if actual != expected:
            raise PipelineError(
                f"smoke {label} rows differ from the expected evidence: unexpected "
                f"{sorted(actual - expected)}, missing {sorted(expected - actual)}"
            )
    ambiguous = tuple(
        (row["citing_doi"], row["ref_index"], row["url"], tuple(row["candidate_dois"]))
        for row in _jsonl_rows(outputs.ambiguous_urls)
    )
    if ambiguous != EXPECTED_AMBIGUOUS_ROWS:
        raise PipelineError(
            f"smoke ambiguous URL rows {ambiguous} differ from the expected "
            f"{EXPECTED_AMBIGUOUS_ROWS}"
        )


def _config_rows(transform: DatasetTransform, config: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in transform.config_files[config]:
        rows.extend(pq.read_table(transform.files[name]).to_pylist())
    return rows


def _check_general_records(transform: DatasetTransform) -> None:
    records = {row["doi"]: row for row in _config_rows(transform, "all")}
    if set(records) != set(EXPECTED_PUBLISHED):
        raise PipelineError(
            f"smoke published works {sorted(records)} differ from the expected "
            f"{sorted(EXPECTED_PUBLISHED)}"
        )
    for doi, expected in EXPECTED_PUBLISHED.items():
        row = records[doi]
        actual = [
            (
                citer["doi"],
                [
                    (match["provenance"], match["match_type"])
                    for match in citer["matches"]
                ],
            )
            for citer in row["cited_by"]
        ]
        if actual != expected:
            raise PipelineError(
                f"smoke published record {doi}: citers {actual}, expected {expected}"
            )
        references = sum(len(citer[1]) for citer in expected)
        counts = (row["citation_count"], row["reference_count"])
        if counts != (len(expected), references):
            raise PipelineError(
                f"smoke published record {doi}: counts "
                f"{(row['citation_count'], row['reference_count'])}, expected "
                f"{(len(expected), references)}"
            )
    for config, expected_works in (
        ("asserted", EXPECTED_ASSERTED_WORKS),
        ("mined", EXPECTED_MINED_WORKS),
    ):
        works = {row["doi"] for row in _config_rows(transform, config)}
        if works != expected_works:
            raise PipelineError(
                f"smoke published {config} works {sorted(works)} differ from the "
                f"expected {sorted(expected_works)}"
            )
    for config in ("all", "asserted", "mined"):
        for row in _config_rows(transform, config):
            citers = [citer["doi"] for citer in row["cited_by"]]
            if DOI_A in citers or row["doi"] in citers:
                raise PipelineError(
                    f"smoke published {config} record {row['doi']} carries a "
                    f"self-citation or the self-citing work as a citer: {citers}"
                )


def _check_ambiguity_records(transform: DatasetTransform) -> None:
    rows = tuple(
        (row["citing_doi"], row["ref_index"], row["url"], tuple(row["candidate_dois"]))
        for row in _config_rows(transform, "ambiguous_urls")
    )
    if rows != EXPECTED_AMBIGUOUS_ROWS:
        raise PipelineError(
            f"smoke published ambiguous URLs {rows} differ from the expected "
            f"{EXPECTED_AMBIGUOUS_ROWS}"
        )


def _check_arxiv_records(transform: DatasetTransform) -> None:
    expected_citers = {
        "all": [(W1, "mined"), (W2, "crossref")],
        "asserted": [(W2, "crossref")],
        "mined": [(W1, "mined")],
    }
    for config, citers in expected_citers.items():
        rows = _config_rows(transform, config)
        summary = [
            (
                row["arxiv_doi"],
                row["arxiv_id"],
                row["citation_count"],
                [(citer["doi"], citer["provenance"]) for citer in row["cited_by"]],
            )
            for row in rows
        ]
        expected = [(ARXIV_PUBLISHED_DOI, ARXIV_PUBLISHED_ID, len(citers), citers)]
        if summary != expected:
            raise PipelineError(
                f"smoke arXiv {config} projection {summary} differs from the "
                f"expected {expected}"
            )


def check_published_records(transforms: dict[str, DatasetTransform]) -> None:
    """Read the prepared Parquet back and require the record identities the
    fixture was designed for: A has three references from two citers, the
    arXiv work two citations, B is disambiguated for w2, w3's independent
    DOI citations of b and c stand while its shared URL stays ambiguous,
    the retired DOI survives, and neither the self-citation nor the
    unregistered DOI appears anywhere."""
    _check_general_records(transforms["datacite"])
    _check_ambiguity_records(transforms["datacite"])
    _check_arxiv_records(transforms["arxiv"])


# --- runner -------------------------------------------------------------------------


@dataclass(frozen=True)
class SmokeExtraction:
    label: str
    identity: RunIdentity
    outputs: FullExtractionOutputs
    report: ExtractionReport


def _cargo_target_dir(config: PipelineConfig) -> Path:
    """Reuse a warm build when `CARGO_TARGET_DIR` names one (local runs);
    the job's read-only source volume builds into the work root."""
    override = os.environ.get("CARGO_TARGET_DIR")
    if override:
        return Path(override)
    return config.work_root / "cargo-target"


def _extract(
    config: PipelineConfig,
    binary: ExtractorBinary,
    events: Any,
    *,
    label: str,
    archive: Path,
    archive_sha256: str,
    release: DataciteRelease,
    bundle: BundleResult,
    run_dir: Path,
) -> SmokeExtraction:
    identity = build_run_identity(
        config, archive_sha256=archive_sha256, release=release, bundle=bundle
    )
    with events.phase(f"extract_{label}"):
        identity_path = run_dir / "run-identity.json"
        write_atomic_json(identity_path, identity.to_dict())
        outputs = run_datacite_extraction(
            binary,
            archive=archive,
            bundle=bundle.directory,
            identity_path=identity_path,
            output_dir=run_dir / "outputs",
            temp_dir=run_dir / "temp",
            resources=config.resources,
            events=events,
        )
        report = load_extraction_report(
            outputs.report, identity_sha256=identity.fingerprint()
        )
        events.emit(
            "extraction_completed",
            label=label,
            partitions=config.resources.partitions,
            files_processed=report.files_processed,
            stats=report.stats,
            aggregation=report.aggregation,
        )
    return SmokeExtraction(
        label=label, identity=identity, outputs=outputs, report=report
    )


def run_datacite_smoke(
    config: PipelineConfig, *, events: EventLogger | None = None
) -> dict[str, Any]:
    """Run the deterministic full-chain smoke under `config.work_root`.

    Builds the real extractor, the real index bundle (through `ensure_bundle`
    with no store, so the bundle is verified locally and never persisted),
    runs the real extraction twice (in-memory and forced-external
    aggregation), transforms the primary outputs with the real DataCite and
    arXiv transforms, and checks paths, equivalence, counts, and record
    identities. `config.resources` is replaced by the bounded smoke settings;
    `config.hf_token` is never read. Returns the job's result dict.
    """
    events = events if events is not None else EventLogger()
    if config.mode != SMOKE_MODE:
        raise PipelineError(
            f"run_datacite_smoke handles mode {SMOKE_MODE!r}, not {config.mode!r}"
        )
    verify_source(config, events)
    smoke_root = config.work_root / "smoke"
    if smoke_root.exists():
        shutil.rmtree(smoke_root)
    with events.phase("create_smoke_fixtures"):
        archive, release_root = create_datacite_smoke(smoke_root)
        archive_sha256 = sha256_file(archive)
    binary = build_extractor(config.source_root, _cargo_target_dir(config), events)
    with events.phase("select_release"):
        release = discover_release(release_root.parent, config.datacite_release)
        events.emit(
            "release_selected",
            release=release.release,
            source_inventory_sha256=release.inventory_sha256,
            pinned=config.datacite_release,
        )
    primary = replace(config, resources=SMOKE_RESOURCES)
    # `PipelinePaths` only supplies `bundle_local` here; with `store=None`
    # and `force=True` nothing under the placeholder bucket root is ever
    # read or created, so no bucket is mounted or touched.
    paths = PipelinePaths.from_roots(
        smoke_root / "bucket", config.work_root, prefix_name=DATACITE_PREFIX_NAME
    )

    def build_index(records: Path, output: Path) -> None:
        run_build_datacite_index(
            binary, records, output, SMOKE_RESOURCES.index_sort_memory_mb
        )

    bundle = ensure_bundle(
        paths=paths,
        release=release,
        build_index=build_index,
        store=None,
        events=events,
        force=True,
    )
    in_memory = _extract(
        primary,
        binary,
        events,
        label="in_memory",
        archive=archive,
        archive_sha256=archive_sha256,
        release=release,
        bundle=bundle,
        run_dir=smoke_root / "in-memory",
    )
    external = _extract(
        replace(config, resources=SMOKE_EXTERNAL_RESOURCES),
        binary,
        events,
        label="external",
        archive=archive,
        archive_sha256=archive_sha256,
        release=release,
        bundle=bundle,
        run_dir=smoke_root / "external",
    )
    check_aggregation_paths(in_memory.report, external.report)
    events.emit(
        "smoke_aggregation_paths",
        in_memory=in_memory.report.aggregation,
        external=external.report.aggregation,
    )
    if in_memory.report.stats != external.report.stats:
        raise PipelineError(
            "smoke extraction counters differ between the in-memory and external "
            f"runs: {in_memory.report.stats} != {external.report.stats}"
        )
    compare_extraction_outputs(in_memory.outputs, external.outputs)

    transforms = transform_datacite_outputs(
        in_memory.outputs.valid,
        in_memory.outputs.ambiguous_urls,
        smoke_root / "transform",
        events,
        SMOKE_RESOURCES,
    )
    counts = smoke_counts(transforms, in_memory.outputs.failed)
    with events.phase("verify_smoke_outputs"):
        check_counts(counts)
        check_extraction_rows(in_memory.outputs)
        check_published_records(transforms)
    events.emit(
        "smoke_completed",
        counts=counts,
        identity_sha256=in_memory.identity.fingerprint(),
        datacite_release=release.release,
    )
    return {
        "status": "smoke-completed",
        "counts": counts,
        "source_tree_sha256": config.source_digest,
        "identity_sha256": in_memory.identity.fingerprint(),
        "datacite_release": release.release,
        "snapshot_id": SMOKE_SNAPSHOT_ID,
    }
