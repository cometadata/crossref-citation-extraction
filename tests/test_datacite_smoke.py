from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
import tarfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from citations_pipeline.datacite_index import discover_release
from citations_pipeline.datacite_smoke import (
    EXPECTED_AMBIGUOUS_ROWS,
    EXPECTED_FAILED_ROWS,
    EXPECTED_VALID_ROWS,
    FILLER_BYTES,
    FILLER_LABELS,
    SMOKE_EXTERNAL_RESOURCES,
    SMOKE_REGISTRY,
    SMOKE_RELEASE,
    SMOKE_RESOURCES,
    check_aggregation_paths,
    compare_extraction_outputs,
    create_datacite_smoke,
    filler_text,
    run_datacite_smoke,
)
from citations_pipeline.extractor import ExtractionReport, full_output_paths
from citations_pipeline.paths import runtime_source_digest
from citations_pipeline.pipeline import PipelineConfig
from citations_pipeline.pipeline_io import PipelineError
from citations_pipeline.telemetry import EventLogger

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


def test_smoke_fixture_is_reproducible(tmp_path):
    left, right = tmp_path / "left", tmp_path / "right"
    create_datacite_smoke(left)
    create_datacite_smoke(right)
    def contents(root):
        return {p.relative_to(root).as_posix(): p.read_bytes()
                for p in root.rglob("*") if p.is_file()}
    assert contents(left) == contents(right)

def test_real_smoke_counts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    result = run_datacite_smoke(PipelineConfig(
        source_root=root, source_digest=runtime_source_digest(root),
        work_root=tmp_path, bucket_root=None, smoke=True, force=False,
        datacite_release=None, hf_token=None, crossref_token=None,
        run_id="test-datacite-smoke", mode="smoke",
    ))
    assert result["status"] == "smoke-completed"
    assert result["counts"] == SMOKE_COUNTS


# --- fixture shape -------------------------------------------------------------

W1, W2, W3 = "10.9999/w1", "10.9999/w2", "10.9999/w3"
SHARED = "https://example.org/shared"
MIB = 1024 * 1024
DOI_LIKE = re.compile(r"\b10\.\d")
URL_LIKE = re.compile(r"://|\b[a-z0-9-]+\.[a-z]{2,}/", re.IGNORECASE)
ARXIV_LIKE = re.compile(r"arxiv|\d{4}\.\d{4,5}|\b[a-z-]+/\d{7}\b", re.IGNORECASE)


def read_archive(archive: Path) -> tuple[list[tarfile.TarInfo], dict[str, Any]]:
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        payload = json.loads(tar.extractfile(members[0]).read())
    return members, payload


def read_gzip_lines(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [line.rstrip("\n") for line in handle if line.strip()]


def test_fixture_release_layout_matches_the_release_readers(tmp_path: Path) -> None:
    archive, release_root = create_datacite_smoke(tmp_path)
    assert archive == tmp_path / "crossref" / "all.json.tar.gz"
    assert release_root == (
        tmp_path / "datacite" / f"datacite-monthly-data-file-{SMOKE_RELEASE}"
    )
    assert (release_root / "_TRANSFER_COMPLETE.json").is_file()
    status = json.loads((release_root / "STATUS.json").read_text())
    assert status["month"] == SMOKE_RELEASE and status["status"] == "Complete"

    month_dir = release_root / "dois" / f"updated_{SMOKE_RELEASE}"
    part = month_dir / "part_0000.jsonl.gz"
    csv = month_dir / f"{SMOKE_RELEASE}.csv.gz"
    inputs = sorted(p for p in release_root.rglob("*") if p.is_file() and p.suffix == ".gz")
    assert inputs == [csv, part]
    for path in inputs:
        # gzip header: magic, method, flags, then a four-byte mtime of zero.
        assert path.read_bytes()[4:8] == b"\0\0\0\0"

    manifest = json.loads((release_root / "MANIFEST.json").read_text())
    assert {entry["filename"] for entry in manifest} == {
        f"dois/updated_{SMOKE_RELEASE}/part_0000.jsonl.gz",
        f"dois/updated_{SMOKE_RELEASE}/{SMOKE_RELEASE}.csv.gz",
    }
    for entry in manifest:
        data = (release_root / entry["filename"]).read_bytes()
        assert entry["size"] == len(data)
        assert entry["sha256"] == base64.b64encode(hashlib.sha256(data).digest()).decode()

    records = [json.loads(line) for line in read_gzip_lines(part)]
    with_pages = [(doi, url) for doi, url, _state in SMOKE_REGISTRY if url is not None]
    assert [(r["id"], r["attributes"]["url"]) for r in records] == with_pages
    assert "10.1234/retired" not in {r["id"] for r in records}

    rows = read_gzip_lines(csv)
    assert rows[0] == "doi,state,client_id,updated"
    states = {line.split(",")[0]: line.split(",")[1] for line in rows[1:]}
    assert states == {doi: state for doi, _url, state in SMOKE_REGISTRY}
    assert states["10.1234/retired"] == "registered"

    release = discover_release(release_root.parent)
    assert release.release == SMOKE_RELEASE and release.root == release_root


def test_fixture_archive_encodes_the_reference_table(tmp_path: Path) -> None:
    archive, _release_root = create_datacite_smoke(tmp_path)
    assert archive.read_bytes()[4:8] == b"\0\0\0\0"
    members, payload = read_archive(archive)
    assert [m.name for m in members] == ["all.json"]
    member = members[0]
    assert (member.mtime, member.uid, member.gid, member.mode) == (0, 0, 0, 0o644)
    assert member.isfile()

    works = {item["DOI"]: item["reference"] for item in payload["items"]}
    assert list(works) == [W1, W2, W3, "10.1234/a"]
    w1, w2, w3 = works[W1], works[W2], works[W3]
    assert [len(w1), len(w2), len(w3)] == [3, 3, 6]

    assert w1[0]["DOI"] == "10.1234/a" and w1[0]["doi-asserted-by"] == "publisher"
    assert w1[0]["URL"] == "https://example.org/a"
    assert "10.1234/a" in w1[0]["unstructured"]
    assert w1[1]["URL"] == "https://example.org/a" and "DOI" not in w1[1]
    assert w1[2]["unstructured"].startswith("arXiv:2403.03542v2")
    assert "DOI" not in w1[2] and "URL" not in w1[2]

    assert w2[0]["DOI"] == "10.48550/arXiv.2403.03542"
    assert w2[0]["doi-asserted-by"] == "crossref"
    assert w2[1]["URL"] == SHARED and "DOI" not in w2[1] and "unstructured" not in w2[1]
    assert w2[2]["DOI"] == "10.1234/b" and w2[2]["doi-asserted-by"] == "publisher"
    assert w2[2]["URL"] == SHARED

    assert w3[0]["DOI"] == "10.1234/retired" and "doi-asserted-by" not in w3[0]
    assert w3[1]["unstructured"].startswith("arXiv:2499.99999 ")
    assert w3[2]["DOI"] == "10.9999/not-in-registry"
    assert w3[3]["DOI"] is False
    assert w3[3]["unstructured"].startswith("https://example.org/a")
    assert w3[4]["URL"] == SHARED
    assert "10.1234/b" in w3[4]["unstructured"] and "10.1234/c" in w3[4]["unstructured"]
    assert isinstance(w3[5], str)

    self_citer = works["10.1234/a"]
    assert [ref["DOI"] for ref in self_citer] == ["10.1234/a"]


def test_fixture_filler_is_inert_and_sized_to_force_external_aggregation(
    tmp_path: Path,
) -> None:
    for label in FILLER_LABELS:
        text = filler_text(label)
        assert len(text.encode("utf-8")) == FILLER_BYTES
        assert text.isascii()
        for pattern in (DOI_LIKE, URL_LIKE, ARXIV_LIKE):
            assert pattern.search(text) is None, (label, pattern.pattern)
    assert len({filler_text(label) for label in FILLER_LABELS}) == len(FILLER_LABELS)

    archive, _release_root = create_datacite_smoke(tmp_path)
    _members, payload = read_archive(archive)
    references = {
        (item["DOI"], index): ref
        for item in payload["items"]
        for index, ref in enumerate(item["reference"])
    }
    sizes = {
        key: len(json.dumps(ref, ensure_ascii=False).encode("utf-8"))
        for key, ref in references.items()
    }
    budget = SMOKE_EXTERNAL_RESOURCES.aggregation_memory_mb * MIB
    assert max(sizes.values()) < 256 * 1024
    per_target: dict[str, int] = {}
    for target, citer, index, *_rest in EXPECTED_VALID_ROWS | EXPECTED_FAILED_ROWS:
        assert (citer, index) in references
        per_target[target] = per_target.get(target, 0) + sizes[(citer, index)]
    assert max(per_target.values()) < budget // 2
    # The single partition of the external run holds every valid and failed
    # row; its decoded bytes must exceed the aggregation budget.
    assert sum(per_target.values()) > budget


def test_smoke_resource_settings_are_bounded_and_valid() -> None:
    SMOKE_RESOURCES.validate()
    SMOKE_EXTERNAL_RESOURCES.validate()
    assert SMOKE_RESOURCES.partitions > 1
    assert SMOKE_RESOURCES.aggregation_memory_mb * MIB > 4 * MIB
    assert (
        SMOKE_EXTERNAL_RESOURCES.partitions,
        SMOKE_EXTERNAL_RESOURCES.writer_memory_mb,
        SMOKE_EXTERNAL_RESOURCES.aggregation_memory_mb,
    ) == (1, 1, 2)
    external = SMOKE_EXTERNAL_RESOURCES
    assert external.aggregation_memory_mb > external.writer_memory_mb
    for name in ("result_batch_mb", "queue_batches", "merge_fan_in", "sort_workers",
                 "lookup_memory_mb", "max_entry_mb", "index_sort_memory_mb",
                 "duckdb_memory_mb"):
        assert getattr(SMOKE_RESOURCES, name) == getattr(external, name)


# --- assertion helpers -----------------------------------------------------------


def report(aggregation: dict[str, int]) -> ExtractionReport:
    return ExtractionReport(
        identity_sha256="0" * 64, files_processed=1, stats={}, aggregation=aggregation
    )


def test_check_aggregation_paths_requires_the_proof_from_both_reports() -> None:
    in_memory = report({"partitions_processed": 3, "in_memory_partitions": 3,
                        "external_partitions": 0, "reused_partitions": 0})
    external = report({"partitions_processed": 1, "in_memory_partitions": 0,
                       "external_partitions": 1, "reused_partitions": 0})
    check_aggregation_paths(in_memory, external)
    with pytest.raises(PipelineError, match="in-memory"):
        check_aggregation_paths(
            report({"partitions_processed": 3, "in_memory_partitions": 2,
                    "external_partitions": 1, "reused_partitions": 0}),
            external,
        )
    with pytest.raises(PipelineError, match="external"):
        check_aggregation_paths(
            in_memory,
            report({"partitions_processed": 1, "in_memory_partitions": 1,
                    "external_partitions": 0, "reused_partitions": 0}),
        )
    with pytest.raises(PipelineError, match="reused"):
        check_aggregation_paths(
            in_memory,
            report({"partitions_processed": 1, "in_memory_partitions": 0,
                    "external_partitions": 1, "reused_partitions": 1}),
        )


def write_outputs(directory: Path, records: dict[str, list[dict[str, Any]]]):
    outputs = full_output_paths(directory, Path("all.json.tar.gz"))
    directory.mkdir(parents=True, exist_ok=True)
    for kind in ("valid", "failed", "publisher", "crossref", "mined", "ambiguous_urls"):
        rows = records.get(kind, [])
        getattr(outputs, kind).write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
        )
    outputs.report.write_text("{}")
    return outputs


def test_compare_extraction_outputs_ignores_record_order_only(tmp_path: Path) -> None:
    entry = {"doi": W1, "ref_index": 0, "provenance": "mined", "match_type": "doi",
             "raw_match": "10.1234/a", "reference": {"DOI": "10.1234/a"}}
    a = {"doi": "10.1234/a", "citation_count": 1, "cited_by": [entry]}
    b = {"doi": "10.1234/b", "citation_count": 1, "cited_by": [entry]}
    ambiguous = {"citing_doi": W2, "ref_index": 1, "url": SHARED, "raw_match": SHARED,
                 "candidate_dois": ["10.1234/b", "10.1234/c"], "reference": {"URL": SHARED}}
    left = write_outputs(tmp_path / "left", {"valid": [a, b], "ambiguous_urls": [ambiguous]})
    right = write_outputs(tmp_path / "right", {"valid": [b, a], "ambiguous_urls": [ambiguous]})
    compare_extraction_outputs(left, right)

    changed = dict(b, citation_count=2, cited_by=[entry, dict(entry, ref_index=3)])
    other = write_outputs(tmp_path / "other", {"valid": [a, changed], "ambiguous_urls": [ambiguous]})
    with pytest.raises(PipelineError, match=r"valid.*10\.1234/b"):
        compare_extraction_outputs(left, other)
    missing = write_outputs(tmp_path / "missing", {"valid": [a, b]})
    with pytest.raises(PipelineError, match="ambiguous_urls"):
        compare_extraction_outputs(left, missing)


def test_run_refuses_other_modes_before_touching_the_work_root(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    config = PipelineConfig(
        source_root=root, source_digest=runtime_source_digest(root),
        work_root=tmp_path / "work", bucket_root=None, smoke=True, force=False,
        datacite_release=None, hf_token=None, crossref_token=None,
        run_id="test-datacite-smoke", mode="datacite",
    )
    with pytest.raises(PipelineError, match="smoke"):
        run_datacite_smoke(config)
    assert not (tmp_path / "work").exists()


# --- the real run, once, inspected from several angles ------------------------------


@pytest.fixture(scope="module")
def smoke_run(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    work_root = tmp_path_factory.mktemp("datacite-smoke")
    lines: list[str] = []
    result = run_datacite_smoke(
        PipelineConfig(
            source_root=root, source_digest=runtime_source_digest(root),
            work_root=work_root, bucket_root=None, smoke=True, force=False,
            datacite_release=None, hf_token="smoke-secret-token-must-not-leak",
            crossref_token="crossref-secret-must-not-leak",
            run_id="test-datacite-smoke", mode="smoke",
        ),
        events=EventLogger(output=lines.append),
    )
    events = [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in lines]
    return {"result": result, "events": events, "lines": lines, "work_root": work_root}


def test_smoke_result_is_the_job_contract(smoke_run: dict[str, Any]) -> None:
    result = smoke_run["result"]
    root = Path(__file__).resolve().parents[1]
    assert result == {
        "status": "smoke-completed",
        "counts": SMOKE_COUNTS,
        "source_tree_sha256": runtime_source_digest(root),
        "identity_sha256": result["identity_sha256"],
        "datacite_release": SMOKE_RELEASE,
        "snapshot_id": "smoke",
    }
    assert re.fullmatch(r"[0-9a-f]{64}", result["identity_sha256"])
    serialized = json.dumps(result)
    assert "huggingface" not in serialized and "commit" not in serialized


def test_smoke_runs_every_real_phase_and_proves_both_aggregation_paths(
    smoke_run: dict[str, Any],
) -> None:
    events = smoke_run["events"]
    completed = [e["phase"] for e in events if e["event"] == "phase_completed"]
    for phase in ("verify_source", "create_smoke_fixtures", "build_extractor",
                  "select_release", "build_datacite_index", "extract_in_memory",
                  "extract_external", "flatten_valid_output", "verify_smoke_outputs"):
        assert phase in completed, phase
    assert completed.count("extract") == 2
    assert not [e for e in events if e["event"] == "phase_failed"]
    assert not [e for e in events if e["event"] in ("persist_bundle", "bundle_persisted",
                                                     "bundle_cache_reused")]
    proofs = [e for e in events if e["event"] == "smoke_aggregation_paths"]
    assert len(proofs) == 1
    in_memory, external = proofs[0]["in_memory"], proofs[0]["external"]
    assert in_memory["external_partitions"] == 0
    assert in_memory["in_memory_partitions"] == in_memory["partitions_processed"] >= 1
    assert (external["partitions_processed"], external["in_memory_partitions"],
            external["external_partitions"]) == (1, 0, 1)
    extractions = [e for e in events if e["event"] == "extraction_completed"]
    assert [e["label"] for e in extractions] == ["in_memory", "external"]
    assert extractions[0]["stats"] == extractions[1]["stats"]
    stats = extractions[0]["stats"]
    assert stats["items"] == 4 and stats["references"] == 13
    assert stats["filtered_self_citations"] == 1
    assert stats["malformed_references"] == 1 and stats["malformed_fields"] == 1
    assert stats["unmatched_dois"] == 1 and stats["invalid_arxiv"] == 1


def test_smoke_never_publishes_mounts_or_leaks_credentials(
    smoke_run: dict[str, Any],
) -> None:
    work_root: Path = smoke_run["work_root"]
    for line in smoke_run["lines"]:
        assert "must-not-leak" not in line
    assert "must-not-leak" not in json.dumps(smoke_run["result"])
    names = {p.name for p in work_root.rglob("*")}
    assert not names & {"_PIPELINE_COMPLETE.json", "_EXTRACTION_COMPLETE.json",
                        "_PUBLICATIONS.json"}
    # The placeholder bucket root handed to `PipelinePaths` is never created.
    assert not (work_root / "smoke" / "bucket").exists()
    assert not (work_root / "derived").exists()
    assert not (work_root / "state").exists()


def test_smoke_published_records_carry_the_expected_identities(
    smoke_run: dict[str, Any],
) -> None:
    work_root: Path = smoke_run["work_root"]
    transform = work_root / "smoke" / "transform"
    datacite, arxiv_dir = transform / "datacite" / "data", transform / "arxiv" / "data"
    general = {
        row["doi"]: row
        for row in pq.read_table(datacite / "all" / "part-00000.parquet").to_pylist()
    }
    assert set(general) == {"10.1234/a", "10.1234/b", "10.1234/c", "10.1234/retired",
                            "10.48550/arxiv.2403.03542"}
    a = general["10.1234/a"]
    assert (a["citation_count"], a["reference_count"]) == (2, 3)
    assert [c["doi"] for c in a["cited_by"]] == [W1, W3]
    evidence = [
        [(m["provenance"], m["match_type"]) for m in c["matches"]] for c in a["cited_by"]
    ]
    assert evidence == [[("publisher", "doi"), ("mined", "url")], [("mined", "url")]]
    b = general["10.1234/b"]
    assert [(c["doi"], c["provenance"]) for c in b["cited_by"]] == [
        (W2, "publisher"), (W3, "mined")]
    assert general["10.1234/c"]["cited_by"][0]["doi"] == W3
    retired = general["10.1234/retired"]
    assert retired["cited_by"][0]["matches"][0]["provenance"] == "mined"
    assert general["10.48550/arxiv.2403.03542"]["citation_count"] == 2
    for row in general.values():
        assert all(c["doi"] != "10.1234/a" for c in row["cited_by"])

    ambiguous = pq.read_table(
        datacite / "ambiguous_urls" / "part-00000.parquet"
    ).to_pylist()
    assert [
        (r["citing_doi"], r["ref_index"], r["url"], r["candidate_dois"]) for r in ambiguous
    ] == [
        (W2, 1, SHARED, ["10.1234/b", "10.1234/c"]),
        (W3, 4, SHARED, ["10.1234/b", "10.1234/c"]),
    ]
    assert len(ambiguous) == len(EXPECTED_AMBIGUOUS_ROWS)

    projection = pq.read_table(arxiv_dir / "all.parquet").to_pylist()
    assert [(r["arxiv_doi"], r["arxiv_id"], r["citation_count"]) for r in projection] == [
        ("10.48550/arXiv.2403.03542", "2403.03542", 2)]
    assert [(c["doi"], c["provenance"]) for c in projection[0]["cited_by"]] == [
        (W1, "mined"), (W2, "crossref")]
    asserted = pq.read_table(arxiv_dir / "asserted.parquet").to_pylist()
    assert [c["doi"] for c in asserted[0]["cited_by"]] == [W2]
