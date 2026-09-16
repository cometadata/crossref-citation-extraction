import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import duckdb

from citations_pipeline import datacite_publish
from citations_pipeline.datacite_publish import (
    MAX_BUCKETS,
    MIN_BUCKETS,
    DatasetTransform,
    aggregation_estimate,
    merge_sorted_batches,
    open_duckdb,
    plan_bucket_count,
    plan_ranges,
    transform_datacite_outputs,
    validate_citation_batch,
    write_dataset_shards,
)
from citations_pipeline.full_state import ResourceSettings
from citations_pipeline.publish import ConfigStats, PublishError, transform_outputs
from citations_pipeline.telemetry import EventLogger


def test_pair_counts_and_legacy_arxiv_schema(tmp_path):
    def entry(citer, ref_index, provenance, match_type, reference):
        return {"doi": citer, "ref_index": ref_index, "provenance": provenance,
                "match_type": match_type, "raw_match": next(iter(reference.values())),
                "reference": reference}

    valid = tmp_path / "valid.jsonl"
    valid.write_text("\n".join(json.dumps(record) for record in [
        {"doi": "10.1234/a", "cited_by": [
            entry("10.9999/w1", 0, "publisher", "doi", {"DOI": "10.1234/a"}),
            entry("10.9999/w1", 1, "mined", "url", {"URL": "https://example.org/a"})]},
        {"doi": "10.48550/arxiv.2403.03542", "cited_by": [
            entry("10.9999/w2", 0, "mined", "arxiv", {"unstructured": "arXiv:2403.03542"})]},
    ]) + "\n")
    ambiguous = tmp_path / "ambiguous.jsonl"
    ambiguous.write_text("")
    result = transform_datacite_outputs(valid, ambiguous, tmp_path / "out", EventLogger(),
                                        ResourceSettings(duckdb_memory_mb=256))
    assert result["datacite"].stats["all"] == ConfigStats(works=2, citations=2, references=3)
    assert result["datacite"].stats["mined"] == ConfigStats(works=2, citations=2, references=2)
    arxiv_file = result["arxiv"].files["data/all.parquet"]
    assert pq.read_schema(arxiv_file).names == ["arxiv_doi", "arxiv_id", "citation_count", "cited_by", "reference_count"]
    row = pq.read_table(arxiv_file).to_pylist()[0]
    assert row["arxiv_doi"] == "10.48550/arXiv.2403.03542"
    assert set(row["cited_by"][0]["matches"][0]) == {"provenance", "raw_match", "reference"}
    assert result["datacite"].ambiguity_rows == 0


# --- fixtures ---------------------------------------------------------------

ARXIV_B = "10.48550/arxiv.2403.03542"
ARXIV_C = "10.48550/arxiv.1409.1556"
UNICODE_TEXT = "Simonyan écrit arXiv:1409.1556 — 日本語 🚀"


def entry(
    citer: str,
    ref_index: int,
    provenance: str,
    match_type: str,
    reference: dict[str, Any],
    raw_match: str | None = None,
) -> dict[str, Any]:
    if raw_match is None:
        raw_match = next(
            str(value) for value in reference.values() if isinstance(value, str)
        )
    return {
        "doi": citer,
        "ref_index": ref_index,
        "provenance": provenance,
        "match_type": match_type,
        "raw_match": raw_match,
        "reference": reference,
    }


def mixed_records() -> list[dict[str, Any]]:
    """Four targets (two arXiv), mixed provenances, out-of-order ref indexes,
    non-string and Unicode reference values, and one unsafe key outside the
    arXiv scope."""
    return [
        {
            "doi": "10.1234/a",
            "cited_by": [
                entry("10.9999/w1", 3, "publisher", "doi",
                      {"DOI": "10.1234/a", "doi-asserted-by": "publisher", "key": "r3"}),
                entry("10.9999/w1", 1, "mined", "url",
                      {"URL": "https://example.org/a", "key": "r1"}),
                entry("10.9999/w2", 0, "crossref", "doi",
                      {"DOI": "10.1234/a", "doi-asserted-by": "crossref"}),
                entry("10.9999/w2", 2, "mined", "url",
                      {"unstructured": "see https://example.org/a"},
                      raw_match="https://example.org/a"),
                entry("10.9999/w3", 7, "mined", "url",
                      {"URL": "https://example.org/a", "first-page": "12",
                       'bad"key': "dropped"}),
            ],
        },
        {
            "doi": ARXIV_B,
            "cited_by": [
                entry("10.9999/w2", 5, "publisher", "doi",
                      {"DOI": "10.48550/arXiv.2403.03542", "doi-asserted-by": "publisher"}),
                entry("10.9999/w2", 6, "mined", "arxiv",
                      {"unstructured": "arXiv:2403.03542"}, raw_match="arXiv:2403.03542"),
                entry("10.9999/w4", 0, "mined", "arxiv",
                      {"unstructured": "arXiv:2403.03542", "year": 2024},
                      raw_match="arXiv:2403.03542"),
            ],
        },
        {
            "doi": ARXIV_C,
            "cited_by": [
                entry("10.9999/w5", 2, "mined", "arxiv",
                      {"key": "r9", "unstructured": UNICODE_TEXT, "year": 2014,
                       "issn-type": [{"value": "é", "type": "print"}], "note": None},
                      raw_match="arXiv:1409.1556"),
            ],
        },
        {
            "doi": "10.5555/d",
            "cited_by": [
                entry("10.9999/w6", 1, "crossref", "doi",
                      {"DOI": "10.5555/d", "doi-asserted-by": "crossref",
                       "volume-title": "Vol"}),
            ],
        },
    ]


def ambiguity_row(
    citing: str,
    ref_index: int,
    url: str,
    candidates: list[str],
    reference: dict[str, Any] | None = None,
    raw_match: str | None = None,
) -> dict[str, Any]:
    return {
        "citing_doi": citing,
        "ref_index": ref_index,
        "url": url,
        "raw_match": raw_match or url.removeprefix("https://"),
        "candidate_dois": candidates,
        "reference": reference if reference is not None else {"unstructured": url},
    }


def mixed_ambiguity_rows() -> list[dict[str, Any]]:
    shared = ambiguity_row("10.9999/w1", 2, "https://example.org/x", ["10.1/a", "10.1/b"],
                           {"unstructured": "voir https://example.org/x", "extra-key": "v"})
    return [
        ambiguity_row("10.9999/w1", 2, "https://example.org/y", ["10.1/c", "10.1/d"]),
        shared,
        ambiguity_row("10.9999/w0", 0, "https://example.org/ü", ["10.1/a", "10.1/b", "10.1/c"]),
        dict(shared),
    ]


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> Path:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def quiet_events(lines: list[str] | None = None) -> EventLogger:
    return EventLogger(output=lines.append if lines is not None else lambda _: None)


def run_transform(
    tmp_path: Path,
    records: list[dict[str, Any]],
    ambiguity: list[dict[str, Any]] | None = None,
    name: str = "out",
    events: EventLogger | None = None,
) -> dict[str, DatasetTransform]:
    valid = write_jsonl(tmp_path / f"{name}-valid.jsonl", records)
    ambiguous = write_jsonl(tmp_path / f"{name}-ambiguous.jsonl", ambiguity or [])
    return transform_datacite_outputs(
        valid,
        ambiguous,
        tmp_path / name,
        events or quiet_events(),
        ResourceSettings(duckdb_memory_mb=256),
    )


def rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


def config_rows(transform: DatasetTransform, config: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name in transform.config_files[config]:
        out.extend(rows(transform.files[name]))
    return out


def struct_names(struct_type: pa.DataType) -> list[str]:
    return [struct_type.field(i).name for i in range(struct_type.num_fields)]


# --- general dataset --------------------------------------------------------


def test_general_schema_counts_and_ordering(tmp_path):
    result = run_transform(tmp_path, mixed_records())
    general = result["datacite"]

    assert general.stats == {
        "all": ConfigStats(works=4, citations=7, references=10),
        "asserted": ConfigStats(works=3, citations=4, references=4),
        "mined": ConfigStats(works=3, citations=6, references=6),
    }
    assert general.config_files == {
        "all": ("data/all/part-00000.parquet",),
        "asserted": ("data/asserted/part-00000.parquet",),
        "mined": ("data/mined/part-00000.parquet",),
        "ambiguous_urls": ("data/ambiguous_urls/part-00000.parquet",),
    }
    assert general.files["data/all/part-00000.parquet"] == (
        tmp_path / "out" / "datacite" / "data" / "all" / "part-00000.parquet"
    )

    schema = pq.read_schema(general.files["data/all/part-00000.parquet"])
    assert schema.names == ["doi", "citation_count", "cited_by", "reference_count"]
    assert schema.field("doi").type == pa.string()
    assert schema.field("citation_count").type == pa.int64()
    assert schema.field("reference_count").type == pa.int64()
    citer = schema.field("cited_by").type.value_type
    assert struct_names(citer) == ["doi", "matches", "provenance"]
    match = citer.field("matches").type.value_type
    assert struct_names(match) == ["provenance", "match_type", "raw_match", "reference"]
    reference = match.field("reference").type
    assert struct_names(reference) == list(general.reference_keys["all"])
    assert all(reference.field(i).type == pa.string() for i in range(reference.num_fields))

    all_rows = config_rows(general, "all")
    assert [row["doi"] for row in all_rows] == ["10.1234/a", ARXIV_B, ARXIV_C, "10.5555/d"]
    work_a = all_rows[0]
    assert (work_a["citation_count"], work_a["reference_count"]) == (3, 5)
    assert [citer["doi"] for citer in work_a["cited_by"]] == [
        "10.9999/w1", "10.9999/w2", "10.9999/w3"
    ]
    w1, w2, w3 = work_a["cited_by"]
    # Strongest provenance wins; matches keep original reference order.
    assert (w1["provenance"], w2["provenance"], w3["provenance"]) == (
        "publisher", "crossref", "mined"
    )
    assert [(m["provenance"], m["match_type"]) for m in w1["matches"]] == [
        ("mined", "url"), ("publisher", "doi")
    ]
    assert w1["matches"][0]["raw_match"] == "https://example.org/a"
    assert w1["matches"][1]["reference"]["doi-asserted-by"] == "publisher"
    assert w1["matches"][1]["reference"]["URL"] is None
    work_b = all_rows[1]
    assert [citer["doi"] for citer in work_b["cited_by"]] == ["10.9999/w2", "10.9999/w4"]
    assert work_b["cited_by"][0]["provenance"] == "publisher"
    assert [m["match_type"] for m in work_b["cited_by"][0]["matches"]] == ["doi", "arxiv"]

    asserted_rows = config_rows(general, "asserted")
    assert [row["doi"] for row in asserted_rows] == ["10.1234/a", ARXIV_B, "10.5555/d"]
    assert [citer["doi"] for citer in asserted_rows[0]["cited_by"]] == [
        "10.9999/w1", "10.9999/w2"
    ]
    assert [m["provenance"] for m in asserted_rows[0]["cited_by"][0]["matches"]] == [
        "publisher"
    ]
    mined_rows = config_rows(general, "mined")
    assert [row["doi"] for row in mined_rows] == ["10.1234/a", ARXIV_B, ARXIV_C]
    assert mined_rows[0]["citation_count"] == 3
    for config in ("all", "asserted", "mined"):
        for row in config_rows(general, config):
            assert row["citation_count"] == len(row["cited_by"])
            assert row["reference_count"] == sum(
                len(citer["matches"]) for citer in row["cited_by"]
            )


def test_reference_values_stringified_with_unicode(tmp_path):
    result = run_transform(tmp_path, mixed_records())
    for transform, doi_field, doi in (
        (result["datacite"], "doi", ARXIV_C),
        (result["arxiv"], "arxiv_doi", "10.48550/arXiv.1409.1556"),
    ):
        work = next(row for row in config_rows(transform, "all") if row[doi_field] == doi)
        reference = work["cited_by"][0]["matches"][0]["reference"]
        assert reference["unstructured"] == UNICODE_TEXT
        assert reference["year"] == "2014"
        assert reference["issn-type"] == '[{"type":"print","value":"é"}]'
        assert reference["note"] is None
        assert reference["DOI"] is None
        assert "note" in reference


def test_reference_keys_scoped_per_destination_and_configuration(tmp_path):
    result = run_transform(tmp_path, mixed_records(), mixed_ambiguity_rows())
    general, arxiv = result["datacite"], result["arxiv"]
    assert general.reference_keys == {
        "all": ("DOI", "URL", "doi-asserted-by", "first-page", "issn-type", "key",
                "note", "unstructured", "volume-title", "year"),
        "asserted": ("DOI", "doi-asserted-by", "key", "volume-title"),
        "mined": ("URL", "first-page", "issn-type", "key", "note", "unstructured", "year"),
        "ambiguous_urls": ("extra-key", "unstructured"),
    }
    assert arxiv.reference_keys == {
        "all": ("DOI", "doi-asserted-by", "issn-type", "key", "note", "unstructured", "year"),
        "asserted": ("DOI", "doi-asserted-by"),
        "mined": ("issn-type", "key", "note", "unstructured", "year"),
    }
    assert general.dropped_reference_keys == ('bad"key',)
    assert arxiv.dropped_reference_keys == ()
    for transform in (general, arxiv):
        for config in ("all", "asserted", "mined"):
            schema = pq.read_schema(transform.files[transform.config_files[config][0]])
            match = schema.field("cited_by").type.value_type.field("matches").type.value_type
            assert struct_names(match.field("reference").type) == list(
                transform.reference_keys[config]
            )
    w3 = config_rows(general, "all")[0]["cited_by"][2]
    assert 'bad"key' not in w3["matches"][0]["reference"]
    assert w3["matches"][0]["reference"]["first-page"] == "12"


# --- arXiv projection -------------------------------------------------------


def legacy_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The arXiv subset of full-mode records in the legacy extractor's shape."""
    legacy = []
    for record in records:
        if not record["doi"].startswith("10.48550/arxiv."):
            continue
        cited_by = sorted(record["cited_by"], key=lambda e: (e["doi"], e["ref_index"]))
        legacy.append(
            {
                "doi": "10.48550/arXiv." + record["doi"].removeprefix("10.48550/arxiv."),
                "cited_by": [
                    {k: v for k, v in e.items() if k not in ("ref_index", "match_type")}
                    for e in cited_by
                ],
            }
        )
    return legacy


def test_arxiv_projection_equals_legacy_transform(tmp_path):
    result = run_transform(tmp_path, mixed_records())
    arxiv = result["arxiv"]
    legacy_valid = write_jsonl(tmp_path / "legacy.jsonl", legacy_records(mixed_records()))
    legacy = transform_outputs(legacy_valid, tmp_path / "legacy", quiet_events())

    assert arxiv.config_files == {
        "all": ("data/all.parquet",),
        "asserted": ("data/asserted.parquet",),
        "mined": ("data/mined.parquet",),
    }
    assert arxiv.files["data/all.parquet"] == tmp_path / "out" / "arxiv" / "data" / "all.parquet"
    assert arxiv.stats == legacy.stats
    assert arxiv.reference_keys == legacy.reference_keys
    assert arxiv.ambiguity_rows == 0
    for config in ("all", "asserted", "mined"):
        new_file = arxiv.files[f"data/{config}.parquet"]
        assert pq.read_schema(new_file).equals(pq.read_schema(legacy.parquet[config]))
        assert rows(new_file) == rows(legacy.parquet[config])
    assert arxiv.stats == {
        "all": ConfigStats(works=2, citations=3, references=4),
        "asserted": ConfigStats(works=1, citations=1, references=1),
        "mined": ConfigStats(works=2, citations=3, references=3),
    }
    all_rows = rows(arxiv.files["data/all.parquet"])
    assert [(row["arxiv_doi"], row["arxiv_id"]) for row in all_rows] == [
        ("10.48550/arXiv.2403.03542", "2403.03542"),
        ("10.48550/arXiv.1409.1556", "1409.1556"),
    ]
    assert "match_type" not in all_rows[0]["cited_by"][0]["matches"][0]


def test_arxiv_projection_matches_general_arxiv_subset(tmp_path):
    result = run_transform(tmp_path, mixed_records())
    for config in ("all", "asserted", "mined"):
        general = [
            row for row in config_rows(result["datacite"], config)
            if row["doi"].startswith("10.48550/arxiv.")
        ]
        projected = config_rows(result["arxiv"], config)
        assert len(general) == len(projected)
        for g, p in zip(general, projected):
            assert p["arxiv_doi"].lower() == g["doi"]
            assert p["citation_count"] == g["citation_count"]
            assert p["reference_count"] == g["reference_count"]
            assert [c["doi"] for c in p["cited_by"]] == [c["doi"] for c in g["cited_by"]]
            for pc_, gc in zip(p["cited_by"], g["cited_by"]):
                assert pc_["provenance"] == gc["provenance"]
                assert [m["raw_match"] for m in pc_["matches"]] == [
                    m["raw_match"] for m in gc["matches"]
                ]
        assert result["arxiv"].stats[config] == ConfigStats(
            works=len(general),
            citations=sum(row["citation_count"] for row in general),
            references=sum(row["reference_count"] for row in general),
        )


# --- empty configurations ---------------------------------------------------


def test_empty_asserted_and_ambiguity_configurations_keep_schemas(tmp_path):
    records = [
        {"doi": "10.1234/a", "cited_by": [
            entry("10.9999/w1", 0, "mined", "url", {"URL": "https://example.org/a"})]},
        {"doi": ARXIV_B, "cited_by": [
            entry("10.9999/w2", 0, "mined", "arxiv", {"unstructured": "arXiv:2403.03542"},
                  raw_match="arXiv:2403.03542")]},
    ]
    result = run_transform(tmp_path, records)
    general, arxiv = result["datacite"], result["arxiv"]

    for transform, all_name, asserted_name in (
        (general, "data/all/part-00000.parquet", "data/asserted/part-00000.parquet"),
        (arxiv, "data/all.parquet", "data/asserted.parquet"),
    ):
        assert transform.config_files["asserted"] == (asserted_name,)
        assert transform.stats["asserted"] == ConfigStats(works=0, citations=0, references=0)
        assert transform.stats["mined"] == transform.stats["all"]
        empty = pq.read_table(transform.files[asserted_name])
        assert empty.num_rows == 0
        assert empty.schema.equals(pq.read_schema(transform.files[all_name]))
        assert transform.reference_keys["asserted"] == transform.reference_keys["all"]
        assert transform.sizes[asserted_name] == transform.files[asserted_name].stat().st_size > 0

    assert general.ambiguity_rows == 0
    ambiguous = pq.read_table(general.files["data/ambiguous_urls/part-00000.parquet"])
    assert ambiguous.num_rows == 0
    assert ambiguous.schema.names == [
        "citing_doi", "ref_index", "url", "raw_match", "candidate_dois", "reference"
    ]
    assert ambiguous.schema.field("ref_index").type == pa.int64()
    assert ambiguous.schema.field("candidate_dois").type.value_type == pa.string()
    assert struct_names(ambiguous.schema.field("reference").type) == ["URL", "unstructured"]
    assert general.reference_keys["ambiguous_urls"] == ("URL", "unstructured")


# --- ambiguity --------------------------------------------------------------


def test_ambiguity_rows_are_deduplicated_ordered_and_validated(tmp_path):
    result = run_transform(tmp_path, mixed_records(), mixed_ambiguity_rows())
    general = result["datacite"]
    assert general.ambiguity_rows == 3
    assert result["arxiv"].ambiguity_rows == 0
    table = pq.read_table(general.files["data/ambiguous_urls/part-00000.parquet"])
    assert table.num_rows == 3
    assert table.schema.names == [
        "citing_doi", "ref_index", "url", "raw_match", "candidate_dois", "reference"
    ]
    assert [row["doi"] for row in config_rows(general, "all")]  # citations unaffected
    ambiguous = table.to_pylist()
    assert [(r["citing_doi"], r["ref_index"], r["url"]) for r in ambiguous] == [
        ("10.9999/w0", 0, "https://example.org/ü"),
        ("10.9999/w1", 2, "https://example.org/x"),
        ("10.9999/w1", 2, "https://example.org/y"),
    ]
    assert ambiguous[0]["candidate_dois"] == ["10.1/a", "10.1/b", "10.1/c"]
    assert ambiguous[0]["raw_match"] == "example.org/ü"
    assert ambiguous[1]["reference"] == {
        "extra-key": "v", "unstructured": "voir https://example.org/x"
    }
    assert ambiguous[2]["reference"] == {
        "extra-key": None, "unstructured": "https://example.org/y"
    }
    assert general.stats["all"] == ConfigStats(works=4, citations=7, references=10)


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/b", "10.1/a"]),
         "sorted and unique"),
        (ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a", "10.1/a"]),
         "sorted and unique"),
        (ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a"]),
         "at least two candidate"),
        ({**ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a", "10.1/b"]),
          "ref_index": -1}, "ref_index"),
        ({**ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a", "10.1/b"]),
          "reference": "text"}, "JSON object"),
        ({k: v for k, v in ambiguity_row("10.9999/w1", 0, "https://e.org/x",
                                        ["10.1/a", "10.1/b"]).items() if k != "url"},
         "missing url"),
    ],
)
def test_invalid_ambiguity_rows_are_rejected(tmp_path, row, message):
    with pytest.raises(PublishError, match=f"{message}.*on line 2 of out-ambiguous.jsonl"):
        run_transform(
            tmp_path,
            mixed_records(),
            [ambiguity_row("10.9999/w9", 0, "https://e.org/z", ["10.1/a", "10.1/b"]), row],
        )


def test_conflicting_ambiguity_rows_are_rejected(tmp_path):
    first = ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a", "10.1/b"])
    second = ambiguity_row("10.9999/w1", 0, "https://e.org/x", ["10.1/a", "10.1/c"])
    with pytest.raises(PublishError, match="conflicting"):
        run_transform(tmp_path, mixed_records(), [first, second])


# --- input validation -------------------------------------------------------


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda e: e.update(match_type="text"), "invalid match_type 'text'"),
        (lambda e: e.update(match_type=None), "invalid match_type None"),
        (lambda e: e.update(provenance="bogus"), "invalid provenance 'bogus'"),
        (lambda e: e.update(ref_index=-1), "ref_index"),
        (lambda e: e.update(ref_index=True), "ref_index"),
        (lambda e: e.update(ref_index="1"), "ref_index"),
        (lambda e: e.update(raw_match=None), "raw_match"),
        (lambda e: e.update(reference=["x"]), "reference must be a JSON object"),
        (lambda e: e.pop("reference"), "missing its reference"),
        (lambda e: e.update(doi=""), "doi must be a nonempty string"),
    ],
)
def test_invalid_entries_are_rejected_naming_the_line(tmp_path, mutate, message):
    records = mixed_records()
    mutate(records[1]["cited_by"][0])
    with pytest.raises(PublishError, match=f"{message}.*on line 2 of out-valid.jsonl"):
        run_transform(tmp_path, records)


def test_malformed_jsonl_names_the_line(tmp_path):
    valid = tmp_path / "valid.jsonl"
    valid.write_text(json.dumps(mixed_records()[0]) + "\n{not json\n")
    ambiguous = tmp_path / "ambiguous.jsonl"
    ambiguous.write_text("")
    with pytest.raises(PublishError, match="invalid JSON on line 2 of valid.jsonl"):
        transform_datacite_outputs(
            valid, ambiguous, tmp_path / "out", quiet_events(), ResourceSettings()
        )
    valid.write_text(json.dumps(mixed_records()[0]) + "\n\n[1, 2]\n")
    with pytest.raises(PublishError, match="malformed record on line 3 of valid.jsonl"):
        transform_datacite_outputs(
            valid, ambiguous, tmp_path / "out", quiet_events(), ResourceSettings()
        )
    valid.write_text(json.dumps({"doi": "10.1/a", "cited_by": {}}) + "\n")
    with pytest.raises(PublishError, match="cited_by must be a list on line 1"):
        transform_datacite_outputs(
            valid, ambiguous, tmp_path / "out", quiet_events(), ResourceSettings()
        )
    write_jsonl(valid, mixed_records())
    ambiguous.write_text('{"citing_doi": 1\n')
    with pytest.raises(PublishError, match="invalid JSON on line 1 of ambiguous.jsonl"):
        transform_datacite_outputs(
            valid, ambiguous, tmp_path / "out", quiet_events(), ResourceSettings()
        )


def test_empty_or_arxiv_free_input_is_rejected(tmp_path):
    with pytest.raises(PublishError, match="no confirmed citations to publish"):
        run_transform(tmp_path, [])
    with pytest.raises(PublishError, match="no confirmed citations to publish for arxiv"):
        run_transform(tmp_path, [mixed_records()[0]], name="general-only")


def test_duplicate_tuples_and_self_citations_are_rejected(tmp_path):
    records = mixed_records()
    records.append({"doi": ARXIV_B, "cited_by": [dict(records[1]["cited_by"][0])]})
    with pytest.raises(PublishError, match="tuples appear more than once"):
        run_transform(tmp_path, records)
    records = mixed_records()
    records[0]["cited_by"].append(
        entry("10.1234/a", 9, "mined", "url", {"URL": "https://example.org/a"})
    )
    with pytest.raises(PublishError, match="self-citations"):
        run_transform(tmp_path, records, name="self")


def test_batch_count_invariants_are_enforced():
    match = pa.struct([("provenance", pa.string())])
    citer = pa.struct([("doi", pa.string()), ("matches", pa.list_(match))])
    schema = pa.schema([
        ("doi", pa.string()),
        ("citation_count", pa.int64()),
        ("cited_by", pa.list_(citer)),
        ("reference_count", pa.int64()),
    ])

    def batch(citation_count, reference_count):
        cited_by = [[
            {"doi": "10.9/w1", "matches": [{"provenance": "mined"}] * 2},
            {"doi": "10.9/w2", "matches": [{"provenance": "mined"}]},
        ]]
        return pa.record_batch(
            [["10.1/a"], [citation_count], cited_by, [reference_count]], schema=schema
        )

    assert validate_citation_batch(batch(2, 3)) == (1, 2, 3)
    with pytest.raises(PublishError, match="count invariant violated for '10.1/a'"):
        validate_citation_batch(batch(3, 3))
    with pytest.raises(PublishError, match="reference_count=2 vs 3 matches"):
        validate_citation_batch(batch(2, 2))
    # Sliced batches validate the referenced rows only.
    two = pa.concat_tables([pa.Table.from_batches([batch(2, 3)])] * 2).combine_chunks()
    sliced = two.to_batches()[0].slice(1, 1)
    assert validate_citation_batch(sliced) == (1, 2, 3)


# --- repeatability, sharding, sizes, settings, events -----------------------


def test_outputs_are_identical_regardless_of_input_order(tmp_path):
    records = mixed_records()
    shuffled = [
        {"doi": record["doi"], "cited_by": list(reversed(record["cited_by"]))}
        for record in reversed(records)
    ]
    ambiguity = mixed_ambiguity_rows()
    first = run_transform(tmp_path, records, ambiguity, name="first")
    second = run_transform(tmp_path, shuffled, list(reversed(ambiguity)), name="second")
    for destination in ("datacite", "arxiv"):
        assert first[destination].config_files == second[destination].config_files
        assert first[destination].stats == second[destination].stats
        assert first[destination].sizes == second[destination].sizes
        for name, path in first[destination].files.items():
            assert path.read_bytes() == second[destination].files[name].read_bytes(), name


def make_reader(batches: list[pa.RecordBatch], schema: pa.Schema) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(schema, iter(batches))


def test_write_dataset_shards_closes_before_exceeding_target(tmp_path):
    schema = pa.schema([("doi", pa.string()), ("payload", pa.string())])
    batches = [
        pa.record_batch(
            [[f"10.1/{i}" for i in range(start, start + 3)], ["x" * 100] * 3],
            schema=schema,
        )
        for start in range(0, 9, 3)
    ]
    batch_bytes = batches[0].nbytes
    files = write_dataset_shards(
        make_reader(batches, schema), tmp_path, "data/all", batch_bytes * 2, False
    )
    assert list(files) == ["data/all/part-00000.parquet", "data/all/part-00001.parquet"]
    assert files["data/all/part-00000.parquet"] == tmp_path / "data/all/part-00000.parquet"
    assert [pq.read_table(path).num_rows for path in files.values()] == [6, 3]
    assert [row["doi"] for path in files.values() for row in rows(path)] == [
        f"10.1/{i}" for i in range(9)
    ]
    assert pq.read_schema(files["data/all/part-00001.parquet"]).equals(schema)
    for path in files.values():
        assert pq.read_table(path).nbytes <= batch_bytes * 2

    # A batch above the target is split at row boundaries, never dropped.
    split = write_dataset_shards(
        make_reader(batches, schema), tmp_path / "split", "data/all", batch_bytes - 1, False
    )
    assert len(split) > 3
    assert [row["doi"] for path in split.values() for row in rows(path)] == [
        f"10.1/{i}" for i in range(9)
    ]
    assert all(pq.read_table(path).nbytes <= batch_bytes - 1 for path in split.values())

    # A single row above the target is an explicit error.
    huge = pa.record_batch([["10.1/big"], ["y" * 10_000]], schema=schema)
    with pytest.raises(PublishError, match="row '10.1/big' needs .* above the shard target"):
        write_dataset_shards(make_reader([huge], schema), tmp_path / "huge", "data/all", 500, False)

    # Single-file mode ignores the target; an empty reader still writes a file.
    single = write_dataset_shards(make_reader(batches, schema), tmp_path, "data/mined", 1, True)
    assert list(single) == ["data/mined.parquet"]
    assert pq.read_table(single["data/mined.parquet"]).num_rows == 9
    empty = write_dataset_shards(make_reader([], schema), tmp_path, "data/asserted", 1, False)
    assert list(empty) == ["data/asserted/part-00000.parquet"]
    table = pq.read_table(empty["data/asserted/part-00000.parquet"])
    assert table.num_rows == 0 and table.schema.equals(schema)


def test_general_configurations_shard_at_the_target(tmp_path, monkeypatch):
    monkeypatch.setattr(datacite_publish, "GENERAL_SHARD_TARGET_BYTES", 4096)
    monkeypatch.setattr(datacite_publish, "RESULT_BATCH_ROWS", 2)
    records = mixed_records()
    for i in range(40):
        records.append({"doi": f"10.7777/{i:03d}", "cited_by": [
            entry(f"10.9999/x{i}", 0, "mined", "url", {"URL": f"https://example.org/{i}"})]})
    result = run_transform(tmp_path, records, mixed_ambiguity_rows())
    general = result["datacite"]
    assert len(general.config_files["all"]) > 1
    assert general.config_files["all"] == tuple(
        f"data/all/part-{i:05d}.parquet" for i in range(len(general.config_files["all"]))
    )
    assert general.stats["all"] == ConfigStats(works=44, citations=47, references=50)
    all_rows = config_rows(general, "all")
    assert [row["doi"] for row in all_rows[:4]] == ["10.1234/a", ARXIV_B, ARXIV_C, "10.5555/d"]
    assert sorted(row["doi"] for row in all_rows[4:]) == [f"10.7777/{i:03d}" for i in range(40)]
    for name in general.config_files["all"]:
        assert pq.read_table(general.files[name]).nbytes <= 4096
        assert general.sizes[name] == general.files[name].stat().st_size
    assert result["arxiv"].config_files["all"] == ("data/all.parquet",)


def test_sizes_events_and_duckdb_settings(tmp_path):
    lines: list[str] = []
    result = run_transform(tmp_path, mixed_records(), mixed_ambiguity_rows(), events=quiet_events(lines))
    for transform in result.values():
        assert set(transform.sizes) == set(transform.files)
        for name, path in transform.files.items():
            assert transform.sizes[name] == path.stat().st_size > 0
    events = [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in lines]
    completed = {e["phase"] for e in events if e["event"] == "phase_completed"}
    assert {
        "flatten_valid_output", "flatten_ambiguous_urls", "scan_reference_keys",
        "widen_flat_references", "check_flat_references", "build_datacite_all",
        "build_datacite_asserted", "build_datacite_mined", "build_arxiv_all",
        "build_arxiv_asserted", "build_arxiv_mined", "build_datacite_ambiguous_urls",
        "validate_arxiv_projection",
    } <= completed
    assert not [e for e in events if e["event"] == "phase_failed"]
    final = next(e for e in events if e["event"] == "transform_completed")
    assert final["rows"] == 10 and final["arxiv_rows"] == 4 and final["ambiguity_rows"] == 3
    assert final["datacite_bytes"] == sum(result["datacite"].sizes.values())
    assert final["datacite_dropped_reference_keys"] == ['bad"key']
    assert final["datasets"] == {
        "datacite": "cometadata/crossref-datacite-citations",
        "arxiv": "cometadata/crossref-arxiv-citations",
    }
    # Intermediates are cleaned up; the spill directory is under the transform dir.
    out = tmp_path / "out"
    for name in ("flat", "ambiguity", "partitions"):
        assert not (out / name).exists()
    assert not list(out.glob("*-staging.parquet"))
    assert (out / "duckdb-tmp").is_dir()
    planned = [e for e in events if e["event"] == "configuration_planned"]
    assert [e["configuration"] for e in planned] == [
        "datacite/all", "datacite/asserted", "datacite/mined",
        "arxiv/all", "arxiv/asserted", "arxiv/mined", "datacite/ambiguous_urls",
    ]
    assert all(e["ranges"] == 1 for e in planned)
    assert planned[0]["rows"] == 10 and planned[0]["estimated_bytes"] > 0

    con = open_duckdb(tmp_path / "settings", ResourceSettings(duckdb_memory_mb=300))
    try:
        assert con.execute("SELECT current_setting('memory_limit')").fetchone() == ("300.0 MiB",)
        assert con.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str(tmp_path / "settings" / "duckdb-tmp"),
        )
    finally:
        con.close()
    with pytest.raises(ValueError, match="duckdb_memory_mb"):
        transform_datacite_outputs(
            tmp_path / "out-valid.jsonl", tmp_path / "out-ambiguous.jsonl",
            tmp_path / "bad", quiet_events(), ResourceSettings(duckdb_memory_mb=0),
        )


# --- bounded-memory execution: bucket ranges, partition merge, OOM handling --


def many_records() -> list[dict[str, Any]]:
    records = mixed_records()
    for i in range(40):
        records.append({"doi": f"10.7777/{i:03d}", "cited_by": [
            entry(f"10.9999/x{i}", 0, "mined", "url", {"URL": f"https://example.org/{i}"}),
            entry(f"10.9999/y{i}", 1, "crossref", "doi", {"DOI": f"10.7777/{i:03d}"})]})
    return records


def many_ambiguity_rows() -> list[dict[str, Any]]:
    rows = mixed_ambiguity_rows()
    for i in range(30):
        rows.append(ambiguity_row(f"10.9999/z{i:02d}", i % 3, f"https://e.org/{i}",
                                  ["10.1/a", "10.1/b"]))
    return rows


def all_rows(result: dict[str, DatasetTransform]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    return {
        (destination, config): config_rows(transform, config)
        for destination, transform in result.items()
        for config in transform.config_files
    }


def test_partitioned_ranges_and_multipass_merge_match_direct_output(tmp_path, monkeypatch):
    records, ambiguity = many_records(), many_ambiguity_rows()
    direct = run_transform(tmp_path, records, ambiguity, name="direct")
    # Every non-empty bucket now exceeds the budget on its own, so each
    # configuration is built from one partition per bucket and merged with a
    # fan-in of two, i.e. several merge passes.
    monkeypatch.setattr(datacite_publish, "AGGREGATION_ROW_OVERHEAD_BYTES", 1 << 30)
    lines: list[str] = []
    valid = write_jsonl(tmp_path / "part-valid.jsonl", records)
    ambiguous = write_jsonl(tmp_path / "part-ambiguous.jsonl", ambiguity)
    partitioned = transform_datacite_outputs(
        valid, ambiguous, tmp_path / "part", quiet_events(lines),
        ResourceSettings(duckdb_memory_mb=256, merge_fan_in=2),
    )
    events = [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in lines]
    planned = {e["configuration"]: e for e in events if e["event"] == "configuration_planned"}
    assert planned["datacite/all"]["ranges"] > 2
    partitioned_events = {
        e["configuration"]: e for e in events if e["event"] == "configuration_partitioned"
    }
    assert set(partitioned_events) == set(planned)
    assert partitioned_events["datacite/all"]["partitions"] == planned["datacite/all"]["ranges"]
    assert partitioned_events["datacite/all"]["fan_in"] == 2
    assert partitioned_events["datacite/all"]["partitions"] > 4  # forces >= 2 merge passes

    for destination in ("datacite", "arxiv"):
        assert partitioned[destination].stats == direct[destination].stats
        assert partitioned[destination].config_files == direct[destination].config_files
        assert partitioned[destination].reference_keys == direct[destination].reference_keys
    assert partitioned["datacite"].ambiguity_rows == direct["datacite"].ambiguity_rows == 33
    assert all_rows(partitioned) == all_rows(direct)
    for destination in ("datacite", "arxiv"):
        for name, path in partitioned[destination].files.items():
            assert pq.read_schema(path).equals(pq.read_schema(direct[destination].files[name])), name
    assert not (tmp_path / "part" / "partitions").exists()


def test_out_of_memory_splits_ranges_until_buckets_fit(tmp_path, monkeypatch):
    records, ambiguity = many_records(), many_ambiguity_rows()
    direct = run_transform(tmp_path, records, ambiguity, name="direct")
    original_select = datacite_publish.select_references
    original_ambiguity = datacite_publish.ambiguity_query
    attempts: list[tuple[str, int, int]] = []

    def select_references(con, flat_dir, destination, config, buckets):
        attempts.append((f"{destination}/{config}", *buckets))
        if buckets[0] != buckets[1]:
            raise duckdb.OutOfMemoryException("injected: range too large")
        original_select(con, flat_dir, destination, config, buckets)

    def ambiguity_query(hive_dir, buckets):
        attempts.append(("ambiguous_urls", *buckets))
        if buckets[0] != buckets[1]:
            raise duckdb.OutOfMemoryException("injected: range too large")
        return original_ambiguity(hive_dir, buckets)

    monkeypatch.setattr(datacite_publish, "select_references", select_references)
    monkeypatch.setattr(datacite_publish, "ambiguity_query", ambiguity_query)
    lines: list[str] = []
    valid = write_jsonl(tmp_path / "oom-valid.jsonl", records)
    ambiguous = write_jsonl(tmp_path / "oom-ambiguous.jsonl", ambiguity)
    recovered = transform_datacite_outputs(
        valid, ambiguous, tmp_path / "oom", quiet_events(lines), ResourceSettings(duckdb_memory_mb=256)
    )
    events = [json.loads(line.removeprefix("PIPELINE_EVENT=")) for line in lines]
    splits = [e for e in events if e["event"] == "configuration_memory_split"]
    assert splits and splits[0] == {
        "event": "configuration_memory_split", "timestamp": splits[0]["timestamp"],
        "configuration": "datacite/all", "buckets": [0, MIN_BUCKETS - 1],
    }
    assert (
        "datacite/all", 0, MIN_BUCKETS - 1) in attempts and ("datacite/all", 0, 0) in attempts
    assert all_rows(recovered) == all_rows(direct)
    for destination in ("datacite", "arxiv"):
        assert recovered[destination].stats == direct[destination].stats
    assert not (tmp_path / "oom" / "partitions").exists()

    def always_oom(con, flat_dir, destination, config, buckets):
        raise duckdb.OutOfMemoryException("injected: never fits")

    monkeypatch.setattr(datacite_publish, "select_references", always_oom)
    with pytest.raises(PublishError, match="datacite/all: bucket 0 cannot be aggregated"):
        transform_datacite_outputs(
            valid, ambiguous, tmp_path / "oom2", quiet_events(), ResourceSettings(duckdb_memory_mb=256)
        )
    assert not (tmp_path / "oom2" / "datacite" / "data" / "all").exists()


def test_streaming_out_of_memory_is_recognized():
    assert datacite_publish._is_out_of_memory(duckdb.OutOfMemoryException("x"))
    assert datacite_publish._is_out_of_memory(OSError("Out of Memory Error: failed to allocate"))
    assert not datacite_publish._is_out_of_memory(OSError("No space left on device"))
    assert not datacite_publish._is_out_of_memory(duckdb.BinderException("x"))


def test_memory_model_planning():
    budget = 1 << 20
    assert aggregation_estimate(10, 100) == 10 * datacite_publish.AGGREGATION_ROW_OVERHEAD_BYTES + 1000
    assert plan_bucket_count(0, 0, budget) == MIN_BUCKETS
    assert plan_bucket_count(10**9, 10**12, budget) == MAX_BUCKETS
    per_bucket = budget // datacite_publish.BUCKETS_PER_RANGE
    rows = 40 * per_bucket // datacite_publish.AGGREGATION_ROW_OVERHEAD_BYTES
    assert plan_bucket_count(rows, 0, budget) == 40

    assert plan_ranges({}, 4, budget) == [(0, 3)]
    one_row = aggregation_estimate(1, 0)
    stats = {0: (1, 0), 1: (1, 0), 2: (1, 0), 3: (1, 0), 5: (1, 0)}
    assert plan_ranges(stats, 8, 2 * one_row) == [(0, 1), (2, 4), (5, 7)]
    assert plan_ranges(stats, 8, 10 * one_row) == [(0, 7)]
    # An oversized bucket forms its own range; empty buckets attach freely.
    assert plan_ranges({1: (10, 0)}, 4, one_row) == [(0, 0), (1, 1), (2, 3)]
    assert plan_ranges({0: (10, 0), 1: (1, 0)}, 2, one_row) == [(0, 0), (1, 1)]


def test_merge_sorted_batches_orders_across_uneven_sources():
    schema = pa.schema([("doi", pa.string()), ("citation_count", pa.int64())])

    def batch(rows):
        return pa.record_batch([[d for d, _ in rows], [c for _, c in rows]], schema=schema)

    def keys(b):
        return [(-c, d) for c, d in zip(b.column("citation_count").to_pylist(), b.column("doi").to_pylist())]

    sources = [
        [batch([("a", 9), ("d", 5)]), batch([]), batch([("e", 5), ("h", 1)])],
        [batch([("b", 9)]), batch([("c", 7), ("f", 3), ("g", 3)]), batch([("i", 1)])],
        [],
        [batch([("j", 0)])],
    ]
    chunks = list(merge_sorted_batches([iter(s) for s in sources], keys))
    merged = [row for chunk in chunks for row in chunk.to_pylist()]
    assert [(r["doi"], r["citation_count"]) for r in merged] == [
        ("a", 9), ("b", 9), ("c", 7), ("d", 5), ("e", 5), ("f", 3), ("g", 3),
        ("h", 1), ("i", 1), ("j", 0),
    ]
    assert len(chunks) > 1 and all(chunk.schema.equals(schema) for chunk in chunks)
    assert list(merge_sorted_batches([iter([]), iter([batch([])])], keys)) == []


def test_row_groups_are_capped_in_bytes(tmp_path, monkeypatch):
    schema = pa.schema([("doi", pa.string()), ("payload", pa.string())])
    batches = [
        pa.record_batch([[f"10.1/{i}" for i in range(start, start + 8)], ["x" * 100] * 8], schema=schema)
        for start in range(0, 16, 8)
    ]
    row_bytes = batches[0].slice(0, 1).nbytes
    monkeypatch.setattr(datacite_publish, "SHARD_ROW_GROUP_BYTES", 2 * row_bytes)
    files = write_dataset_shards(make_reader(batches, schema), tmp_path, "data/all", 10 * row_bytes, False)
    groups = [pq.ParquetFile(path).metadata.num_row_groups for path in files.values()]
    assert len(files) == 2 and all(g >= 4 for g in groups)
    assert [row["doi"] for path in files.values() for row in rows(path)] == [f"10.1/{i}" for i in range(16)]
    single = write_dataset_shards(make_reader(batches, schema), tmp_path, "data/mined", 1, True)
    assert pq.ParquetFile(single["data/mined.parquet"]).metadata.num_row_groups >= 8


def test_partitioned_copy_retries_on_one_thread_and_restores_settings(tmp_path, monkeypatch):
    con = open_duckdb(tmp_path, ResourceSettings(duckdb_memory_mb=256))
    try:
        (threads,) = con.execute("SELECT current_setting('threads')").fetchone()
        (flush,) = con.execute(
            "SELECT current_setting('partitioned_write_flush_threshold')"
        ).fetchone()
        original = datacite_publish._execute_copy
        seen: list[tuple[int, int]] = []

        def flaky_copy(con, sql):
            seen.append((
                int(con.execute("SELECT current_setting('threads')").fetchone()[0]),
                int(con.execute(
                    "SELECT current_setting('partitioned_write_flush_threshold')"
                ).fetchone()[0]),
            ))
            if len(seen) == 1:
                raise duckdb.OutOfMemoryException("injected")
            original(con, sql)

        monkeypatch.setattr(datacite_publish, "_execute_copy", flaky_copy)
        datacite_publish._copy_partitioned(
            con, "SELECT i AS x, i % 4 AS bucket FROM range(100) t(i)", tmp_path / "hive"
        )
        assert seen == [(min(2, int(threads)), 65_536), (1, 65_536)]
        assert sorted(p.name for p in (tmp_path / "hive").iterdir()) == [
            f"bucket={i}" for i in range(4)
        ]
        assert con.execute("SELECT current_setting('threads')").fetchone() == (threads,)
        assert con.execute(
            "SELECT current_setting('partitioned_write_flush_threshold')"
        ).fetchone() == (flush,)
        assert con.execute(
            f"SELECT count(*) FROM {datacite_publish._hive_source(tmp_path / 'hive')}"
        ).fetchone() == (100,)

        def never(con, sql):
            raise duckdb.OutOfMemoryException("injected")

        monkeypatch.setattr(datacite_publish, "_execute_copy", never)
        with pytest.raises(PublishError, match="partitioning hive needs more than the DuckDB"):
            datacite_publish._copy_partitioned(
                con, "SELECT i AS x, i % 4 AS bucket FROM range(100) t(i)", tmp_path / "hive"
            )
        assert con.execute("SELECT current_setting('threads')").fetchone() == (threads,)
    finally:
        con.close()
