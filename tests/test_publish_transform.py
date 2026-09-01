import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from citations_pipeline.publish import PublishError, transform_outputs
from citations_pipeline.telemetry import EventLogger


def write_valid_jsonl(path: Path) -> None:
    # Work A: cited by W1 (publisher) and W2 (one crossref + one mined match).
    # Work B: cited by W3 (mined) with non-string reference values.
    records = [
        {
            "doi": "10.48550/arXiv.2403.03542",
            "citation_count": 3,
            "cited_by": [
                {
                    "doi": "10.9999/w1",
                    "provenance": "publisher",
                    "raw_match": "10.48550/arXiv.2403.03542",
                    "reference": {
                        "key": "r1",
                        "DOI": "10.48550/arXiv.2403.03542",
                        "doi-asserted-by": "publisher",
                    },
                },
                {
                    "doi": "10.9999/w2",
                    "provenance": "crossref",
                    "raw_match": "10.48550/arXiv.2403.03542",
                    "reference": {
                        "key": "r1",
                        "DOI": "10.48550/arXiv.2403.03542",
                        "doi-asserted-by": "crossref",
                    },
                },
                {
                    "doi": "10.9999/w2",
                    "provenance": "mined",
                    "raw_match": "arXiv:2403.03542",
                    "reference": {"key": "r2", "unstructured": "see arXiv:2403.03542"},
                },
            ],
        },
        {
            "doi": "10.48550/arXiv.1409.1556",
            "citation_count": 1,
            "cited_by": [
                {
                    "doi": "10.9999/w3",
                    "provenance": "mined",
                    "raw_match": "arXiv:1409.1556",
                    "reference": {
                        "key": "r9",
                        "unstructured": "Simonyan écrit arXiv:1409.1556",
                        "year": 2014,
                        "issn-type": [{"type": "print"}],
                    },
                },
            ],
        },
    ]
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def test_transform_matches_published_contract(tmp_path: Path) -> None:
    valid = tmp_path / "all.json_valid.jsonl"
    write_valid_jsonl(valid)
    result = transform_outputs(valid, tmp_path / "transform", EventLogger())

    # Stats
    assert result.stats["all"].works == 2
    assert result.stats["all"].citations == 3
    assert result.stats["all"].references == 4
    assert result.stats["asserted"].works == 1
    assert result.stats["asserted"].citations == 2
    assert result.stats["asserted"].references == 2
    assert result.stats["mined"].works == 2
    assert result.stats["mined"].citations == 2
    assert result.stats["mined"].references == 2

    # Top-level schema: exact names and order
    schema = pq.read_schema(result.parquet["all"])
    assert [field.name for field in schema] == [
        "arxiv_doi",
        "arxiv_id",
        "citation_count",
        "cited_by",
        "reference_count",
    ]
    cited_by_struct = schema.field("cited_by").type.value_type
    assert [
        cited_by_struct.field(i).name for i in range(cited_by_struct.num_fields)
    ] == ["doi", "matches", "provenance"]
    match_struct = cited_by_struct.field("matches").type.value_type
    assert [match_struct.field(i).name for i in range(match_struct.num_fields)] == [
        "provenance",
        "raw_match",
        "reference",
    ]

    rows = pq.read_table(result.parquet["all"]).to_pylist()
    # Ordered by citation_count desc, then arxiv_doi
    assert [row["arxiv_doi"] for row in rows] == [
        "10.48550/arXiv.2403.03542",
        "10.48550/arXiv.1409.1556",
    ]
    work_a = rows[0]
    assert work_a["arxiv_id"] == "2403.03542"
    assert work_a["citation_count"] == 2
    assert work_a["reference_count"] == 3
    # cited_by sorted by citing DOI; W2 groups two matches in input order
    assert [citer["doi"] for citer in work_a["cited_by"]] == [
        "10.9999/w1",
        "10.9999/w2",
    ]
    w2 = work_a["cited_by"][1]
    assert w2["provenance"] == "crossref"  # publisher > crossref > mined precedence
    assert [match["provenance"] for match in w2["matches"]] == ["crossref", "mined"]
    assert w2["matches"][1]["raw_match"] == "arXiv:2403.03542"

    # Reference values are stringified; absent keys are null
    work_b = rows[1]
    reference = work_b["cited_by"][0]["matches"][0]["reference"]
    assert reference["year"] == "2014"
    assert reference["issn-type"] == '[{"type":"print"}]'
    assert reference["unstructured"] == "Simonyan écrit arXiv:1409.1556"
    assert reference["DOI"] is None

    # asserted config drops works with no publisher/crossref matches
    asserted_rows = pq.read_table(result.parquet["asserted"]).to_pylist()
    assert [row["arxiv_doi"] for row in asserted_rows] == [
        "10.48550/arXiv.2403.03542"
    ]
    assert asserted_rows[0]["reference_count"] == 2
    # W2 keeps only its crossref match in asserted
    w2_asserted = asserted_rows[0]["cited_by"][1]
    assert [match["provenance"] for match in w2_asserted["matches"]] == ["crossref"]

    # mined config: both works appear via their mined matches only
    mined_rows = pq.read_table(result.parquet["mined"]).to_pylist()
    assert {row["arxiv_doi"] for row in mined_rows} == {
        "10.48550/arXiv.2403.03542",
        "10.48550/arXiv.1409.1556",
    }


def test_transform_is_deterministic(tmp_path: Path) -> None:
    valid = tmp_path / "all.json_valid.jsonl"
    write_valid_jsonl(valid)
    first = transform_outputs(valid, tmp_path / "t1", EventLogger())
    second = transform_outputs(valid, tmp_path / "t2", EventLogger())
    for config in ("all", "asserted", "mined"):
        assert (
            first.parquet[config].read_bytes() == second.parquet[config].read_bytes()
        )


def test_transform_drops_unsafe_reference_keys(tmp_path: Path) -> None:
    valid = tmp_path / "all.json_valid.jsonl"
    record = {
        "doi": "10.48550/arXiv.2403.03542",
        "citation_count": 1,
        "cited_by": [
            {
                "doi": "10.9999/w1",
                "provenance": "mined",
                "raw_match": "arXiv:2403.03542",
                "reference": {"key": "r1", 'bad"key': "x"},
            }
        ],
    }
    valid.write_text(json.dumps(record) + "\n")
    result = transform_outputs(valid, tmp_path / "transform", EventLogger())
    assert result.dropped_reference_keys == ('bad"key',)
    rows = pq.read_table(result.parquet["all"]).to_pylist()
    assert "bad\"key" not in rows[0]["cited_by"][0]["matches"][0]["reference"]


def test_transform_rejects_empty_input(tmp_path: Path) -> None:
    valid = tmp_path / "all.json_valid.jsonl"
    valid.write_text("")
    with pytest.raises(PublishError, match="no validated citations"):
        transform_outputs(valid, tmp_path / "transform", EventLogger())
