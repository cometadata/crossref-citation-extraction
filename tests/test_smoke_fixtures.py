import gzip
import json
import tarfile
from pathlib import Path

from citations_pipeline.smoke import (
    SMOKE_EXPECTED,
    create_smoke_datacite_records,
    create_smoke_snapshot,
)


def test_expected_counts_partition() -> None:
    assert SMOKE_EXPECTED["all"]["references"] == (
        SMOKE_EXPECTED["asserted"]["references"]
        + SMOKE_EXPECTED["mined"]["references"]
    )
    assert SMOKE_EXPECTED == {
        "all": {"works": 2, "citations": 3, "references": 3},
        "asserted": {"works": 1, "citations": 2, "references": 2},
        "mined": {"works": 1, "citations": 1, "references": 1},
        "failed_works": 1,
    }


def test_snapshot_is_deterministic_and_well_formed(tmp_path: Path) -> None:
    first = tmp_path / "a" / "all.json.tar.gz"
    second = tmp_path / "b" / "all.json.tar.gz"
    create_smoke_snapshot(first)
    create_smoke_snapshot(second)
    assert first.read_bytes() == second.read_bytes()

    with tarfile.open(first, "r:gz") as archive:
        names = archive.getnames()
        payload = json.loads(archive.extractfile(names[0]).read())
    dois = [item["DOI"] for item in payload["items"]]
    assert dois == ["10.5555/smoke-pub", "10.5555/smoke-mixed", "10.5555/smoke-fail"]
    mixed = payload["items"][1]
    asserted_by = [
        ref.get("doi-asserted-by") for ref in mixed["reference"]
    ]
    assert "crossref" in asserted_by


def test_datacite_records_are_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "a.jsonl.gz"
    second = tmp_path / "b.jsonl.gz"
    create_smoke_datacite_records(first)
    create_smoke_datacite_records(second)
    assert first.read_bytes() == second.read_bytes()
    with gzip.open(first, "rt", encoding="utf-8") as handle:
        ids = [json.loads(line)["id"] for line in handle]
    assert "10.48550/arxiv.2403.03542" in ids
    assert "10.48550/arxiv.1409.1556" in ids
    assert not any(record_id.endswith("2499.99999") for record_id in ids)
