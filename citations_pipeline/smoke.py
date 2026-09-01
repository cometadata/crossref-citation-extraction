from __future__ import annotations

import gzip
import io
import json
import tarfile
from pathlib import Path

SMOKE_EXPECTED = {
    "all": {"works": 2, "citations": 3, "references": 3},
    "asserted": {"works": 1, "citations": 2, "references": 2},
    "mined": {"works": 1, "citations": 1, "references": 1},
    "failed_works": 1,
}

_SMOKE_WORKS = [
    {
        "DOI": "10.5555/smoke-pub",
        "reference": [
            {
                "key": "r1",
                "DOI": "10.48550/arXiv.2403.03542",
                "doi-asserted-by": "publisher",
            }
        ],
    },
    {
        "DOI": "10.5555/smoke-mixed",
        "reference": [
            {
                "key": "r1",
                "DOI": "10.48550/arXiv.2403.03542",
                "doi-asserted-by": "crossref",
            },
            {
                "key": "r2",
                "unstructured": "Simonyan, K.: Very deep networks. arXiv:1409.1556",
            },
        ],
    },
    {
        "DOI": "10.5555/smoke-fail",
        "reference": [
            {"key": "r1", "unstructured": "Nonexistent preprint arXiv:2499.99999"}
        ],
    },
]

_SMOKE_DATACITE_IDS = (
    "10.48550/arxiv.2403.03542",
    "10.48550/arxiv.1409.1556",
    "10.1234/not-arxiv",
)


def create_smoke_snapshot(path: Path) -> None:
    payload = (
        json.dumps({"items": _SMOKE_WORKS}, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as outer,
        tarfile.open(fileobj=outer, mode="w|") as archive,
    ):
        info = tarfile.TarInfo("smoke.json")
        info.size = len(payload)
        info.mtime = 0
        info.mode = 0o644
        archive.addfile(info, io.BytesIO(payload))


def create_smoke_datacite_records(path: Path) -> None:
    lines = "".join(
        json.dumps({"id": record_id}) + "\n" for record_id in _SMOKE_DATACITE_IDS
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with (
        path.open("wb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as outer,
    ):
        outer.write(lines)
