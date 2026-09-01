from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .paths import CONFIG_NAMES, CONFIG_PROVENANCES, DATASET_REPO

REFERENCE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._:-]*$")
ARXIV_DOI_PREFIX = "10.48550/arXiv."
FLAT_BATCH_ROWS = 50_000
ROW_GROUP_SIZE = 262_144


class PublishError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigStats:
    works: int
    citations: int
    references: int


@dataclass(frozen=True)
class ReferenceKeyScan:
    keys: dict[str, tuple[str, ...]]
    dropped: tuple[str, ...]


@dataclass(frozen=True)
class TransformResult:
    parquet: dict[str, Path]
    stats: dict[str, ConfigStats]
    reference_keys: dict[str, tuple[str, ...]]
    dropped_reference_keys: tuple[str, ...]


def _iter_records(valid_path: Path) -> Iterator[dict[str, Any]]:
    with valid_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError as error:
                raise PublishError(
                    f"invalid JSON on line {line_number} of {valid_path.name}"
                ) from error
            if not isinstance(record, dict) or "doi" not in record:
                raise PublishError(
                    f"malformed record on line {line_number} of {valid_path.name}"
                )
            yield record


def _entry_fields(record: dict[str, Any], entry: Any) -> tuple[str, str, str, Any]:
    if not isinstance(entry, dict):
        raise PublishError(f"malformed cited_by entry for {record.get('doi')}")
    citing = entry.get("doi")
    provenance = entry.get("provenance")
    raw_match = entry.get("raw_match")
    if not isinstance(citing, str) or not isinstance(provenance, str):
        raise PublishError(f"malformed cited_by entry for {record.get('doi')}")
    if not isinstance(raw_match, str):
        raise PublishError(
            f"cited_by entry missing raw_match for {record.get('doi')} "
            "(extractor output predates the raw_match schema?)"
        )
    return citing, provenance, raw_match, entry.get("reference")


def scan_reference_keys(valid_path: Path) -> ReferenceKeyScan:
    per_config: dict[str, set[str]] = {name: set() for name in CONFIG_NAMES}
    dropped: set[str] = set()
    for record in _iter_records(valid_path):
        for entry in record.get("cited_by") or []:
            _citing, provenance, _raw, reference = _entry_fields(record, entry)
            if not isinstance(reference, dict):
                continue
            for key in reference:
                if not REFERENCE_KEY_RE.fullmatch(key):
                    dropped.add(key)
                    continue
                for config, provenances in CONFIG_PROVENANCES.items():
                    if provenance in provenances:
                        per_config[config].add(key)
    return ReferenceKeyScan(
        keys={name: tuple(sorted(keys)) for name, keys in per_config.items()},
        dropped=tuple(sorted(dropped)),
    )


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def flatten_valid_jsonl(
    valid_path: Path, flat_path: Path, keys: tuple[str, ...]
) -> int:
    fields = [
        pa.field("arxiv_doi", pa.string()),
        pa.field("citing_doi", pa.string()),
        pa.field("provenance", pa.string()),
        pa.field("raw_match", pa.string()),
        pa.field("match_idx", pa.int64()),
    ] + [pa.field(f"ref__{key}", pa.string()) for key in keys]
    schema = pa.schema(fields)
    flat_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    columns: dict[str, list[Any]] = {field.name: [] for field in fields}
    writer = pq.ParquetWriter(flat_path, schema, compression="zstd")
    try:
        for record in _iter_records(valid_path):
            arxiv_doi = record["doi"]
            for match_idx, entry in enumerate(record.get("cited_by") or []):
                citing, provenance, raw_match, reference = _entry_fields(
                    record, entry
                )
                columns["arxiv_doi"].append(arxiv_doi)
                columns["citing_doi"].append(citing)
                columns["provenance"].append(provenance)
                columns["raw_match"].append(raw_match)
                columns["match_idx"].append(match_idx)
                reference_dict = reference if isinstance(reference, dict) else {}
                for key in keys:
                    value = reference_dict.get(key)
                    columns[f"ref__{key}"].append(
                        None if value is None else _stringify(value)
                    )
                total += 1
                if total % FLAT_BATCH_ROWS == 0:
                    writer.write_batch(
                        pa.record_batch(
                            [columns[field.name] for field in fields], schema=schema
                        )
                    )
                    columns = {field.name: [] for field in fields}
        if columns["arxiv_doi"]:
            writer.write_batch(
                pa.record_batch(
                    [columns[field.name] for field in fields], schema=schema
                )
            )
    finally:
        writer.close()
    return total


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def build_config_parquet(
    con: duckdb.DuckDBPyConnection,
    flat_path: Path,
    config: str,
    keys: tuple[str, ...],
    out_path: Path,
) -> None:
    provenances = ", ".join(f"'{p}'" for p in CONFIG_PROVENANCES[config])
    if keys:
        reference_fields = ", ".join(
            f"{_quote_identifier(key)} := {_quote_identifier('ref__' + key)}"
            for key in keys
        )
        reference_expression = f"struct_pack({reference_fields})"
    else:
        reference_expression = "NULL"
    sql = f"""
    COPY (
      WITH per_citer AS (
        SELECT
          arxiv_doi,
          citing_doi,
          list(
            struct_pack(
              provenance := provenance,
              raw_match := raw_match,
              reference := {reference_expression}
            )
            ORDER BY match_idx
          ) AS matches,
          CASE
            WHEN bool_or(provenance = 'publisher') THEN 'publisher'
            WHEN bool_or(provenance = 'crossref') THEN 'crossref'
            ELSE 'mined'
          END AS provenance,
          count(*)::BIGINT AS match_count
        FROM read_parquet({_quote_path(flat_path)})
        WHERE provenance IN ({provenances})
        GROUP BY arxiv_doi, citing_doi
      ),
      per_work AS (
        SELECT
          arxiv_doi,
          list(
            struct_pack(
              doi := citing_doi,
              matches := matches,
              provenance := provenance
            )
            ORDER BY citing_doi
          ) AS cited_by,
          count(*)::BIGINT AS citation_count,
          sum(match_count)::BIGINT AS reference_count
        FROM per_citer
        GROUP BY arxiv_doi
      )
      SELECT
        arxiv_doi,
        substring(arxiv_doi, {len(ARXIV_DOI_PREFIX) + 1}) AS arxiv_id,
        citation_count,
        cited_by,
        reference_count
      FROM per_work
      ORDER BY citation_count DESC, arxiv_doi
    ) TO {_quote_path(out_path)}
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """
    con.execute(sql)


def transform_outputs(
    valid_path: Path, transform_dir: Path, events: Any
) -> TransformResult:
    transform_dir.mkdir(parents=True, exist_ok=True)
    with events.phase("scan_reference_keys"):
        scan = scan_reference_keys(valid_path)

    flat_path = transform_dir / "flat.parquet"
    with events.phase("flatten_valid_output"):
        total_rows = flatten_valid_jsonl(valid_path, flat_path, scan.keys["all"])
    if total_rows == 0:
        raise PublishError("no validated citations to publish")

    temp_dir = transform_dir / "duckdb-tmp"
    temp_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = {_quote_path(temp_dir)}")

    parquet: dict[str, Path] = {}
    stats: dict[str, ConfigStats] = {}
    for config in CONFIG_NAMES:
        out_path = transform_dir / f"{config}.parquet"
        with events.phase(f"build_{config}_parquet"):
            build_config_parquet(con, flat_path, config, scan.keys[config], out_path)
        works, citations, references = con.execute(
            "SELECT count(*), coalesce(sum(citation_count), 0),"
            " coalesce(sum(reference_count), 0)"
            f" FROM read_parquet({_quote_path(out_path)})"
        ).fetchone()
        parquet[config] = out_path
        stats[config] = ConfigStats(
            works=int(works), citations=int(citations), references=int(references)
        )

    if stats["all"].works == 0:
        raise PublishError("no validated citations to publish")
    expected = stats["asserted"].references + stats["mined"].references
    if stats["all"].references != expected:
        raise PublishError(
            "reference partition invariant violated: "
            f"all={stats['all'].references} != asserted+mined={expected}"
        )
    events.emit(
        "transform_completed",
        dataset=DATASET_REPO,
        rows=total_rows,
        dropped_reference_keys=list(scan.dropped),
        **{
            f"{name}_works": stats[name].works
            for name in CONFIG_NAMES
        },
    )
    return TransformResult(
        parquet=parquet,
        stats=stats,
        reference_keys=scan.keys,
        dropped_reference_keys=scan.dropped,
    )


@dataclass(frozen=True)
class PublicationResult:
    commit_sha: str
    commit_url: str


def _count(value: int) -> str:
    return f"{value:,}"


def render_dataset_card(
    stats: dict[str, ConfigStats],
    *,
    snapshot_id: str,
    datacite_release: str,
    extractor_version: str,
) -> str:
    return f"""---
license: cc0-1.0
task_categories:
  - text-classification
  - feature-extraction
language:
  - en
tags:
  - arxiv
  - crossref
  - citations
  - preprints
  - scholarly-communications
  - bibliometrics
  - doi
pretty_name: Crossref arXiv Citations
size_categories:
  - 100K<n<1M
configs:
  - config_name: all
    data_files:
      - split: train
        path: data/all.parquet
    default: true
  - config_name: asserted
    data_files:
      - split: train
        path: data/asserted.parquet
  - config_name: mined
    data_files:
      - split: train
        path: data/mined.parquet
---

# Crossref arXiv Citations

A dataset of arXiv preprints and their citations extracted from Crossref
metadata, validated against DataCite records.

## Dataset Description

This dataset maps arXiv works to the works in Crossref that cite them. Each
record represents an arXiv preprint with all known citations from
Crossref-registered works.

Built from the Crossref Metadata Plus monthly snapshot `{snapshot_id}` and the
DataCite monthly data file `{datacite_release}` with
[crossref-citation-extraction](https://github.com/cometadata/crossref-arxiv-citation-extraction)
version {extractor_version}.

### Dataset Configurations

| Config | Description | arXiv Works | Citing Works | Reference Matches |
|--------|-------------|-------------|--------------|-------------------|
| `all` | All validated citations (default) | {_count(stats["all"].works)} | {_count(stats["all"].citations)} | {_count(stats["all"].references)} |
| `asserted` | Citations where the DOI was explicitly provided by the publisher or matched by Crossref | {_count(stats["asserted"].works)} | {_count(stats["asserted"].citations)} | {_count(stats["asserted"].references)} |
| `mined` | Citations where the DOI was extracted from unstructured text | {_count(stats["mined"].works)} | {_count(stats["mined"].citations)} | {_count(stats["mined"].references)} |

### Loading the Dataset

```python
from datasets import load_dataset

dataset = load_dataset("cometadata/crossref-arxiv-citations")
asserted = load_dataset("cometadata/crossref-arxiv-citations", "asserted")
mined = load_dataset("cometadata/crossref-arxiv-citations", "mined")
```

## Data Schema

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `arxiv_doi` | string | The DOI for the arXiv work (format: `10.48550/arXiv.{{id}}`) |
| `arxiv_id` | string | The arXiv identifier (e.g., `1412.6980`) |
| `reference_count` | integer | Total number of reference match instances |
| `citation_count` | integer | Number of unique citing works |
| `cited_by` | array | List of citing work objects |

Each citing work object in `cited_by` contains:

| Field | Type | Description |
|-------|------|-------------|
| `doi` | string | DOI of the citing work |
| `provenance` | string | Highest-confidence provenance among this work's matches (`publisher` > `crossref` > `mined`) |
| `matches` | array | Reference matches from this citing work |

Each match contains:

| Field | Type | Description |
|-------|------|-------------|
| `provenance` | string | How this match was obtained (see below) |
| `raw_match` | string | The exact substring that matched an arXiv identifier pattern |
| `reference` | struct | The Crossref reference metadata (string-valued fields; absent keys are null) |

### Provenance Values

- `publisher` - the reference carries an explicit arXiv DOI with `doi-asserted-by: publisher`
- `crossref` - the reference carries an explicit arXiv DOI with `doi-asserted-by: crossref`
- `mined` - the arXiv identifier was extracted from unstructured reference text

The `asserted` config contains only `publisher` and `crossref` matches; the
`mined` config contains only `mined` matches. A citing work appears in a
config only with its matches of that config's provenances, and all counts are
recomputed per config.

## Extraction Process

arXiv references are extracted from the Crossref snapshot by scanning
reference metadata for arXiv identifiers in modern (`arXiv:2403.03542`),
legacy (`arXiv:cs.DM/9910013`), DOI (`10.48550/arXiv.2403.03542`), and URL
(`arxiv.org/abs/2403.03542`) forms. Identifiers are normalized (lowercase,
version suffixes removed). Extracted arXiv DOIs are validated against an
index of DOIs built from the DataCite monthly data file `{datacite_release}`;
only arXiv works registered in DataCite are included.

## Data Sources

- Crossref Metadata Plus monthly snapshot `{snapshot_id}`
- DataCite monthly data file `{datacite_release}`
- Extraction tools: [cometadata/crossref-arxiv-citation-extraction](https://github.com/cometadata/crossref-arxiv-citation-extraction)

## Limitations

- Only includes citations registered in Crossref metadata
- Reference parsing depends on successful identifier extraction from
  unstructured references
- The `mined` subset may contain lower-quality matches from noisy reference
  text

## License

This dataset is released under
[CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).

## Citation

```bibtex
@dataset{{crossref_arxiv_citations,
  title = {{Crossref arXiv Citations}},
  author = {{Cometadata}},
  publisher = {{Hugging Face}},
  url = {{https://huggingface.co/datasets/cometadata/crossref-arxiv-citations}}
}}
```
"""


def publish_dataset(
    *,
    api: Any,
    token: str | None,
    parquet: dict[str, Path],
    card: str,
    snapshot_id: str,
    events: Any,
) -> PublicationResult:
    from huggingface_hub import CommitOperationAdd

    for config in CONFIG_NAMES:
        path = parquet.get(config)
        if path is None or not path.is_file():
            raise PublishError(f"parquet file for config {config} is missing")

    operations = [
        CommitOperationAdd(
            path_in_repo="README.md", path_or_fileobj=card.encode("utf-8")
        )
    ]
    for config in CONFIG_NAMES:
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"data/{config}.parquet",
                path_or_fileobj=str(parquet[config]),
            )
        )

    with events.phase("publish_dataset"):
        try:
            parent = api.dataset_info(DATASET_REPO, token=token).sha
            commit = api.create_commit(
                repo_id=DATASET_REPO,
                repo_type="dataset",
                operations=operations,
                commit_message=f"Update from Crossref snapshot {snapshot_id}",
                parent_commit=parent,
                token=token,
            )
            api.dataset_info(DATASET_REPO, revision=commit.oid, token=token)
        except PublishError:
            raise
        except Exception as error:
            raise PublishError(f"dataset commit failed: {error}") from error
    events.emit("dataset_published", commit=commit.oid)
    return PublicationResult(commit_sha=commit.oid, commit_url=commit.commit_url)
