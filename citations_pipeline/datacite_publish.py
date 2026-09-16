"""Full-mode publication transform: the general DataCite citation dataset and
the arXiv compatibility projection from one confirmed-citation JSONL file.

Pipeline:

1. `flatten_valid_jsonl` reads the confirmed JSONL once (bounded batches) into
   a staging Parquet whose `reference` column is a string-valued map of the
   publishable keys, stringified with the legacy convention. Keys that cannot
   become struct fields are reported, never silently lost from the raw JSONL.
2. `scan_reference_keys` derives the struct keys of every destination and
   configuration from that staging table, so no scope gains keys that appear
   only outside it.
3. `widen_flat_references` rewrites the staging table into the flat table
   `target_doi, citing_doi, ref_index, provenance, match_type, raw_match,
   ref__<key>...`, hive-partitioned by a hash bucket of the target DOI.
4. Each destination/configuration runs the two-level DuckDB grouping over a
   `selected_references` view of the flat table (scoped by target DOI and
   provenance set). DuckDB's list aggregation and its sort of nested rows
   hold everything in memory, so the query runs per bucket range sized from
   a measured memory model against the DuckDB budget; a range that still
   exhausts memory is split and retried, and a single bucket that cannot fit
   fails explicitly. Range outputs are merged in sorted order with a
   fan-in-bounded multi-pass merge and streamed through
   `write_dataset_shards`, validated row by row and against the written
   files.
5. Ambiguous URL evidence is transformed independently, the same way, into
   the general dataset's `ambiguous_urls` configuration.
6. The arXiv projection is checked for exact equivalence with the arXiv DOI
   subset of the general result.

DuckDB runs with an explicit memory limit and spill directory; no
configuration is ever materialized in Python.
"""

from __future__ import annotations

import json
import shutil
from bisect import bisect_right
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .full_state import ResourceSettings
from .paths import (
    CONFIG_NAMES,
    CONFIG_PROVENANCES,
    DATACITE_DATASET_REPO,
    DATASET_REPO,
)
from .publish import ConfigStats, PublishError
from .publish_common import (
    REFERENCE_KEY_RE,
    quote_identifier,
    quote_path,
    stringify_value,
)

DATACITE_DESTINATION = "datacite"
ARXIV_DESTINATION = "arxiv"
DESTINATIONS = (DATACITE_DESTINATION, ARXIV_DESTINATION)
DESTINATION_REPOS = {
    DATACITE_DESTINATION: DATACITE_DATASET_REPO,
    ARXIV_DESTINATION: DATASET_REPO,
}
CITATION_CONFIGS = CONFIG_NAMES
AMBIGUITY_CONFIG = "ambiguous_urls"
DATACITE_CONFIGS = CITATION_CONFIGS + (AMBIGUITY_CONFIG,)
ARXIV_CONFIGS = CITATION_CONFIGS
PROVENANCES = CONFIG_PROVENANCES["all"]
MATCH_TYPES = ("doi", "arxiv", "url")
# Canonical (lowercase) arXiv DOI prefix selects the arXiv scope; the
# published projection renders the established mixed-case form.
ARXIV_SCOPE_PREFIX = "10.48550/arxiv."
ARXIV_DOI_PREFIX = "10.48550/arXiv."

# General configurations are sharded at 1 GiB of uncompressed Arrow data,
# closing a shard before the batch that would exceed it. Row groups are
# capped in bytes so readers never need a whole shard's worth of memory.
GENERAL_SHARD_TARGET_BYTES = 1 << 30
SHARD_ROW_GROUP_BYTES = 64 << 20
PARTITION_ROW_GROUP_BYTES = 8 << 20
FLAT_BATCH_ROWS = 50_000
FLAT_ROW_GROUP_ROWS = 8192
# DuckDB's partitioned COPY buffers rows for every open partition writer on
# every thread, so the flat tables are written on few threads (retrying on
# one) with a small flush threshold; it is I/O bound and stays fast.
COPY_THREAD_ATTEMPTS = (2, 1)
COPY_FLUSH_ROWS = 65_536
RESULT_BATCH_ROWS = 4096
MERGE_BATCH_ROWS = 1024

# Memory model of one bucket range's DuckDB query (list aggregation plus the
# sort of nested rows, neither of which spills). Measured on DuckDB 1.5.5:
# 4-12 KiB per flat row depending on reference size and thread count. The
# model is deliberately above that; a range is sized against
# `AGGREGATION_BUDGET_FRACTION` of the DuckDB limit, and an underestimate is
# caught by the split-and-retry on out-of-memory errors.
AGGREGATION_ROW_OVERHEAD_BYTES = 8 << 10
AGGREGATION_DATA_FACTOR = 10
AGGREGATION_BUDGET_FRACTION = 0.5
BUCKETS_PER_RANGE = 8
MIN_BUCKETS = 16
MAX_BUCKETS = 1024

_AMBIGUITY_FIELDS = (
    "citing_doi",
    "ref_index",
    "url",
    "raw_match",
    "candidate_dois",
    "reference",
)
_CITATION_KIND = "citations"
_AMBIGUITY_KIND = "ambiguity"


@dataclass(frozen=True)
class DatasetTransform:
    files: dict[str, Path]
    config_files: dict[str, tuple[str, ...]]
    stats: dict[str, ConfigStats]
    reference_keys: dict[str, tuple[str, ...]]
    dropped_reference_keys: tuple[str, ...]
    ambiguity_rows: int
    # Compressed on-disk size of every entry in `files`, for the production
    # size estimate and the publication manifest.
    sizes: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class FlattenResult:
    rows: int
    data_bytes: int
    keys: tuple[str, ...]
    dropped: dict[str, tuple[str, ...]]


# --- JSONL flattening ---------------------------------------------------------


def _is_index(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _reference_items(
    reference: Any,
    *,
    where: str,
    keys: set[str],
    dropped: set[str],
) -> tuple[list[tuple[str, str | None]], int]:
    """Publishable `(key, stringified value)` pairs of one reference object
    and their character count, recording unsafe keys in `dropped` and safe
    ones in `keys`."""
    if not isinstance(reference, dict):
        raise PublishError(f"reference must be a JSON object {where}")
    items: list[tuple[str, str | None]] = []
    size = 0
    for key, value in reference.items():
        if not REFERENCE_KEY_RE.fullmatch(key):
            dropped.add(key)
            continue
        keys.add(key)
        text = None if value is None else stringify_value(value)
        items.append((key, text))
        size += len(key) + (len(text) if text is not None else 0)
    return items, size


class _BatchedParquetWriter:
    """Accumulates Python column values and writes them as Parquet row groups
    of at most `batch_rows` rows."""

    def __init__(self, path: Path, schema: pa.Schema, batch_rows: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._schema = schema
        self._batch_rows = batch_rows
        self._columns: dict[str, list[Any]] = {name: [] for name in schema.names}
        self._pending = 0
        self.rows = 0
        self._writer = pq.ParquetWriter(path, schema, compression="zstd")

    def append(self, row: dict[str, Any]) -> None:
        for name in self._schema.names:
            self._columns[name].append(row[name])
        self._pending += 1
        self.rows += 1
        if self._pending >= self._batch_rows:
            self._flush()

    def _flush(self) -> None:
        if self._pending == 0:
            return
        batch = pa.record_batch(
            [
                pa.array(self._columns[field.name], type=field.type)
                for field in self._schema
            ],
            schema=self._schema,
        )
        self._writer.write_batch(batch)
        self._columns = {name: [] for name in self._schema.names}
        self._pending = 0

    def close(self) -> None:
        try:
            self._flush()
        finally:
            self._writer.close()


_REFERENCE_MAP = pa.map_(pa.string(), pa.string())
_FLAT_STAGING_SCHEMA = pa.schema(
    [
        pa.field("target_doi", pa.string()),
        pa.field("citing_doi", pa.string()),
        pa.field("ref_index", pa.int64()),
        pa.field("provenance", pa.string()),
        pa.field("match_type", pa.string()),
        pa.field("raw_match", pa.string()),
        pa.field("reference", _REFERENCE_MAP),
    ]
)
_AMBIGUITY_STAGING_SCHEMA = pa.schema(
    [
        pa.field("citing_doi", pa.string()),
        pa.field("ref_index", pa.int64()),
        pa.field("url", pa.string()),
        pa.field("raw_match", pa.string()),
        pa.field("candidate_dois", pa.list_(pa.string())),
        pa.field("reference", _REFERENCE_MAP),
    ]
)


def _numbered_records(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """`(line_number, record)` for every nonempty JSONL line; malformed JSON
    and non-object lines raise `PublishError` naming the line."""
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except ValueError as error:
                raise PublishError(
                    f"invalid JSON on line {line_number} of {path.name}"
                ) from error
            if not isinstance(record, dict):
                raise PublishError(
                    f"malformed record on line {line_number} of {path.name}"
                )
            yield line_number, record


def _valid_entry(entry: Any, where: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise PublishError(f"cited_by entry must be an object {where}")
    citing = entry.get("doi")
    ref_index = entry.get("ref_index")
    provenance = entry.get("provenance")
    match_type = entry.get("match_type")
    raw_match = entry.get("raw_match")
    if not isinstance(citing, str) or not citing:
        raise PublishError(f"cited_by entry doi must be a nonempty string {where}")
    if not _is_index(ref_index):
        raise PublishError(
            f"cited_by entry ref_index must be a non-negative integer {where}"
        )
    if provenance not in PROVENANCES:
        raise PublishError(
            f"invalid provenance {provenance!r} {where}; expected one of "
            f"{', '.join(PROVENANCES)}"
        )
    if match_type not in MATCH_TYPES:
        raise PublishError(
            f"invalid match_type {match_type!r} {where}; expected one of "
            f"{', '.join(MATCH_TYPES)}"
        )
    if not isinstance(raw_match, str):
        raise PublishError(f"cited_by entry raw_match must be a string {where}")
    if "reference" not in entry:
        raise PublishError(f"cited_by entry is missing its reference {where}")
    return {
        "citing_doi": citing,
        "ref_index": ref_index,
        "provenance": provenance,
        "match_type": match_type,
        "raw_match": raw_match,
    }


def flatten_valid_jsonl(valid_path: Path, staging_path: Path) -> FlattenResult:
    """Read the confirmed JSONL once into the staging table, validating every
    entry. Returns the row count, the decoded character count, the sorted
    union of publishable reference keys, and the dropped keys per
    destination."""
    keys: set[str] = set()
    dropped: dict[str, set[str]] = {destination: set() for destination in DESTINATIONS}
    data_bytes = 0
    writer = _BatchedParquetWriter(staging_path, _FLAT_STAGING_SCHEMA, FLAT_BATCH_ROWS)
    try:
        for line_number, record in _numbered_records(valid_path):
            where = f"on line {line_number} of {valid_path.name}"
            target = record.get("doi")
            cited_by = record.get("cited_by")
            if not isinstance(target, str) or not target:
                raise PublishError(f"doi must be a nonempty string {where}")
            if not isinstance(cited_by, list):
                raise PublishError(f"cited_by must be a list {where}")
            entry_dropped: set[str] = set()
            for entry in cited_by:
                row = _valid_entry(entry, where)
                row["target_doi"] = target
                row["reference"], size = _reference_items(
                    entry["reference"], where=where, keys=keys, dropped=entry_dropped
                )
                data_bytes += (
                    len(target) + len(row["citing_doi"]) + len(row["raw_match"]) + size
                )
                writer.append(row)
            dropped[DATACITE_DESTINATION] |= entry_dropped
            if target.startswith(ARXIV_SCOPE_PREFIX):
                dropped[ARXIV_DESTINATION] |= entry_dropped
    finally:
        writer.close()
    return FlattenResult(
        rows=writer.rows,
        data_bytes=data_bytes,
        keys=tuple(sorted(keys)),
        dropped={name: tuple(sorted(found)) for name, found in dropped.items()},
    )


def _ambiguity_row(record: dict[str, Any], where: str) -> dict[str, Any]:
    missing = [name for name in _AMBIGUITY_FIELDS if name not in record]
    if missing:
        raise PublishError(f"ambiguity row is missing {', '.join(missing)} {where}")
    for name in ("citing_doi", "url", "raw_match"):
        if not isinstance(record[name], str) or not record[name]:
            raise PublishError(f"ambiguity row {name} must be a nonempty string {where}")
    if not _is_index(record["ref_index"]):
        raise PublishError(
            f"ambiguity row ref_index must be a non-negative integer {where}"
        )
    candidates = record["candidate_dois"]
    if not isinstance(candidates, list) or not all(
        isinstance(item, str) and item for item in candidates
    ):
        raise PublishError(
            f"ambiguity row candidate_dois must be a list of strings {where}"
        )
    if len(candidates) < 2:
        raise PublishError(
            f"ambiguity row needs at least two candidate DOIs {where}"
        )
    if candidates != sorted(set(candidates)):
        raise PublishError(
            f"ambiguity row candidate_dois must be sorted and unique {where}"
        )
    return {
        "citing_doi": record["citing_doi"],
        "ref_index": record["ref_index"],
        "url": record["url"],
        "raw_match": record["raw_match"],
        "candidate_dois": list(candidates),
    }


def flatten_ambiguous_jsonl(ambiguous_path: Path, staging_path: Path) -> FlattenResult:
    """Read the ambiguity JSONL once into its staging table, validating every
    row: sorted, unique candidate lists with at least two members."""
    keys: set[str] = set()
    dropped: set[str] = set()
    data_bytes = 0
    writer = _BatchedParquetWriter(
        staging_path, _AMBIGUITY_STAGING_SCHEMA, FLAT_BATCH_ROWS
    )
    try:
        for line_number, record in _numbered_records(ambiguous_path):
            where = f"on line {line_number} of {ambiguous_path.name}"
            row = _ambiguity_row(record, where)
            row["reference"], size = _reference_items(
                record["reference"], where=where, keys=keys, dropped=dropped
            )
            data_bytes += (
                len(row["citing_doi"])
                + len(row["url"])
                + len(row["raw_match"])
                + sum(len(doi) for doi in row["candidate_dois"])
                + size
            )
            writer.append(row)
    finally:
        writer.close()
    return FlattenResult(
        rows=writer.rows,
        data_bytes=data_bytes,
        keys=tuple(sorted(keys)),
        dropped={DATACITE_DESTINATION: tuple(sorted(dropped))},
    )


# --- DuckDB session and flat tables ------------------------------------------


def open_duckdb(
    transform_dir: Path, resources: ResourceSettings
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB bounded by `resources.duckdb_memory_mb` that spills
    under `transform_dir/duckdb-tmp`."""
    spill = transform_dir / "duckdb-tmp"
    spill.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{resources.duckdb_memory_mb}MiB'")
    con.execute(f"SET temp_directory = {quote_path(spill)}")
    # Every published query carries its own ORDER BY; free DuckDB from
    # preserving scan order elsewhere so aggregation can use less memory.
    con.execute("SET preserve_insertion_order = false")
    # 64-bit Arrow offsets keep a batch of highly cited works exportable; the
    # Parquet files are written without stored Arrow schemas, so published
    # types are unaffected.
    con.execute("SET arrow_large_buffer_size = true")
    return con


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ref_column(key: str) -> str:
    return quote_identifier(f"ref__{key}")


def _hive_source(directory: Path) -> str:
    """`read_parquet` over a bucket-partitioned directory, exposing `bucket`."""
    return (
        f"read_parquet({quote_path(directory / '**' / '*.parquet')}, "
        "hive_partitioning = true)"
    )


def _map_bytes_sql(column: str) -> str:
    return (
        "coalesce(list_sum(list_transform(map_entries("
        f"{column}), e -> strlen(e.key) + strlen(coalesce(e.value, '')))), 0)"
    )


def _scope_predicate(destination: str) -> str:
    if destination == ARXIV_DESTINATION:
        return f"starts_with(target_doi, '{ARXIV_SCOPE_PREFIX}')"
    return "TRUE"


def _provenance_list(config: str) -> str:
    return ", ".join(f"'{name}'" for name in CONFIG_PROVENANCES[config])


def _selection_predicate(destination: str, config: str) -> str:
    return (
        f"provenance IN ({_provenance_list(config)}) AND "
        f"{_scope_predicate(destination)}"
    )


def scan_reference_keys(
    con: duckdb.DuckDBPyConnection, staging_path: Path
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Sorted reference keys observed in each `(destination, configuration)`
    scope of the staging table."""
    observed = con.execute(
        f"""
        SELECT DISTINCT starts_with(target_doi, '{ARXIV_SCOPE_PREFIX}') AS is_arxiv,
               provenance, key
        FROM (
          SELECT target_doi, provenance, unnest(map_keys(reference)) AS key
          FROM read_parquet({quote_path(staging_path)})
        )
        """
    ).fetchall()
    scopes: dict[tuple[str, str], set[str]] = {
        (destination, config): set()
        for destination in DESTINATIONS
        for config in CITATION_CONFIGS
    }
    for is_arxiv, provenance, key in observed:
        for destination in DESTINATIONS:
            if destination == ARXIV_DESTINATION and not is_arxiv:
                continue
            for config in CITATION_CONFIGS:
                if provenance in CONFIG_PROVENANCES[config]:
                    scopes[(destination, config)].add(key)
    return {scope: tuple(sorted(keys)) for scope, keys in scopes.items()}


def _execute_copy(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.execute(sql)


def _copy_partitioned(
    con: duckdb.DuckDBPyConnection, select_sql: str, directory: Path
) -> None:
    """Write `select_sql` as a `bucket`-partitioned Parquet directory within
    the DuckDB memory limit (see `COPY_THREAD_ATTEMPTS`)."""
    sql = f"""
        COPY ({select_sql}) TO {quote_path(directory)}
          (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bucket),
           ROW_GROUP_SIZE {FLAT_ROW_GROUP_ROWS})
        """
    (threads,) = con.execute("SELECT current_setting('threads')").fetchone()
    try:
        con.execute(f"SET partitioned_write_flush_threshold = {COPY_FLUSH_ROWS}")
        for attempt, copy_threads in enumerate(COPY_THREAD_ATTEMPTS):
            shutil.rmtree(directory, ignore_errors=True)
            con.execute(f"SET threads = {min(copy_threads, int(threads))}")
            try:
                _execute_copy(con, sql)
                return
            except duckdb.OutOfMemoryException as error:
                if attempt == len(COPY_THREAD_ATTEMPTS) - 1:
                    raise PublishError(
                        f"partitioning {directory.name} needs more than the DuckDB "
                        "memory limit even on one thread; raise duckdb_memory_mb"
                    ) from error
    finally:
        con.execute(f"SET threads = {int(threads)}")
        con.execute("RESET partitioned_write_flush_threshold")


def widen_flat_references(
    con: duckdb.DuckDBPyConnection,
    staging_path: Path,
    flat_dir: Path,
    keys: tuple[str, ...],
    buckets: int,
) -> None:
    """Rewrite the staging table as the flat table with one `ref__<key>`
    column per publishable key, a `row_bytes` size estimate, and hive
    partitions by `bucket = hash(target_doi) % buckets`."""
    columns = ["target_doi", "citing_doi", "ref_index", "provenance", "match_type", "raw_match"]
    columns.extend(
        f"map_extract_value(reference, {_literal(key)}) AS {_ref_column(key)}"
        for key in keys
    )
    columns.append(
        "(strlen(target_doi) + strlen(citing_doi) + strlen(raw_match) + "
        f"{_map_bytes_sql('reference')})::BIGINT AS row_bytes"
    )
    columns.append(f"hash(target_doi) % {buckets} AS bucket")
    _copy_partitioned(
        con,
        f"SELECT {', '.join(columns)} FROM read_parquet({quote_path(staging_path)})",
        flat_dir,
    )


def widen_ambiguity_rows(
    con: duckdb.DuckDBPyConnection,
    staging_path: Path,
    hive_dir: Path,
    keys: tuple[str, ...],
    buckets: int,
) -> None:
    """Rewrite the ambiguity staging table with its published `reference`
    struct, a `row_bytes` estimate, and hive partitions by
    `bucket = hash(citing_doi) % buckets`."""
    _copy_partitioned(
        con,
        f"""
        SELECT citing_doi, ref_index, url, raw_match, candidate_dois,
               {_reference_expression(keys, from_map=True)} AS reference,
               (strlen(citing_doi) + strlen(url) + strlen(raw_match)
                + coalesce(list_sum(list_transform(candidate_dois, d -> strlen(d))), 0)
                + {_map_bytes_sql('reference')})::BIGINT AS row_bytes,
               hash(citing_doi) % {buckets} AS bucket
        FROM read_parquet({quote_path(staging_path)})
        """,
        hive_dir,
    )


def check_flat_references(
    con: duckdb.DuckDBPyConnection, flat_dir: Path
) -> dict[str, int]:
    """Enforce the confirmed-match contract on the flat table: unique
    `(target, citer, ref_index)` tuples, no self-citations, and a nonempty
    scope for every destination. Returns the row count per destination."""
    flat = _hive_source(flat_dir)
    duplicates, self_citations, rows, arxiv_rows = con.execute(
        f"""
        SELECT
          (SELECT count(*) FROM (
             SELECT 1 FROM {flat}
             GROUP BY target_doi, citing_doi, ref_index HAVING count(*) > 1)),
          (SELECT count(*) FROM {flat} WHERE target_doi = citing_doi),
          (SELECT count(*) FROM {flat}),
          (SELECT count(*) FROM {flat}
             WHERE {_scope_predicate(ARXIV_DESTINATION)})
        """
    ).fetchone()
    if duplicates:
        raise PublishError(
            f"{duplicates} (cited, citing, ref_index) tuples appear more than once"
        )
    if self_citations:
        raise PublishError(f"{self_citations} self-citations in confirmed matches")
    counts = {DATACITE_DESTINATION: int(rows), ARXIV_DESTINATION: int(arxiv_rows)}
    for destination, count in counts.items():
        if count == 0:
            raise PublishError(f"no confirmed citations to publish for {destination}")
    return counts


# --- queries --------------------------------------------------------------------


def _reference_expression(keys: tuple[str, ...], *, from_map: bool = False) -> str:
    """`struct_pack` of the ordered reference keys, read from the flat table's
    `ref__<key>` columns or (staging tables) from the `reference` map."""
    if not keys:
        raise PublishError("no publishable reference keys in confirmed citations")
    fields = ", ".join(
        f"{quote_identifier(key)} := "
        + (
            f"map_extract_value(reference, {_literal(key)})"
            if from_map
            else _ref_column(key)
        )
        for key in keys
    )
    return f"struct_pack({fields})"


def citation_query(destination: str, config: str, keys: tuple[str, ...]) -> str:
    """The two-level grouping for one destination/configuration over the
    `selected_references` view (see `select_references`)."""
    match_fields = ["provenance := provenance"]
    if destination == DATACITE_DESTINATION:
        match_fields.append("match_type := match_type")
    match_fields.append("raw_match := raw_match")
    match_fields.append(f"reference := {_reference_expression(keys)}")
    if destination == ARXIV_DESTINATION:
        arxiv_id = f"substring(doi, {len(ARXIV_SCOPE_PREFIX) + 1})"
        projection = (
            f"'{ARXIV_DOI_PREFIX}' || {arxiv_id} AS arxiv_doi, "
            f"{arxiv_id} AS arxiv_id, citation_count, cited_by, reference_count"
        )
    else:
        projection = "doi, citation_count, cited_by, reference_count"
    return f"""
    WITH per_citer AS (
      SELECT target_doi, citing_doi,
             list(struct_pack({', '.join(match_fields)})
                  ORDER BY ref_index) AS matches,
             CASE WHEN bool_or(provenance = 'publisher') THEN 'publisher'
                  WHEN bool_or(provenance = 'crossref') THEN 'crossref'
                  ELSE 'mined' END AS provenance,
             count(*)::BIGINT AS reference_count
      FROM selected_references
      GROUP BY target_doi, citing_doi
    ),
    per_work AS (
      SELECT target_doi AS doi,
             count(*)::BIGINT AS citation_count,
             list(struct_pack(doi := citing_doi, matches := matches,
                              provenance := provenance)
                  ORDER BY citing_doi) AS cited_by,
             sum(reference_count)::BIGINT AS reference_count
      FROM per_citer
      GROUP BY target_doi
    )
    SELECT {projection}
    FROM per_work
    ORDER BY citation_count DESC, doi
    """


def select_references(
    con: duckdb.DuckDBPyConnection,
    flat_dir: Path,
    destination: str,
    config: str,
    buckets: tuple[int, int],
) -> None:
    """(Re)define the `selected_references` view: the flat table restricted
    to the destination's DOI scope, the configuration's provenances, and
    the inclusive bucket range."""
    lo, hi = buckets
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW selected_references AS
        SELECT * FROM {_hive_source(flat_dir)}
        WHERE {_selection_predicate(destination, config)}
          AND bucket BETWEEN {lo} AND {hi}
        """
    )


def ambiguity_query(hive_dir: Path, buckets: tuple[int, int]) -> str:
    """Distinct ambiguity rows of a bucket range in their published order."""
    lo, hi = buckets
    return f"""
    SELECT citing_doi, ref_index, url, raw_match, candidate_dois, reference
    FROM (
      SELECT DISTINCT citing_doi, ref_index, url, raw_match, candidate_dois, reference
      FROM {_hive_source(hive_dir)}
      WHERE bucket BETWEEN {lo} AND {hi}
    )
    ORDER BY citing_doi, ref_index, url
    """


def ambiguity_schema(keys: tuple[str, ...]) -> pa.Schema:
    """The `ambiguous_urls` schema, used for the empty configuration."""
    return pa.schema(
        [
            pa.field("citing_doi", pa.string()),
            pa.field("ref_index", pa.int64()),
            pa.field("url", pa.string()),
            pa.field("raw_match", pa.string()),
            pa.field("candidate_dois", pa.list_(pa.string())),
            pa.field("reference", pa.struct([pa.field(key, pa.string()) for key in keys])),
        ]
    )


def stream_query(con: duckdb.DuckDBPyConnection, sql: str) -> pa.RecordBatchReader:
    """Execute `sql` and stream its result as Arrow batches of
    `RESULT_BATCH_ROWS` rows."""
    con.execute(sql)
    if hasattr(con, "to_arrow_reader"):
        return con.to_arrow_reader(RESULT_BATCH_ROWS)
    return con.fetch_record_batch(RESULT_BATCH_ROWS)


def _is_out_of_memory(error: BaseException) -> bool:
    """DuckDB raises `OutOfMemoryException` from `execute`; the same failure
    while Arrow batches are being pulled surfaces as an `OSError`."""
    if isinstance(error, duckdb.OutOfMemoryException):
        return True
    return isinstance(error, OSError) and "Out of Memory" in str(error)


# --- batches, row groups, and shards -----------------------------------------


def validate_citation_batch(batch: pa.RecordBatch) -> tuple[int, int, int]:
    """Check every work of a citation batch: `citation_count == len(cited_by)`
    and `reference_count == sum(len(matches))`. Returns the batch's works,
    citations, and references."""
    citation_count = batch.column("citation_count")
    reference_count = batch.column("reference_count")
    cited_by = batch.column("cited_by")
    citers_per_work = pc.cast(pc.list_value_length(cited_by), pa.int64())
    citers = cited_by.flatten()
    matches_per_citer = pc.cast(
        pc.list_value_length(citers.field("matches")), pa.int64()
    )
    offsets = pc.cast(cited_by.offsets, pa.int64())
    offsets = pc.subtract(offsets, offsets[0])
    cumulative = pa.concat_arrays(
        [pa.array([0], pa.int64()), pc.cumulative_sum(matches_per_citer)]
    )
    references_per_work = pc.subtract(
        pc.take(cumulative, offsets.slice(1)),
        pc.take(cumulative, offsets.slice(0, len(offsets) - 1)),
    )
    bad = pc.or_(
        pc.not_equal(citers_per_work, citation_count),
        pc.not_equal(references_per_work, reference_count),
    )
    if pc.any(bad).as_py():
        index = pc.index(bad, True).as_py()
        raise PublishError(
            f"count invariant violated for {batch.column(0)[index].as_py()!r}: "
            f"citation_count={citation_count[index].as_py()} vs "
            f"{citers_per_work[index].as_py()} citing works, "
            f"reference_count={reference_count[index].as_py()} vs "
            f"{references_per_work[index].as_py()} matches"
        )
    return (
        batch.num_rows,
        pc.sum(citation_count).as_py() or 0,
        pc.sum(reference_count).as_py() or 0,
    )


def citation_keys(batch: pa.RecordBatch) -> list[tuple[int, str]]:
    """Per-row sort keys of a citation batch: `citation_count` descending,
    then the DOI (column 0; the arXiv rendering keeps the canonical order
    because every arXiv DOI shares the same prefix)."""
    counts = batch.column("citation_count").to_pylist()
    dois = batch.column(0).to_pylist()
    return [(-count, doi) for count, doi in zip(counts, dois)]


def ambiguity_keys(batch: pa.RecordBatch) -> list[tuple[str, int, str]]:
    return list(
        zip(
            batch.column("citing_doi").to_pylist(),
            batch.column("ref_index").to_pylist(),
            batch.column("url").to_pylist(),
        )
    )


class _Counter:
    """Wraps a batch reader, validating and totalling batches as they pass.
    Citation batches are checked per work; ambiguity batches must arrive
    with strictly increasing keys (a repeated key means two conflicting rows
    survived deduplication)."""

    def __init__(self, kind: str) -> None:
        self._kind = kind
        self._last_key: tuple[Any, ...] | None = None
        self.works = 0
        self.citations = 0
        self.references = 0

    def consume(self, batch: pa.RecordBatch) -> None:
        if self._kind == _CITATION_KIND:
            works, citations, references = validate_citation_batch(batch)
            self.citations += citations
            self.references += references
        else:
            keys = ambiguity_keys(batch)
            previous = self._last_key
            for key in keys:
                if previous is not None and key <= previous:
                    raise PublishError(
                        f"ambiguous URL key {key!r} carries conflicting rows"
                    )
                previous = key
            self._last_key = previous
            works = batch.num_rows
        self.works += works

    def wrap(self, reader: pa.RecordBatchReader) -> pa.RecordBatchReader:
        def batches() -> Iterator[pa.RecordBatch]:
            for batch in reader:
                self.consume(batch)
                yield batch

        return pa.RecordBatchReader.from_batches(reader.schema, batches())

    def stats(self) -> ConfigStats:
        return ConfigStats(
            works=self.works, citations=self.citations, references=self.references
        )


def _row_group_pieces(batch: pa.RecordBatch, row_group_bytes: int) -> Iterator[pa.RecordBatch]:
    """Split a batch at row boundaries into pieces of at most
    `row_group_bytes` of Arrow data; a single row is never split."""
    if batch.nbytes <= row_group_bytes or batch.num_rows <= 1:
        yield batch
        return
    half = batch.num_rows // 2
    yield from _row_group_pieces(batch.slice(0, half), row_group_bytes)
    yield from _row_group_pieces(batch.slice(half), row_group_bytes)


def _parquet_writer(path: Path, schema: pa.Schema) -> pq.ParquetWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    # No stored Arrow schema: the published types are exactly what the
    # Parquet schema declares, matching the established files.
    return pq.ParquetWriter(path, schema, compression="zstd", store_schema=False)


def _write_parquet(reader: pa.RecordBatchReader, path: Path, row_group_bytes: int) -> None:
    writer = _parquet_writer(path, reader.schema)
    try:
        for batch in reader:
            for piece in _row_group_pieces(batch, row_group_bytes):
                writer.write_batch(piece)
    finally:
        writer.close()


def write_dataset_shards(
    reader: pa.RecordBatchReader,
    out_dir: Path,
    repo_prefix: str,
    target_bytes: int,
    single_file: bool,
) -> dict[str, Path]:
    """Write the reader's batches in order to `<out_dir>/<repo_prefix>.parquet`
    (single file) or `<out_dir>/<repo_prefix>/part-NNNNN.parquet` shards of at
    most `target_bytes` uncompressed Arrow bytes each, with row groups of at
    most `SHARD_ROW_GROUP_BYTES`. A shard closes before the piece that would
    exceed the target; a single row above the target is an error, never
    dropped. An empty reader still produces one schema-bearing file. Returns
    repository-relative name to local path, in order."""
    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")
    schema = reader.schema
    files: dict[str, Path] = {}
    writer: pq.ParquetWriter | None = None
    shard_bytes = 0
    row_group_bytes = (
        SHARD_ROW_GROUP_BYTES if single_file else min(SHARD_ROW_GROUP_BYTES, target_bytes)
    )

    def open_shard() -> pq.ParquetWriter:
        if single_file:
            name = f"{repo_prefix}.parquet"
        else:
            name = f"{repo_prefix}/part-{len(files):05d}.parquet"
        path = out_dir / name
        files[name] = path
        return _parquet_writer(path, schema)

    try:
        for batch in reader:
            for piece in _row_group_pieces(batch, row_group_bytes):
                if not single_file and piece.nbytes > target_bytes:
                    raise PublishError(
                        f"row {piece.column(0)[0].as_py()!r} needs {piece.nbytes} "
                        f"bytes of Arrow data, above the shard target of "
                        f"{target_bytes} bytes"
                    )
                if (
                    writer is not None
                    and not single_file
                    and shard_bytes + piece.nbytes > target_bytes
                ):
                    writer.close()
                    writer = None
                    shard_bytes = 0
                if writer is None:
                    writer = open_shard()
                writer.write_batch(piece)
                shard_bytes += piece.nbytes
        if writer is None:
            writer = open_shard()
    finally:
        if writer is not None:
            writer.close()
    return files


def _remove_output(out_dir: Path, repo_prefix: str, single_file: bool) -> None:
    if single_file:
        (out_dir / f"{repo_prefix}.parquet").unlink(missing_ok=True)
    else:
        shutil.rmtree(out_dir / repo_prefix, ignore_errors=True)


# --- bucket ranges and the memory model ---------------------------------------


def aggregation_budget(resources: ResourceSettings) -> int:
    """Bytes one bucket range's query may need under the DuckDB limit."""
    return int(resources.duckdb_memory_mb * (1 << 20) * AGGREGATION_BUDGET_FRACTION)


def aggregation_estimate(rows: int, data_bytes: int) -> int:
    """Modelled DuckDB memory for aggregating and sorting `rows` flat rows
    holding `data_bytes` of string data."""
    return rows * AGGREGATION_ROW_OVERHEAD_BYTES + data_bytes * AGGREGATION_DATA_FACTOR


def plan_bucket_count(rows: int, data_bytes: int, budget: int) -> int:
    """Hash buckets for a flat table so that about `BUCKETS_PER_RANGE` buckets
    fit one budget, bounded to keep partition counts practical."""
    per_bucket = max(budget // BUCKETS_PER_RANGE, 1)
    wanted = -(-aggregation_estimate(rows, data_bytes) // per_bucket)
    return min(max(wanted, MIN_BUCKETS), MAX_BUCKETS)


def bucket_stats(
    con: duckdb.DuckDBPyConnection, source: str, predicate: str
) -> dict[int, tuple[int, int]]:
    """`bucket -> (rows, data bytes)` of the rows matching `predicate`."""
    observed = con.execute(
        f"SELECT bucket, count(*), sum(row_bytes) FROM {source}"
        f" WHERE {predicate} GROUP BY bucket"
    ).fetchall()
    return {int(bucket): (int(rows), int(size)) for bucket, rows, size in observed}


def plan_ranges(
    stats: dict[int, tuple[int, int]], bucket_count: int, budget: int
) -> list[tuple[int, int]]:
    """Inclusive bucket ranges covering `0..bucket_count-1` in order, each
    estimated to fit `budget`; a single bucket above the budget still forms
    its own range (its query fails explicitly if it cannot run)."""
    ranges: list[tuple[int, int]] = []
    start = 0
    used = 0
    for bucket in range(bucket_count):
        rows, size = stats.get(bucket, (0, 0))
        estimate = aggregation_estimate(rows, size)
        if used + estimate > budget and bucket > start:
            ranges.append((start, bucket - 1))
            start, used = bucket, 0
        used += estimate
    ranges.append((start, bucket_count - 1))
    return ranges


# --- sorted merge of range outputs -----------------------------------------


class _SortedSource:
    """One exhausted-on-demand stream of sorted batches with its keys."""

    __slots__ = ("_batches", "_key_of", "batch", "keys")

    def __init__(
        self,
        batches: Iterator[pa.RecordBatch],
        key_of: Callable[[pa.RecordBatch], list[Any]],
    ) -> None:
        self._batches = batches
        self._key_of = key_of
        self.batch: pa.RecordBatch | None = None
        self.keys: list[Any] = []

    def refill(self) -> bool:
        while not self.keys:
            batch = next(self._batches, None)
            if batch is None:
                self.batch = None
                return False
            self.batch = batch
            self.keys = self._key_of(batch)
        return True

    def take(self, count: int) -> pa.RecordBatch:
        assert self.batch is not None
        piece = self.batch.slice(0, count)
        self.batch = self.batch.slice(count)
        self.keys = self.keys[count:]
        return piece


def merge_sorted_batches(
    sources: Sequence[Iterator[pa.RecordBatch]],
    key_of: Callable[[pa.RecordBatch], list[Any]],
) -> Iterator[pa.RecordBatch]:
    """Merge streams that are each sorted by `key_of` into one sorted stream.
    Every step emits all buffered rows up to the smallest last key among the
    current batches, so at most one batch per source is resident."""
    live = [
        source
        for source in (_SortedSource(iter(batches), key_of) for batches in sources)
        if source.refill()
    ]
    while live:
        bound = min(source.keys[-1] for source in live)
        pieces: list[pa.RecordBatch] = []
        keys: list[Any] = []
        for source in live:
            count = bisect_right(source.keys, bound)
            if count:
                keys.extend(source.keys[:count])
                pieces.append(source.take(count))
        live = [source for source in live if source.refill()]
        chunk = pa.concat_batches(pieces)
        order = sorted(range(len(keys)), key=keys.__getitem__)
        if order != list(range(len(keys))):
            chunk = chunk.take(pa.array(order, pa.int64()))
        yield chunk


def _iter_merged(
    files: Sequence[Path], key_of: Callable[[pa.RecordBatch], list[Any]]
) -> Iterator[pa.RecordBatch]:
    handles = [pq.ParquetFile(path) for path in files]
    try:
        yield from merge_sorted_batches(
            [handle.iter_batches(batch_size=MERGE_BATCH_ROWS) for handle in handles],
            key_of,
        )
    finally:
        for handle in handles:
            handle.close()


def _merged_reader(
    files: list[Path],
    key_of: Callable[[pa.RecordBatch], list[Any]],
    fan_in: int,
    work_dir: Path,
) -> pa.RecordBatchReader:
    """A sorted stream over sorted `files`, merging at most `fan_in` at a time
    (earlier passes write intermediate files and remove their inputs)."""
    passes = 0
    while len(files) > fan_in:
        merged: list[Path] = []
        for index in range(0, len(files), fan_in):
            group = files[index : index + fan_in]
            out = work_dir / f"merge-{passes:02d}-{index // fan_in:05d}.parquet"
            _write_parquet(
                pa.RecordBatchReader.from_batches(
                    pq.read_schema(group[0]), _iter_merged(group, key_of)
                ),
                out,
                PARTITION_ROW_GROUP_BYTES,
            )
            for path in group:
                path.unlink()
            merged.append(out)
        files = merged
        passes += 1
    return pa.RecordBatchReader.from_batches(
        pq.read_schema(files[0]), _iter_merged(files, key_of)
    )


# --- building one sorted configuration ----------------------------------------


@dataclass(frozen=True)
class SortedOutput:
    """How to produce one sorted configuration: its label, a function that
    prepares and returns the SQL for an inclusive bucket range, the per-row
    sort key of its batches, and its kind (`citations` or `ambiguity`)."""

    label: str
    sql_for: Callable[[duckdb.DuckDBPyConnection, int, int], str]
    key_of: Callable[[pa.RecordBatch], list[Any]]
    kind: str


def _citation_output(
    flat_dir: Path, destination: str, config: str, keys: tuple[str, ...]
) -> SortedOutput:
    sql = citation_query(destination, config, keys)

    def sql_for(con: duckdb.DuckDBPyConnection, lo: int, hi: int) -> str:
        select_references(con, flat_dir, destination, config, (lo, hi))
        return sql

    return SortedOutput(
        label=f"{destination}/{config}",
        sql_for=sql_for,
        key_of=citation_keys,
        kind=_CITATION_KIND,
    )


def _ambiguity_output(hive_dir: Path) -> SortedOutput:
    return SortedOutput(
        label=f"{DATACITE_DESTINATION}/{AMBIGUITY_CONFIG}",
        sql_for=lambda _con, lo, hi: ambiguity_query(hive_dir, (lo, hi)),
        key_of=ambiguity_keys,
        kind=_AMBIGUITY_KIND,
    )


def _memory_error(output: SortedOutput, bucket: int) -> PublishError:
    return PublishError(
        f"{output.label}: bucket {bucket} cannot be aggregated within the DuckDB "
        "memory limit; raise duckdb_memory_mb"
    )


def _write_range(
    con: duckdb.DuckDBPyConnection,
    output: SortedOutput,
    lo: int,
    hi: int,
    work_dir: Path,
    files: list[Path],
    events: Any,
) -> None:
    """Run the query for buckets `lo..hi` into one sorted partition file,
    splitting the range and retrying when DuckDB runs out of memory."""
    path = work_dir / f"range-{lo:05d}-{hi:05d}.parquet"
    try:
        _write_parquet(
            stream_query(con, output.sql_for(con, lo, hi)), path, PARTITION_ROW_GROUP_BYTES
        )
    except Exception as error:
        if not _is_out_of_memory(error):
            raise
        path.unlink(missing_ok=True)
        if lo == hi:
            raise _memory_error(output, lo) from error
        mid = (lo + hi) // 2
        events.emit(
            "configuration_memory_split", configuration=output.label, buckets=[lo, hi]
        )
        _write_range(con, output, lo, mid, work_dir, files, events)
        _write_range(con, output, mid + 1, hi, work_dir, files, events)
        return
    files.append(path)


def build_sorted_output(
    con: duckdb.DuckDBPyConnection,
    output: SortedOutput,
    ranges: list[tuple[int, int]],
    *,
    out_dir: Path,
    repo_prefix: str,
    single_file: bool,
    target_bytes: int,
    work_dir: Path,
    fan_in: int,
    events: Any,
) -> tuple[dict[str, Path], _Counter]:
    """Stream one configuration into its published files. A single range is
    streamed straight from DuckDB; several ranges (or a single range that
    exhausts memory) go through sorted partition files and a merge."""
    counter = _Counter(output.kind)
    if len(ranges) == 1:
        lo, hi = ranges[0]
        try:
            reader = counter.wrap(stream_query(con, output.sql_for(con, lo, hi)))
            files = write_dataset_shards(reader, out_dir, repo_prefix, target_bytes, single_file)
            return files, counter
        except Exception as error:
            if not _is_out_of_memory(error):
                raise
            _remove_output(out_dir, repo_prefix, single_file)
            if lo == hi:
                raise _memory_error(output, lo) from error
            events.emit(
                "configuration_memory_split", configuration=output.label, buckets=[lo, hi]
            )
            mid = (lo + hi) // 2
            ranges = [(lo, mid), (mid + 1, hi)]
            counter = _Counter(output.kind)

    partition_dir = work_dir / output.label.replace("/", "-")
    shutil.rmtree(partition_dir, ignore_errors=True)
    partition_dir.mkdir(parents=True)
    partition_files: list[Path] = []
    for lo, hi in ranges:
        _write_range(con, output, lo, hi, partition_dir, partition_files, events)
    events.emit(
        "configuration_partitioned",
        configuration=output.label,
        partitions=len(partition_files),
        fan_in=fan_in,
    )
    reader = counter.wrap(_merged_reader(partition_files, output.key_of, fan_in, partition_dir))
    files = write_dataset_shards(reader, out_dir, repo_prefix, target_bytes, single_file)
    shutil.rmtree(partition_dir)
    return files, counter


# --- validation -----------------------------------------------------------------


def _path_list(paths: Sequence[Path]) -> str:
    return "[" + ", ".join(quote_path(path) for path in paths) + "]"


def _check_written_totals(
    con: duckdb.DuckDBPyConnection,
    files: dict[str, Path],
    expected: ConfigStats,
    label: str,
) -> None:
    works, citations, references = con.execute(
        "SELECT count(*), coalesce(sum(citation_count), 0),"
        " coalesce(sum(reference_count), 0)"
        f" FROM read_parquet({_path_list(list(files.values()))})"
    ).fetchone()
    written = ConfigStats(
        works=int(works), citations=int(citations), references=int(references)
    )
    if written != expected:
        raise PublishError(
            f"{label} files hold {written} but the transform counted {expected}"
        )


def _check_written_rows(
    con: duckdb.DuckDBPyConnection, files: dict[str, Path], expected: int, label: str
) -> None:
    (written,) = con.execute(
        f"SELECT count(*) FROM read_parquet({_path_list(list(files.values()))})"
    ).fetchone()
    if int(written) != expected:
        raise PublishError(
            f"{label} files hold {written} rows but the transform counted {expected}"
        )


# Per-work digest of the citing works: DOI, strongest provenance, and match
# count. Hashing keeps nested data out of the validation join.
_CITER_DIGEST = (
    "hash(list_transform(cited_by, c -> struct_pack(doi := c.doi, "
    "provenance := c.provenance, matches := len(c.matches))))"
)


def validate_arxiv_projection(
    con: duckdb.DuckDBPyConnection,
    general_files: list[Path],
    arxiv_file: Path,
    expected: ConfigStats,
    config: str,
) -> None:
    """The arXiv projection must equal the arXiv DOI subset of the general
    configuration: same works, per-work counts, citing works with their
    provenance and match counts, and the rendered DOI form."""
    row = con.execute(
        f"""
        WITH general AS (
          SELECT doi, citation_count, reference_count, {_CITER_DIGEST} AS citers
          FROM read_parquet({_path_list(general_files)})
          WHERE starts_with(doi, '{ARXIV_SCOPE_PREFIX}')
        ),
        projected AS (
          SELECT '{ARXIV_SCOPE_PREFIX}' || arxiv_id AS doi, arxiv_doi, arxiv_id,
                 citation_count, reference_count, {_CITER_DIGEST} AS citers
          FROM read_parquet({quote_path(arxiv_file)})
        )
        SELECT
          count(*) FILTER (WHERE p.doi IS NULL) AS missing,
          count(*) FILTER (WHERE g.doi IS NULL) AS extra,
          count(*) FILTER (WHERE g.doi IS NOT NULL AND p.doi IS NOT NULL AND (
            g.citation_count != p.citation_count
            OR g.reference_count != p.reference_count
            OR g.citers != p.citers)) AS mismatched,
          count(*) FILTER (WHERE p.doi IS NOT NULL
            AND p.arxiv_doi != '{ARXIV_DOI_PREFIX}' || p.arxiv_id) AS misrendered,
          count(g.doi), coalesce(sum(g.citation_count), 0),
          coalesce(sum(g.reference_count), 0)
        FROM general g FULL OUTER JOIN projected p ON g.doi = p.doi
        """
    ).fetchone()
    missing, extra, mismatched, misrendered, works, citations, references = row
    subset = ConfigStats(
        works=int(works), citations=int(citations), references=int(references)
    )
    if missing or extra or mismatched or misrendered or subset != expected:
        raise PublishError(
            f"arXiv projection of {config} differs from the general arXiv subset: "
            f"{missing} works missing, {extra} extra, {mismatched} mismatched, "
            f"{misrendered} misrendered; subset {subset} vs projection {expected}"
        )


def _check_partition_invariant(destination: str, stats: dict[str, ConfigStats]) -> None:
    expected = stats["asserted"].references + stats["mined"].references
    if stats["all"].references != expected:
        raise PublishError(
            f"{destination} reference partition invariant violated: "
            f"all={stats['all'].references} != asserted+mined={expected}"
        )


def _config_keys(
    scope_keys: dict[tuple[str, str], tuple[str, ...]], destination: str, config: str
) -> tuple[str, ...]:
    """A configuration's struct keys; an empty scope borrows the destination's
    `all` keys, then the general `all` keys, so its file still declares the
    reference schema."""
    for scope in (
        (destination, config),
        (destination, "all"),
        (DATACITE_DESTINATION, "all"),
    ):
        if scope_keys[scope]:
            return scope_keys[scope]
    raise PublishError("no publishable reference keys in confirmed citations")


# --- the transform ---------------------------------------------------------------


def _emit_plan(
    events: Any,
    label: str,
    stats: dict[int, tuple[int, int]],
    ranges: list[tuple[int, int]],
    budget: int,
) -> None:
    rows = sum(count for count, _ in stats.values())
    size = sum(size for _, size in stats.values())
    events.emit(
        "configuration_planned",
        configuration=label,
        rows=rows,
        data_bytes=size,
        estimated_bytes=aggregation_estimate(rows, size),
        budget_bytes=budget,
        buckets=len(stats),
        ranges=len(ranges),
    )


def _emit_built(
    events: Any, label: str, written: dict[str, Path], **counts: int
) -> None:
    events.emit(
        "configuration_built",
        configuration=label,
        files=len(written),
        bytes=sum(path.stat().st_size for path in written.values()),
        **counts,
    )


def transform_datacite_outputs(
    valid_path: Path,
    ambiguous_path: Path,
    transform_dir: Path,
    events: Any,
    resources: ResourceSettings,
) -> dict[str, DatasetTransform]:
    resources.validate()
    budget = aggregation_budget(resources)
    transform_dir.mkdir(parents=True, exist_ok=True)
    references_staging = transform_dir / "references-staging.parquet"
    ambiguity_staging = transform_dir / "ambiguity-staging.parquet"
    flat_dir = transform_dir / "flat"
    ambiguity_dir = transform_dir / "ambiguity"
    partitions_dir = transform_dir / "partitions"

    with events.phase("flatten_valid_output"):
        flattened = flatten_valid_jsonl(valid_path, references_staging)
    if flattened.rows == 0:
        raise PublishError("no confirmed citations to publish")
    with events.phase("flatten_ambiguous_urls"):
        ambiguity = flatten_ambiguous_jsonl(ambiguous_path, ambiguity_staging)

    files: dict[str, dict[str, Path]] = {name: {} for name in DESTINATIONS}
    config_files: dict[str, dict[str, tuple[str, ...]]] = {name: {} for name in DESTINATIONS}
    stats: dict[str, dict[str, ConfigStats]] = {name: {} for name in DESTINATIONS}
    reference_keys: dict[str, dict[str, tuple[str, ...]]] = {name: {} for name in DESTINATIONS}

    con = open_duckdb(transform_dir, resources)
    try:
        with events.phase("scan_reference_keys"):
            scope_keys = scan_reference_keys(con, references_staging)
            scope_keys[(DATACITE_DESTINATION, AMBIGUITY_CONFIG)] = ambiguity.keys
        buckets = plan_bucket_count(flattened.rows, flattened.data_bytes, budget)
        with events.phase("widen_flat_references"):
            widen_flat_references(con, references_staging, flat_dir, flattened.keys, buckets)
            references_staging.unlink()
        with events.phase("check_flat_references"):
            scope_rows = check_flat_references(con, flat_dir)

        flat_source = _hive_source(flat_dir)
        for destination in DESTINATIONS:
            single_file = destination == ARXIV_DESTINATION
            for config in CITATION_CONFIGS:
                keys = _config_keys(scope_keys, destination, config)
                output = _citation_output(flat_dir, destination, config, keys)
                with events.phase(f"build_{destination}_{config}"):
                    observed = bucket_stats(
                        con, flat_source, _selection_predicate(destination, config)
                    )
                    ranges = plan_ranges(observed, buckets, budget)
                    _emit_plan(events, output.label, observed, ranges, budget)
                    written, counter = build_sorted_output(
                        con,
                        output,
                        ranges,
                        out_dir=transform_dir / destination,
                        repo_prefix=f"data/{config}",
                        single_file=single_file,
                        target_bytes=GENERAL_SHARD_TARGET_BYTES,
                        work_dir=partitions_dir,
                        fan_in=resources.merge_fan_in,
                        events=events,
                    )
                    config_stats = counter.stats()
                    _check_written_totals(con, written, config_stats, output.label)
                files[destination].update(written)
                config_files[destination][config] = tuple(written)
                stats[destination][config] = config_stats
                reference_keys[destination][config] = keys
                _emit_built(
                    events,
                    output.label,
                    written,
                    works=config_stats.works,
                    citations=config_stats.citations,
                    references=config_stats.references,
                )
            _check_partition_invariant(destination, stats[destination])

        ambiguity_keys_ = _config_keys(scope_keys, DATACITE_DESTINATION, AMBIGUITY_CONFIG)
        output = _ambiguity_output(ambiguity_dir)
        with events.phase(f"build_{DATACITE_DESTINATION}_{AMBIGUITY_CONFIG}"):
            if ambiguity.rows == 0:
                written = write_dataset_shards(
                    pa.RecordBatchReader.from_batches(
                        ambiguity_schema(ambiguity_keys_), iter(())
                    ),
                    transform_dir / DATACITE_DESTINATION,
                    f"data/{AMBIGUITY_CONFIG}",
                    GENERAL_SHARD_TARGET_BYTES,
                    single_file=False,
                )
                ambiguity_rows = 0
            else:
                ambiguity_buckets = plan_bucket_count(
                    ambiguity.rows, ambiguity.data_bytes, budget
                )
                widen_ambiguity_rows(
                    con, ambiguity_staging, ambiguity_dir, ambiguity_keys_, ambiguity_buckets
                )
                observed = bucket_stats(con, _hive_source(ambiguity_dir), "TRUE")
                ranges = plan_ranges(observed, ambiguity_buckets, budget)
                _emit_plan(events, output.label, observed, ranges, budget)
                written, counter = build_sorted_output(
                    con,
                    output,
                    ranges,
                    out_dir=transform_dir / DATACITE_DESTINATION,
                    repo_prefix=f"data/{AMBIGUITY_CONFIG}",
                    single_file=False,
                    target_bytes=GENERAL_SHARD_TARGET_BYTES,
                    work_dir=partitions_dir,
                    fan_in=resources.merge_fan_in,
                    events=events,
                )
                ambiguity_rows = counter.works
                _check_written_rows(con, written, ambiguity_rows, output.label)
        files[DATACITE_DESTINATION].update(written)
        config_files[DATACITE_DESTINATION][AMBIGUITY_CONFIG] = tuple(written)
        reference_keys[DATACITE_DESTINATION][AMBIGUITY_CONFIG] = ambiguity_keys_
        _emit_built(
            events,
            output.label,
            written,
            rows=ambiguity_rows,
            duplicates_removed=ambiguity.rows - ambiguity_rows,
        )

        with events.phase("validate_arxiv_projection"):
            for config in CITATION_CONFIGS:
                validate_arxiv_projection(
                    con,
                    [
                        files[DATACITE_DESTINATION][name]
                        for name in config_files[DATACITE_DESTINATION][config]
                    ],
                    files[ARXIV_DESTINATION][f"data/{config}.parquet"],
                    stats[ARXIV_DESTINATION][config],
                    config,
                )
    finally:
        con.close()
    shutil.rmtree(flat_dir, ignore_errors=True)
    shutil.rmtree(ambiguity_dir, ignore_errors=True)
    shutil.rmtree(partitions_dir, ignore_errors=True)
    ambiguity_staging.unlink(missing_ok=True)

    dropped = {
        DATACITE_DESTINATION: tuple(
            sorted(
                set(flattened.dropped[DATACITE_DESTINATION])
                | set(ambiguity.dropped[DATACITE_DESTINATION])
            )
        ),
        ARXIV_DESTINATION: flattened.dropped[ARXIV_DESTINATION],
    }
    result = {
        destination: DatasetTransform(
            files=files[destination],
            config_files=config_files[destination],
            stats=stats[destination],
            reference_keys=reference_keys[destination],
            dropped_reference_keys=dropped[destination],
            ambiguity_rows=ambiguity_rows if destination == DATACITE_DESTINATION else 0,
            sizes={
                name: path.stat().st_size for name, path in files[destination].items()
            },
        )
        for destination in DESTINATIONS
    }
    events.emit(
        "transform_completed",
        rows=flattened.rows,
        arxiv_rows=scope_rows[ARXIV_DESTINATION],
        ambiguity_rows=ambiguity_rows,
        buckets=buckets,
        **{
            f"{destination}_{config}_works": stats[destination][config].works
            for destination in DESTINATIONS
            for config in CITATION_CONFIGS
        },
        **{
            f"{destination}_bytes": sum(result[destination].sizes.values())
            for destination in DESTINATIONS
        },
        **{
            f"{destination}_dropped_reference_keys": list(dropped[destination])
            for destination in DESTINATIONS
        },
        datasets={name: DESTINATION_REPOS[name] for name in DESTINATIONS},
    )
    return result
