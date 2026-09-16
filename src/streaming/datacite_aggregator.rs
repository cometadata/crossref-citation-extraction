//! Full-mode (DataCite) aggregation: bounded, replay-safe aggregation of
//! hashed partitions into grouped JSONL outputs.
//!
//! Each partition is deduplicated on `(cited_id, citing_doi, ref_index)` with
//! the evidence order in [`super::kway_merge`], processed in memory when its
//! Parquet metadata estimate fits the budget and through bounded external
//! sort otherwise, validated against the DataCite index, and committed
//! through [`super::partition_commit`]. Final outputs are assembled from the
//! commits through temporary files and renamed into place, never appended.
//! The legacy arXiv aggregator lives in [`super::aggregator`]; the two share
//! only [`AggregationStats`] and the merge chunk ceiling.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use log::{debug, info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use serde_json::value::RawValue;

use super::aggregator::{AggregationStats, STREAM_CHUNK_SIZE};
use super::kway_merge::{compare_rows, get_parquet_row_count, merge_sorted_segments, MergeRow};
use super::limits::AggregationLimits;
use super::partition_commit::{
    aggregation_dir, hit_failpoint, remove_stale_staging, sync_parent, tmp_output_path,
    PartitionCommit, PartitionStaging, FAILPOINT_DURING_FINAL_ASSEMBLY,
};
use super::partitioning::PartitionMode;
use super::segmented_writer::{committed_segments, read_segment, MATCH_TYPE_COLUMN};
use crate::common::OutputPaths;
use crate::index::datacite::{DataciteIndex, TargetLookup};

/// Bytes charged per decoded row beyond its Parquet-encoded bytes: the
/// `MergeRow` slot, allocator overhead for five strings, and an allowance
/// for the identifier columns, which dictionary encoding stores once per
/// segment rather than once per row.
const ROW_OVERHEAD_BYTES: usize = 336;

/// Fewest rows a merge cursor reads at a time; below this the per-read
/// Parquet scan cost dominates.
const MERGE_CHUNK_MIN_ROWS: usize = 64;

/// The schema rule for reading full-mode segments and sorted runs: every
/// file carries `match_type`. The reader looks only at the variant; the
/// partition count plays no part in decoding a row.
const FULL_SCHEMA: PartitionMode = PartitionMode::Hashed { count: 1 };

/// Scratch subdirectory for sorted runs inside a staging directory.
const RUNS_DIR: &str = "runs";

/// Fragment file name for an output kind.
fn fragment_name(kind: &str) -> String {
    format!("{kind}.jsonl")
}

/// Every output kind with its target path, if enabled.
fn output_targets(outputs: &OutputPaths) -> [(&'static str, Option<&Path>); 5] {
    [
        ("valid", outputs.valid.as_deref()),
        ("failed", outputs.failed.as_deref()),
        ("publisher", outputs.publisher.as_deref()),
        ("crossref", outputs.crossref.as_deref()),
        ("mined", outputs.mined.as_deref()),
    ]
}

/// Row count and estimated decoded bytes of one segment, from its footer.
#[derive(Debug, Clone, Copy)]
struct SegmentEstimate {
    rows: usize,
    bytes: usize,
}

/// Estimate a segment's decoded size from Parquet metadata: the stored
/// (uncompressed) byte size of every row group plus a per-row overhead.
fn estimate_segment(path: &Path) -> Result<SegmentEstimate> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let reader = SerializedFileReader::new(file).map_err(|e| {
        anyhow::anyhow!("Failed to read parquet metadata of {}: {e}", path.display())
    })?;
    let metadata = reader.metadata();
    let rows = usize::try_from(metadata.file_metadata().num_rows())
        .with_context(|| format!("negative row count in {}", path.display()))?;
    let encoded: i64 = metadata
        .row_groups()
        .iter()
        .map(|group| group.total_byte_size().max(0))
        .sum();
    let bytes = usize::try_from(encoded)
        .unwrap_or(usize::MAX)
        .saturating_add(rows.saturating_mul(ROW_OVERHEAD_BYTES));
    Ok(SegmentEstimate { rows, bytes })
}

/// Partition directories under `partition_dir`, in name order. Names
/// starting with `_` or `.` are bookkeeping, not partitions.
fn list_partitions(partition_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut partitions = Vec::new();
    for entry in fs::read_dir(partition_dir)
        .with_context(|| format!("Failed to list {}", partition_dir.display()))?
    {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .with_context(|| format!("partition name is not UTF-8: {}", path.display()))?;
        if name.starts_with('_') || name.starts_with('.') {
            continue;
        }
        partitions.push((name.to_owned(), path));
    }
    partitions.sort();
    Ok(partitions)
}

/// A committed partition must hold a fragment for every enabled output
/// kind it counted rows for; otherwise it was aggregated under a different
/// output selection and cannot be reused.
fn check_committed_fragments(commit: &PartitionCommit, outputs: &OutputPaths) -> Result<()> {
    let stats = &commit.stats;
    let counts = [
        stats.valid_count,
        stats.failed_count,
        stats.publisher_citations,
        stats.crossref_citations,
        stats.mined_citations,
    ];
    for ((kind, target), count) in output_targets(outputs).iter().zip(counts) {
        if target.is_some() && count > 0 {
            ensure!(
                commit.files.contains_key(&fragment_name(kind)),
                "committed partition {} has no {kind} fragment although it counted {count} rows; \
                 it was aggregated with a different output selection, rerun without resume",
                commit.partition
            );
        }
    }
    Ok(())
}

/// A `cited_by` entry in full mode: the legacy fields plus the reference
/// index and evidence kind. The original reference is copied through as
/// raw JSON, or `null` when the stored text is not valid JSON.
#[derive(serde::Serialize)]
struct FullEntryOut<'a> {
    doi: &'a str,
    ref_index: u32,
    provenance: &'a str,
    match_type: &'static str,
    raw_match: &'a str,
    reference: Option<&'a RawValue>,
}

impl<'a> FullEntryOut<'a> {
    fn from_row(row: &'a MergeRow) -> Self {
        Self {
            doi: &row.citing_doi,
            ref_index: row.ref_index,
            provenance: &row.provenance,
            match_type: row.match_type.as_str(),
            raw_match: &row.raw_match,
            reference: serde_json::from_str::<&RawValue>(&row.ref_json).ok(),
        }
    }
}

#[derive(serde::Serialize)]
struct FullRecordOut<'a> {
    doi: &'a str,
    citation_count: usize,
    cited_by: Vec<&'a FullEntryOut<'a>>,
}

fn write_full_record<W: Write>(
    writer: &mut W,
    doi: &str,
    cited_by: Vec<&FullEntryOut<'_>>,
) -> Result<()> {
    let record = FullRecordOut {
        doi,
        citation_count: cited_by.len(),
        cited_by,
    };
    serde_json::to_writer(&mut *writer, &record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Emit one cited work's deduplicated rows (sorted by citing DOI and
/// reference index) to the partition's fragments and count it.
fn emit_group(
    cited_id: &str,
    rows: &[MergeRow],
    index: &DataciteIndex,
    outputs: &OutputPaths,
    staging: &mut PartitionStaging,
    stats: &mut AggregationStats,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    stats.total_citations += rows.len();
    let entries: Vec<FullEntryOut<'_>> = rows.iter().map(FullEntryOut::from_row).collect();

    if !index.contains_doi(cited_id) {
        stats.failed_count += 1;
        if outputs.failed.is_some() {
            let writer = staging.writer(&fragment_name("failed"))?;
            write_full_record(writer, cited_id, entries.iter().collect())?;
        }
        return Ok(());
    }

    stats.valid_count += 1;
    if outputs.valid.is_some() {
        let writer = staging.writer(&fragment_name("valid"))?;
        write_full_record(writer, cited_id, entries.iter().collect())?;
    }
    let publisher: Vec<&FullEntryOut<'_>> = entries
        .iter()
        .filter(|e| e.provenance == "publisher")
        .collect();
    let crossref: Vec<&FullEntryOut<'_>> = entries
        .iter()
        .filter(|e| e.provenance == "crossref")
        .collect();
    let mined: Vec<&FullEntryOut<'_>> = entries
        .iter()
        .filter(|e| e.provenance != "publisher" && e.provenance != "crossref")
        .collect();
    stats.publisher_citations += publisher.len();
    stats.crossref_citations += crossref.len();
    stats.mined_citations += mined.len();
    for (kind, enabled, subset) in [
        ("publisher", outputs.publisher.is_some(), publisher),
        ("crossref", outputs.crossref.is_some(), crossref),
        ("mined", outputs.mined.is_some(), mined),
    ] {
        if enabled && !subset.is_empty() {
            let writer = staging.writer(&fragment_name(kind))?;
            write_full_record(writer, cited_id, subset)?;
        }
    }
    Ok(())
}

/// Emit every cited work of a sorted, deduplicated row vector.
fn emit_sorted_rows(
    rows: &[MergeRow],
    index: &DataciteIndex,
    outputs: &OutputPaths,
    staging: &mut PartitionStaging,
    stats: &mut AggregationStats,
) -> Result<()> {
    let mut start = 0;
    while start < rows.len() {
        let cited_id = rows[start].cited_id.as_str();
        let len = rows[start..]
            .iter()
            .take_while(|row| row.cited_id == cited_id)
            .count();
        emit_group(
            cited_id,
            &rows[start..start + len],
            index,
            outputs,
            staging,
            stats,
        )?;
        start += len;
    }
    Ok(())
}

/// Load every segment of a partition, drop self-citations, sort by the full
/// row order and keep the best row per key. Returns `None` as soon as the
/// decoded rows would exceed `budget`, so a metadata estimate that fell
/// short (dictionary-encoded repetition) never grows past the bound.
fn load_partition_in_memory(segments: &[PathBuf], budget: usize) -> Result<Option<Vec<MergeRow>>> {
    let mut rows: Vec<MergeRow> = Vec::new();
    let mut heap_bytes = 0usize;
    for segment in segments {
        for row in read_segment(segment, FULL_SCHEMA)? {
            if row.citing_doi == row.cited_id {
                continue;
            }
            let row = MergeRow::from(row);
            heap_bytes += row.heap_bytes();
            rows.push(row);
            if heap_bytes + rows.capacity() * size_of::<MergeRow>() > budget {
                return Ok(None);
            }
        }
    }
    rows.sort_unstable_by(compare_rows);
    rows.dedup_by(|later, earlier| later.same_key(earlier));
    Ok(Some(rows))
}

/// A sorted, deduplicated run on disk with its row count and the accounted
/// bytes of its decoded rows.
#[derive(Debug, Clone)]
struct Run {
    path: PathBuf,
    rows: u64,
    bytes: usize,
}

/// Build the segment-layout frame for sorted rows (moves every string).
fn rows_to_frame(rows: Vec<MergeRow>) -> Result<DataFrame> {
    let n = rows.len();
    let mut citing = Vec::with_capacity(n);
    let mut ref_index = Vec::with_capacity(n);
    let mut cited = Vec::with_capacity(n);
    let mut provenance = Vec::with_capacity(n);
    let mut match_type = Vec::with_capacity(n);
    let mut raw_match = Vec::with_capacity(n);
    let mut ref_json = Vec::with_capacity(n);
    for row in rows {
        citing.push(row.citing_doi);
        ref_index.push(row.ref_index);
        cited.push(row.cited_id);
        provenance.push(row.provenance);
        match_type.push(row.match_type.as_str());
        raw_match.push(row.raw_match);
        ref_json.push(row.ref_json);
    }
    DataFrame::new(vec![
        Column::new("citing_doi".into(), citing),
        Column::new("ref_index".into(), ref_index),
        Column::new("cited_id".into(), cited),
        Column::new("provenance".into(), provenance),
        Column::new(MATCH_TYPE_COLUMN.into(), match_type),
        Column::new("raw_match".into(), raw_match),
        Column::new("ref_json".into(), ref_json),
    ])
    .context("Failed to build a sorted run frame")
}

/// Sort one segment into a run: drop self-citations, order by the full row
/// order, keep the best row per key, and write it in the segment layout.
fn sort_segment_to_run(segment: &Path, output: &Path) -> Result<Run> {
    let mut rows: Vec<MergeRow> = read_segment(segment, FULL_SCHEMA)?
        .into_iter()
        .filter(|row| row.citing_doi != row.cited_id)
        .map(MergeRow::from)
        .collect();
    rows.sort_unstable_by(compare_rows);
    rows.dedup_by(|later, earlier| later.same_key(earlier));
    let bytes = rows.iter().map(MergeRow::accounted_bytes).sum();
    let count = rows.len() as u64;
    let mut frame = rows_to_frame(rows)?;
    let file = File::create(output)
        .with_context(|| format!("Failed to create run {}", output.display()))?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut frame)
        .with_context(|| format!("Failed to write run {}", output.display()))?;
    Ok(Run {
        path: output.to_path_buf(),
        rows: count,
        bytes,
    })
}

/// Rows each merge cursor buffers so the cursors and one output batch stay
/// within half the memory budget, from the runs' measured average row size.
fn merge_chunk_rows(limits: &AggregationLimits, runs: &[Run]) -> usize {
    let rows: u64 = runs.iter().map(|run| run.rows).sum();
    let bytes: u64 = runs.iter().map(|run| run.bytes as u64).sum();
    let average = if rows == 0 { 1 } else { (bytes / rows).max(1) };
    let per_cursor = limits.memory_bytes / 2 / (limits.merge_fan_in + 1);
    usize::try_from(per_cursor as u64 / average)
        .unwrap_or(STREAM_CHUNK_SIZE)
        .clamp(MERGE_CHUNK_MIN_ROWS, STREAM_CHUNK_SIZE)
}

/// Merge a group of runs into one new run, writing row groups of
/// `chunk_rows` as they are produced, then verify it by row count.
fn merge_runs_to_file(group: &[Run], output: &Path, chunk_rows: usize) -> Result<Run> {
    let paths: Vec<PathBuf> = group.iter().map(|run| run.path.clone()).collect();
    let file = File::create(output)
        .with_context(|| format!("Failed to create run {}", output.display()))?;
    let schema = rows_to_frame(Vec::new())?.schema().clone();
    let mut writer = ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .batched(&schema)
        .context("Failed to open a batched run writer")?;

    let mut batch: Vec<MergeRow> = Vec::new();
    let mut rows = 0u64;
    let mut bytes = 0usize;
    merge_sorted_segments(&paths, chunk_rows, FULL_SCHEMA, |row| {
        rows += 1;
        bytes += row.accounted_bytes();
        batch.push(row);
        if batch.len() >= chunk_rows {
            writer.write_batch(&rows_to_frame(std::mem::take(&mut batch))?)?;
        }
        Ok(())
    })?;
    if !batch.is_empty() {
        writer.write_batch(&rows_to_frame(batch)?)?;
    }
    writer
        .finish()
        .with_context(|| format!("Failed to finish run {}", output.display()))?;

    let written = get_parquet_row_count(output)? as u64;
    ensure!(
        written == rows,
        "merged run {} holds {written} rows but {rows} were written",
        output.display()
    );
    ensure!(
        fs::metadata(output)?.len() > 0,
        "merged run {} is empty",
        output.display()
    );
    Ok(Run {
        path: output.to_path_buf(),
        rows,
        bytes,
    })
}

/// External path: sort each segment into a run on a bounded pool, merge
/// runs in passes of at most `merge_fan_in`, then stream the final merge
/// into per-cited-work groups under the group budget.
#[allow(clippy::too_many_arguments)]
fn aggregate_partition_external(
    name: &str,
    segments: &[PathBuf],
    estimates: &[SegmentEstimate],
    index: &DataciteIndex,
    outputs: &OutputPaths,
    limits: &AggregationLimits,
    pool: &rayon::ThreadPool,
    staging: &mut PartitionStaging,
    stats: &mut AggregationStats,
) -> Result<()> {
    let runs_dir = staging.dir().join(RUNS_DIR);
    fs::create_dir_all(&runs_dir)
        .with_context(|| format!("Failed to create {}", runs_dir.display()))?;

    // Sort concurrency is bounded by the pool and by how many of the
    // largest segments fit the budget at once.
    let largest = estimates.iter().map(|e| e.bytes).max().unwrap_or(0);
    ensure!(
        largest <= limits.memory_bytes,
        "partition {name}: a segment is estimated at {largest} decoded bytes, over the \
         {}-byte aggregation memory budget; extraction segments must fit the budget",
        limits.memory_bytes
    );
    let concurrent = (limits.memory_bytes / largest.max(1)).clamp(1, limits.sort_workers);
    let mut runs: Vec<Run> = Vec::with_capacity(segments.len());
    for (chunk_index, chunk) in segments.chunks(concurrent).enumerate() {
        let sorted = pool.install(|| {
            chunk
                .par_iter()
                .enumerate()
                .map(|(i, segment)| {
                    let run_index = chunk_index * concurrent + i;
                    let output = runs_dir.join(format!("run_p0_{run_index:06}.parquet"));
                    sort_segment_to_run(segment, &output)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        runs.extend(sorted);
    }

    let mut pass = 1;
    while runs.len() > limits.merge_fan_in {
        let chunk_rows = merge_chunk_rows(limits, &runs);
        let mut next = Vec::with_capacity(runs.len().div_ceil(limits.merge_fan_in));
        for (group_index, group) in runs.chunks(limits.merge_fan_in).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let output = runs_dir.join(format!("run_p{pass}_{group_index:06}.parquet"));
            let merged = merge_runs_to_file(group, &output, chunk_rows)?;
            // The replacement is closed and verified; its inputs are redundant.
            for run in group {
                fs::remove_file(&run.path)
                    .with_context(|| format!("Failed to remove run {}", run.path.display()))?;
            }
            next.push(merged);
        }
        debug!(
            "Partition {name}: merge pass {pass} reduced {} runs to {}",
            runs.len(),
            next.len()
        );
        runs = next;
        pass += 1;
    }

    let chunk_rows = merge_chunk_rows(limits, &runs);
    let paths: Vec<PathBuf> = runs.iter().map(|run| run.path.clone()).collect();
    let mut group: Vec<MergeRow> = Vec::new();
    let mut group_bytes = 0usize;
    merge_sorted_segments(&paths, chunk_rows, FULL_SCHEMA, |row| {
        if group
            .last()
            .is_some_and(|last| last.cited_id != row.cited_id)
        {
            emit_group(&group[0].cited_id, &group, index, outputs, staging, stats)?;
            group.clear();
            group_bytes = 0;
        }
        group_bytes += row.accounted_bytes();
        ensure!(
            group_bytes <= limits.memory_bytes,
            "cited work {} has more retained references than fit the aggregation memory \
             budget: {group_bytes} bytes over {} references so far, budget {} bytes; raise \
             the aggregation memory limit",
            row.cited_id,
            group.len() + 1,
            limits.memory_bytes
        );
        group.push(row);
        Ok(())
    })?;
    if !group.is_empty() {
        emit_group(&group[0].cited_id, &group, index, outputs, staging, stats)?;
    }
    fs::remove_dir_all(&runs_dir)
        .with_context(|| format!("Failed to remove {}", runs_dir.display()))?;
    Ok(())
}

/// Aggregate one partition into a committed directory, choosing the path
/// from its Parquet metadata.
#[allow(clippy::too_many_arguments)]
fn aggregate_partition(
    name: &str,
    segments: &[PathBuf],
    index: &DataciteIndex,
    outputs: &OutputPaths,
    limits: &AggregationLimits,
    pool: &rayon::ThreadPool,
    agg_dir: &Path,
    identity_sha256: &str,
) -> Result<PartitionCommit> {
    let estimates = segments
        .iter()
        .map(|segment| estimate_segment(segment))
        .collect::<Result<Vec<_>>>()?;
    let total_rows: usize = estimates.iter().map(|e| e.rows).sum();
    let total_bytes = estimates
        .iter()
        .fold(0usize, |acc, e| acc.saturating_add(e.bytes));

    let mut staging = PartitionStaging::create(agg_dir, name)?;
    let mut stats = AggregationStats::default();
    let mut in_memory = None;
    if total_bytes <= limits.memory_bytes {
        in_memory = load_partition_in_memory(segments, limits.memory_bytes)?;
        if in_memory.is_none() {
            warn!(
                "Partition {name}: decoded rows exceed the {}-byte budget although {} segments \
                 were estimated at {total_bytes} bytes; using external sort",
                limits.memory_bytes,
                segments.len()
            );
        }
    }
    match in_memory {
        Some(rows) => {
            debug!(
                "Partition {name}: {} retained rows of {total_rows} (estimated {total_bytes} \
                 bytes) aggregated in memory",
                rows.len()
            );
            emit_sorted_rows(&rows, index, outputs, &mut staging, &mut stats)?;
            stats.in_memory_partitions = 1;
        }
        None => {
            info!(
                "Partition {name}: external sort of {} segments ({total_rows} rows, estimated \
                 {total_bytes} bytes)",
                segments.len()
            );
            aggregate_partition_external(
                name,
                segments,
                &estimates,
                index,
                outputs,
                limits,
                pool,
                &mut staging,
                &mut stats,
            )?;
            stats.external_partitions = 1;
        }
    }
    stats.partitions_processed = 1;
    let (_, commit) = staging.commit(identity_sha256, stats.snapshot())?;
    Ok(commit)
}

/// Build every enabled output from the committed fragments, in partition
/// order, through a temporary file that then replaces the target.
fn assemble_outputs(
    agg_dir: &Path,
    commits: &[(String, PartitionCommit)],
    outputs: &OutputPaths,
) -> Result<()> {
    for (kind, target) in output_targets(outputs) {
        let Some(target) = target else {
            continue;
        };
        let fragment = fragment_name(kind);
        let tmp = tmp_output_path(target);
        let mut writer = BufWriter::new(
            File::create(&tmp).with_context(|| format!("Failed to create {}", tmp.display()))?,
        );
        for (name, commit) in commits {
            let Some(expected) = commit.files.get(&fragment) else {
                continue;
            };
            let path = agg_dir.join(name).join(&fragment);
            let mut input =
                File::open(&path).with_context(|| format!("Failed to open {}", path.display()))?;
            let copied = io::copy(&mut input, &mut writer)?;
            ensure!(
                copied == expected.size,
                "fragment {} is {copied} bytes but its commit recorded {}",
                path.display(),
                expected.size
            );
        }
        writer.flush()?;
        writer
            .into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()
            .with_context(|| format!("Failed to sync {}", tmp.display()))?;
        hit_failpoint(FAILPOINT_DURING_FINAL_ASSEMBLY, target)?;
        fs::rename(&tmp, target)
            .with_context(|| format!("Failed to move {} to {}", tmp.display(), target.display()))?;
        sync_parent(target)?;
        info!("Wrote {kind} output {}", target.display());
    }
    Ok(())
}

/// Aggregate full-mode (hashed) partitions into grouped JSONL outputs,
/// validating every cited DOI against the DataCite index.
///
/// Each partition is deduplicated on `(cited_id, citing_doi, ref_index)`
/// with deterministic evidence selection, processed in memory when its
/// Parquet metadata estimate fits `limits.memory_bytes` and through bounded
/// external sort otherwise, and committed under
/// `<partition_dir>/_aggregation/<partition>/` with checksummed fragments.
/// With `resume`, commits that verify against `identity_sha256` are reused
/// and only the rest are recomputed; without it, previous commits are
/// discarded first. Final outputs are assembled from the commits into
/// temporary files and renamed into place, never appended.
///
/// Every `cited_by` entry carries `doi`, `ref_index`, `provenance`,
/// `match_type`, `raw_match`, and the original `reference`, ordered by
/// `(citing_doi, ref_index)`; `citation_count` is the entry count. Cited
/// works the index lacks (retained failed arXiv evidence) go to `failed`.
pub fn aggregate_datacite(
    partition_dir: &Path,
    index: &DataciteIndex,
    outputs: &OutputPaths,
    limits: AggregationLimits,
    identity_sha256: &str,
    resume: bool,
) -> Result<AggregationStats> {
    limits.validate()?;
    ensure!(
        !identity_sha256.is_empty(),
        "aggregation identity must not be empty"
    );
    let agg_dir = aggregation_dir(partition_dir);
    if !resume && agg_dir.exists() {
        info!(
            "Discarding previous aggregation commits under {}",
            agg_dir.display()
        );
        fs::remove_dir_all(&agg_dir)
            .with_context(|| format!("Failed to remove {}", agg_dir.display()))?;
    }
    fs::create_dir_all(&agg_dir)
        .with_context(|| format!("Failed to create {}", agg_dir.display()))?;
    remove_stale_staging(&agg_dir)?;

    let partitions = list_partitions(partition_dir)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(limits.sort_workers)
        .build()
        .context("Failed to build the sort thread pool")?;
    info!(
        "Aggregating {} partitions under {} (memory {} bytes, fan-in {}, {} sort workers)",
        partitions.len(),
        partition_dir.display(),
        limits.memory_bytes,
        limits.merge_fan_in,
        limits.sort_workers
    );

    let mut stats = AggregationStats::default();
    let mut commits: Vec<(String, PartitionCommit)> = Vec::with_capacity(partitions.len());
    for (name, partition_path) in &partitions {
        let committed = agg_dir.join(name);
        let reusable =
            PartitionCommit::load_verified(&committed, identity_sha256).with_context(|| {
                format!(
                    "verifying committed partition {}; rerun without resume to discard it",
                    committed.display()
                )
            })?;
        if let Some(commit) = reusable {
            check_committed_fragments(&commit, outputs)?;
            stats.absorb(&commit.stats);
            stats.reused_partitions += 1;
            commits.push((name.clone(), commit));
            continue;
        }
        let segments = committed_segments(partition_path)?;
        if segments.is_empty() {
            debug!("Partition {name} has no segments");
            continue;
        }
        let commit = aggregate_partition(
            name,
            &segments,
            index,
            outputs,
            &limits,
            &pool,
            &agg_dir,
            identity_sha256,
        )
        .with_context(|| format!("Failed to aggregate partition {name}"))?;
        stats.absorb(&commit.stats);
        commits.push((name.clone(), commit));
        if stats.partitions_processed % 100 == 0 {
            info!(
                "Progress: {}/{} partitions, {} valid, {} citations",
                stats.partitions_processed,
                partitions.len(),
                stats.valid_count,
                stats.total_citations
            );
        }
    }

    assemble_outputs(&agg_dir, &commits, outputs)?;
    info!(
        "Aggregation complete: {} valid, {} failed, {} citations; {} partitions in memory, {} \
         external, {} reused",
        stats.valid_count,
        stats.failed_count,
        stats.total_citations,
        stats.in_memory_partitions,
        stats.external_partitions,
        stats.reused_partitions
    );
    Ok(stats)
}
