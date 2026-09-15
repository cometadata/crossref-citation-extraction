use anyhow::{Context, Result};
use log::{info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use super::checkpoint::AggregationCheckpoint;
use super::kway_merge::{merge_sorted_segments, MergeRow};
use super::partitioning::PartitionMode;
use crate::index::FstIndex;

/// Maximum number of segments to process at once in memory.
/// Partitions with more segments will use external sort.
const SEGMENT_BATCH_THRESHOLD: usize = 100;

/// Chunk size for reading parquet files during streaming (rows per chunk).
/// Must fit in u32 for Polars slice() compatibility.
const STREAM_CHUNK_SIZE: usize = 500_000;
const _: () = assert!(
    STREAM_CHUNK_SIZE <= u32::MAX as usize,
    "STREAM_CHUNK_SIZE must fit in u32"
);

/// Total rows buffered across all merge cursors (bounds merge memory).
const MERGE_BUFFER_ROWS: usize = 4_000_000;

/// Progress logging interval (rows processed).
const PROGRESS_LOG_INTERVAL: u64 = 5_000_000;

/// Sort each segment of a partition to its own temp file, in parallel.
/// Returns one sorted file per input segment; global ordering is produced
/// later by the k-way merge, not here.
fn sort_segments_to_temp(segments: &[PathBuf], partition_path: &Path) -> Result<Vec<PathBuf>> {
    // Set Polars temp directory to our partition path (with enough space).
    // SAFETY: partitions are processed sequentially; do not parallelize
    // partition processing while this global env var is set per-partition.
    std::env::set_var("POLARS_TEMP_DIR", partition_path);

    info!("Sorting {} segments in parallel...", segments.len());

    segments
        .par_iter()
        .enumerate()
        .map(|(i, seg)| {
            let sorted_seg_path = partition_path.join(format!("_sorted_{:05}.parquet", i));
            sort_single_segment(seg, &sorted_seg_path)?;
            Ok(sorted_seg_path)
        })
        .collect()
}

/// Sort a single segment file: deduplicate, filter self-citations, and sort by
/// (cited_id, citing_doi, ref_index) — the key the k-way merge expects.
fn sort_single_segment(input: &Path, output: &Path) -> Result<()> {
    // Each segment is small enough to fit in memory (~1M rows typically)
    let mut df = LazyFrame::scan_parquet(input, Default::default())?
        .unique(
            Some(vec![
                "citing_doi".into(),
                "ref_index".into(),
                "cited_id".into(),
            ]),
            UniqueKeepStrategy::First,
        )
        .filter(col("citing_doi").neq(col("cited_id")))
        .sort(
            ["cited_id", "citing_doi", "ref_index"],
            SortMultipleOptions::default(),
        )
        .collect()
        .map_err(|e| anyhow::anyhow!("Failed to process segment: {}", e))?;

    let file = File::create(output)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)
        .map_err(|e| anyhow::anyhow!("Failed to write sorted segment: {}", e))?;

    Ok(())
}

/// An output record, serialized directly from borrowed citation values so the
/// `cited_by` array never has to be cloned or concatenated.
#[derive(serde::Serialize)]
struct CitedRecordOut<'a> {
    doi: &'a str,
    citation_count: usize,
    cited_by: Vec<&'a serde_json::Value>,
}

fn write_jsonl_record(w: &mut BufWriter<File>, record: &CitedRecordOut<'_>) -> Result<()> {
    serde_json::to_writer(&mut *w, record)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// Flush a group of citations for a single cited DOI to output files.
#[allow(clippy::too_many_arguments)]
fn flush_group(
    cited_id: &str,
    citations: &[GroupedCitation],
    arxiv_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    if citations.is_empty() {
        return Ok(());
    }

    let is_valid = arxiv_index
        .map(|idx| idx.contains(cited_id))
        .unwrap_or(false);

    // Group by provenance using fixed Vecs (only 3 types - avoids HashMap overhead)
    let mut publisher_citations = Vec::new();
    let mut crossref_citations = Vec::new();
    let mut mined_citations = Vec::new();

    for cit in citations {
        let reference: serde_json::Value =
            serde_json::from_str(&cit.ref_json).unwrap_or(serde_json::Value::Null);
        let citation_json = serde_json::json!({
            "doi": cit.citing_doi,
            "provenance": cit.provenance,
            "raw_match": cit.raw_match,
            "reference": reference
        });
        match cit.provenance.as_str() {
            "publisher" => publisher_citations.push(citation_json),
            "crossref" => crossref_citations.push(citation_json),
            _ => mined_citations.push(citation_json),
        }
    }

    let citation_count =
        publisher_citations.len() + crossref_citations.len() + mined_citations.len();
    stats.total_citations += citation_count;

    let record = CitedRecordOut {
        doi: cited_id,
        citation_count,
        cited_by: publisher_citations
            .iter()
            .chain(crossref_citations.iter())
            .chain(mined_citations.iter())
            .collect(),
    };

    if is_valid {
        if let Some(ref mut w) = valid_writer {
            write_jsonl_record(w, &record)?;
        }
        stats.valid_count += 1;
    } else {
        if let Some(ref mut w) = failed_writer {
            write_jsonl_record(w, &record)?;
        }
        stats.failed_count += 1;
    }

    let split_by_provenance =
        publisher_writer.is_some() || crossref_writer.is_some() || mined_writer.is_some();

    if split_by_provenance && is_valid {
        write_provenance_outputs_direct(
            cited_id,
            &publisher_citations,
            &crossref_citations,
            &mined_citations,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )?;
    }

    Ok(())
}

/// Process a large partition using external sort:
/// sort each segment, then k-way merge directly into group aggregation.
#[allow(clippy::too_many_arguments)]
fn process_partition_external_sort(
    segments: &[PathBuf],
    partition_path: &Path,
    arxiv_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    info!(
        "External sort for partition with {} segments: {}",
        segments.len(),
        partition_path.display()
    );

    let sorted_files = sort_segments_to_temp(segments, partition_path)?;

    let chunk_rows = (MERGE_BUFFER_ROWS / sorted_files.len().max(1)).clamp(1024, STREAM_CHUNK_SIZE);

    let mut current_cited_id: Option<String> = None;
    let mut current_group: Vec<GroupedCitation> = Vec::new();
    let mut rows_processed = 0u64;

    // Legacy partitions may predate the match_type column.
    let mode = PartitionMode::Legacy;
    let merge_result = (|| -> Result<()> {
        merge_sorted_segments(&sorted_files, chunk_rows, mode, |row: MergeRow| {
            if current_cited_id.as_deref() != Some(row.cited_id.as_str()) {
                if let Some(id) = current_cited_id.take() {
                    flush_group(
                        &id,
                        &current_group,
                        arxiv_index,
                        valid_writer,
                        failed_writer,
                        publisher_writer,
                        crossref_writer,
                        mined_writer,
                        stats,
                    )?;
                }
                current_cited_id = Some(row.cited_id);
                current_group.clear();
            }
            current_group.push(GroupedCitation {
                citing_doi: row.citing_doi,
                provenance: row.provenance,
                raw_match: row.raw_match,
                ref_json: row.ref_json,
            });
            rows_processed += 1;
            if rows_processed.is_multiple_of(PROGRESS_LOG_INTERVAL) {
                info!(
                    "  Progress: {} rows, {} valid DOIs",
                    rows_processed, stats.valid_count
                );
            }
            Ok(())
        })?;

        if let Some(id) = current_cited_id.take() {
            flush_group(
                &id,
                &current_group,
                arxiv_index,
                valid_writer,
                failed_writer,
                publisher_writer,
                crossref_writer,
                mined_writer,
                stats,
            )?;
        }
        Ok(())
    })();

    for sorted_path in &sorted_files {
        if sorted_path.exists() {
            if let Err(e) = fs::remove_file(sorted_path) {
                warn!(
                    "Failed to delete temp file {}: {}",
                    sorted_path.display(),
                    e
                );
            }
        }
    }

    merge_result?;
    info!("  Total rows processed: {}", rows_processed);

    stats.partitions_processed += 1;
    Ok(())
}

/// Statistics from aggregation run.
///
/// All fields are public and updated during aggregation. Used for progress
/// reporting, checkpointing, and final summary output.
#[derive(Debug, Clone, Default)]
pub struct AggregationStats {
    /// Number of partition directories processed
    pub partitions_processed: usize,
    /// Number of cited DOIs that passed validation
    pub valid_count: usize,
    /// Number of cited DOIs that failed validation
    pub failed_count: usize,
    /// Total number of individual citations across all cited DOIs
    pub total_citations: usize,
    /// Citations with publisher provenance
    pub publisher_citations: usize,
    /// Citations with crossref provenance
    pub crossref_citations: usize,
    /// Citations with mined provenance
    pub mined_citations: usize,
}

/// Clean up any stale temp files from interrupted runs.
/// This includes the _sorted*.parquet files written by the external-sort path.
fn cleanup_stale_temp_files(partition_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(partition_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Ok(partition_entries) = fs::read_dir(&path) {
                for file_entry in partition_entries.filter_map(|e| e.ok()) {
                    let file_path = file_entry.path();
                    if let Some(name) = file_path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with("_sorted") {
                            info!("Cleaning up stale temp file: {}", file_path.display());
                            if let Err(e) = fs::remove_file(&file_path) {
                                warn!(
                                    "Failed to delete stale temp file {}: {}",
                                    file_path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Helper to open a writer with optional append mode
fn open_output_writer(path: &Path, append: bool) -> Result<BufWriter<File>> {
    let file = if append {
        OpenOptions::new().create(true).append(true).open(path)?
    } else {
        File::create(path)?
    };
    Ok(BufWriter::new(file))
}

/// Flush all writers
fn flush_all_writers(
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
) -> Result<()> {
    if let Some(ref mut w) = valid_writer {
        w.flush()?;
    }
    if let Some(ref mut w) = failed_writer {
        w.flush()?;
    }
    if let Some(ref mut w) = publisher_writer {
        w.flush()?;
    }
    if let Some(ref mut w) = crossref_writer {
        w.flush()?;
    }
    if let Some(ref mut w) = mined_writer {
        w.flush()?;
    }
    Ok(())
}

/// Aggregate partitions with streaming validation and provenance splitting.
///
/// Processes all partition directories under `partition_dir`, aggregates citations
/// by cited DOI, validates against the arXiv FST index, and writes results to
/// the specified output files.
///
/// # Arguments
///
/// * `partition_dir` - Directory containing partition subdirectories (e.g., "10.1234/")
/// * `arxiv_index` - Optional FST index for validation. If None, all records go to failed output.
/// * `outputs` - Paths for output files (valid, failed, and provenance-split files)
/// * `resume` - If true, resume from checkpoint if available
/// * `checkpoint_interval` - Save checkpoint every N partitions (0 = disabled).
///   Note: partitions completed after the last checkpoint are re-appended on
///   resume; interval 1 bounds that duplication window to one partition.
///
/// # Returns
///
/// Statistics about the aggregation run (counts of valid/failed records, citations, etc.)
pub fn aggregate_and_validate(
    partition_dir: &Path,
    arxiv_index: Option<&FstIndex>,
    outputs: &crate::common::OutputPaths,
    resume: bool,
    checkpoint_interval: usize,
) -> Result<AggregationStats> {
    // Warn if no arXiv index provided - all records will be marked as failed
    if arxiv_index.is_none() {
        warn!("No arXiv index provided - all records will be marked as failed validation");
    }

    let checkpoint_path = partition_dir.join("aggregation.checkpoint");

    // Load checkpoint if resuming
    let (completed_set, mut stats, mut completed_partitions) = if resume {
        match AggregationCheckpoint::load(&checkpoint_path)? {
            Some(cp) => {
                let set: HashSet<String> = cp.completed_partitions.iter().cloned().collect();
                let stats = AggregationStats::from(cp.stats);
                info!(
                    "Resuming aggregation: {} partitions already processed",
                    set.len()
                );
                (set, stats, cp.completed_partitions)
            }
            None => (HashSet::new(), AggregationStats::default(), Vec::new()),
        }
    } else {
        // Clean up any existing checkpoint when not resuming
        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path)?;
        }
        (HashSet::new(), AggregationStats::default(), Vec::new())
    };

    // Open writers (append mode if resuming with existing checkpoint)
    let append_mode = !completed_set.is_empty();
    let mut valid_writer = outputs
        .valid
        .as_ref()
        .map(|p| {
            open_output_writer(p, append_mode)
                .with_context(|| format!("Failed to open valid output {}", p.display()))
        })
        .transpose()?;
    let mut failed_writer = outputs
        .failed
        .as_ref()
        .map(|p| {
            open_output_writer(p, append_mode)
                .with_context(|| format!("Failed to open failed output {}", p.display()))
        })
        .transpose()?;
    let mut publisher_writer = outputs
        .publisher
        .as_ref()
        .map(|p| {
            open_output_writer(p, append_mode)
                .with_context(|| format!("Failed to open publisher output {}", p.display()))
        })
        .transpose()?;
    let mut crossref_writer = outputs
        .crossref
        .as_ref()
        .map(|p| {
            open_output_writer(p, append_mode)
                .with_context(|| format!("Failed to open crossref output {}", p.display()))
        })
        .transpose()?;
    let mut mined_writer = outputs
        .mined
        .as_ref()
        .map(|p| {
            open_output_writer(p, append_mode)
                .with_context(|| format!("Failed to open mined output {}", p.display()))
        })
        .transpose()?;

    // Clean up any stale temp files from interrupted runs
    cleanup_stale_temp_files(partition_dir)?;

    // Collect and sort partitions for deterministic order (important for resume)
    // Every subdirectory is a partition; sibling files (checkpoints, arxiv.fst)
    // are excluded by the is_dir check.
    let mut partitions: Vec<_> = fs::read_dir(partition_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    partitions.sort();

    let total_partitions = partitions.len();
    let partitions_to_process = total_partitions - completed_set.len();
    info!(
        "Processing {} partitions ({} already completed)...",
        partitions_to_process,
        completed_set.len()
    );

    let mut batch_count = 0;

    // Iterate partition directories
    for partition_path in partitions {
        let partition_name = partition_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        // Skip already completed partitions (resume mode)
        if completed_set.contains(&partition_name) {
            continue;
        }

        process_partition(
            &partition_path,
            arxiv_index,
            &mut valid_writer,
            &mut failed_writer,
            &mut publisher_writer,
            &mut crossref_writer,
            &mut mined_writer,
            &mut stats,
        )?;

        // Track completed partition
        completed_partitions.push(partition_name);
        batch_count += 1;

        // Checkpoint every N partitions
        if checkpoint_interval > 0 && batch_count % checkpoint_interval == 0 {
            flush_all_writers(
                &mut valid_writer,
                &mut failed_writer,
                &mut publisher_writer,
                &mut crossref_writer,
                &mut mined_writer,
            )?;
            AggregationCheckpoint::save(&checkpoint_path, &completed_partitions, &stats)?;
            info!(
                "Checkpoint saved: {}/{} partitions complete",
                stats.partitions_processed, total_partitions
            );
        }

        if stats.partitions_processed % 100 == 0 {
            info!(
                "Progress: {}/{} partitions, {} valid, {} citations",
                stats.partitions_processed,
                total_partitions,
                stats.valid_count,
                stats.total_citations
            );
        }
    }

    // Final flush
    flush_all_writers(
        &mut valid_writer,
        &mut failed_writer,
        &mut publisher_writer,
        &mut crossref_writer,
        &mut mined_writer,
    )?;

    info!(
        "Aggregation complete: {} valid, {} failed, {} total citations",
        stats.valid_count, stats.failed_count, stats.total_citations
    );
    let has_provenance_outputs =
        outputs.publisher.is_some() || outputs.crossref.is_some() || outputs.mined.is_some();
    if has_provenance_outputs {
        info!(
            "  By provenance: {} publisher, {} crossref, {} mined",
            stats.publisher_citations, stats.crossref_citations, stats.mined_citations
        );
    }

    // Clean up checkpoint on successful completion
    AggregationCheckpoint::cleanup(&checkpoint_path)?;

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn process_partition(
    partition_path: &Path,
    arxiv_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    let segments: Vec<_> = fs::read_dir(partition_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("parquet")
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with('_'))
                    .unwrap_or(false)
        })
        .collect();

    if segments.is_empty() {
        return Ok(());
    }

    if segments.len() > SEGMENT_BATCH_THRESHOLD {
        process_partition_external_sort(
            &segments,
            partition_path,
            arxiv_index,
            valid_writer,
            failed_writer,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )
    } else {
        process_partition_inmemory(
            &segments,
            arxiv_index,
            valid_writer,
            failed_writer,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )
    }
}

/// Process a partition with few segments by loading all data into memory at once
/// This is faster for small partitions but uses more memory
#[allow(clippy::too_many_arguments)]
fn process_partition_inmemory(
    segments: &[std::path::PathBuf],
    arxiv_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    let lazy_frames: Result<Vec<_>, _> = segments
        .iter()
        .map(|p| LazyFrame::scan_parquet(p, Default::default()))
        .collect();

    let merged = concat(&lazy_frames?, UnionArgs::default())?
        .unique(
            Some(vec![
                "citing_doi".into(),
                "ref_index".into(),
                "cited_id".into(),
            ]),
            UniqueKeepStrategy::First,
        )
        .filter(col("citing_doi").neq(col("cited_id")))
        .collect()?;

    // Segment row order depends on how the parallel extraction interleaved
    // writes, so the aggregated lists would otherwise vary run to run. Sorting
    // on the merge key first makes cited_by deterministic and matches the
    // ordering the external-sort path produces.
    let grouped = merged
        .lazy()
        .sort(
            ["cited_id", "citing_doi", "ref_index"],
            SortMultipleOptions::default(),
        )
        .group_by([col("cited_id")])
        .agg([
            col("citing_doi"),
            col("provenance"),
            col("raw_match"),
            col("ref_json"),
        ])
        .collect()?;

    write_grouped_results(
        &grouped,
        arxiv_index,
        valid_writer,
        failed_writer,
        publisher_writer,
        crossref_writer,
        mined_writer,
        stats,
    )?;

    stats.partitions_processed += 1;
    Ok(())
}

/// Citation data for streaming aggregation
#[derive(Clone)]
struct GroupedCitation {
    citing_doi: String,
    provenance: String,
    raw_match: String,
    ref_json: String,
}

/// Write grouped results from a Polars DataFrame to output files
#[allow(clippy::too_many_arguments)]
fn write_grouped_results(
    grouped: &DataFrame,
    arxiv_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    let cited_id_col = grouped.column("cited_id")?.str()?;
    let citing_dois_col = grouped.column("citing_doi")?.list()?;
    let provenances_col = grouped.column("provenance")?.list()?;
    let raw_matches_col = grouped.column("raw_match")?.list()?;
    let ref_jsons_col = grouped.column("ref_json")?.list()?;

    let split_by_provenance =
        publisher_writer.is_some() || crossref_writer.is_some() || mined_writer.is_some();

    for i in 0..grouped.height() {
        let cited_id = cited_id_col.get(i).unwrap_or("");

        let is_valid = arxiv_index
            .map(|idx| idx.contains(cited_id))
            .unwrap_or(false);

        let grouped = build_cited_by_array_grouped(
            citing_dois_col.get_as_series(i),
            provenances_col.get_as_series(i),
            raw_matches_col.get_as_series(i),
            ref_jsons_col.get_as_series(i),
        )?;

        let citation_count = grouped.total_count();
        stats.total_citations += citation_count;

        if split_by_provenance && is_valid {
            write_provenance_outputs_direct(
                cited_id,
                &grouped.publisher,
                &grouped.crossref,
                &grouped.mined,
                publisher_writer,
                crossref_writer,
                mined_writer,
                stats,
            )?;
        }

        let record = CitedRecordOut {
            doi: cited_id,
            citation_count,
            cited_by: grouped
                .publisher
                .iter()
                .chain(grouped.crossref.iter())
                .chain(grouped.mined.iter())
                .collect(),
        };

        if is_valid {
            if let Some(ref mut w) = valid_writer {
                write_jsonl_record(w, &record)?;
            }
            stats.valid_count += 1;
        } else {
            if let Some(ref mut w) = failed_writer {
                write_jsonl_record(w, &record)?;
            }
            stats.failed_count += 1;
        }
    }

    Ok(())
}

/// Write provenance-specific output files using direct Vec references (avoids HashMap)
#[allow(clippy::too_many_arguments)]
fn write_provenance_outputs_direct(
    cited_id: &str,
    publisher_citations: &[serde_json::Value],
    crossref_citations: &[serde_json::Value],
    mined_citations: &[serde_json::Value],
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    if !publisher_citations.is_empty() {
        if let Some(ref mut w) = publisher_writer {
            let record = CitedRecordOut {
                doi: cited_id,
                citation_count: publisher_citations.len(),
                cited_by: publisher_citations.iter().collect(),
            };
            write_jsonl_record(w, &record)?;
        }
        stats.publisher_citations += publisher_citations.len();
    }

    if !crossref_citations.is_empty() {
        if let Some(ref mut w) = crossref_writer {
            let record = CitedRecordOut {
                doi: cited_id,
                citation_count: crossref_citations.len(),
                cited_by: crossref_citations.iter().collect(),
            };
            write_jsonl_record(w, &record)?;
        }
        stats.crossref_citations += crossref_citations.len();
    }

    if !mined_citations.is_empty() {
        if let Some(ref mut w) = mined_writer {
            let record = CitedRecordOut {
                doi: cited_id,
                citation_count: mined_citations.len(),
                cited_by: mined_citations.iter().collect(),
            };
            write_jsonl_record(w, &record)?;
        }
        stats.mined_citations += mined_citations.len();
    }

    Ok(())
}

/// Grouped citations by provenance (avoids HashMap overhead)
struct GroupedCitationsByProvenance {
    publisher: Vec<serde_json::Value>,
    crossref: Vec<serde_json::Value>,
    mined: Vec<serde_json::Value>,
}

impl GroupedCitationsByProvenance {
    fn new() -> Self {
        Self {
            publisher: Vec::new(),
            crossref: Vec::new(),
            mined: Vec::new(),
        }
    }

    fn total_count(&self) -> usize {
        self.publisher.len() + self.crossref.len() + self.mined.len()
    }
}

fn build_cited_by_array_grouped(
    citing_dois: Option<Series>,
    provenances: Option<Series>,
    raw_matches: Option<Series>,
    ref_jsons: Option<Series>,
) -> Result<GroupedCitationsByProvenance> {
    let citing_dois = citing_dois.ok_or_else(|| anyhow::anyhow!("missing citing_dois"))?;
    let provenances = provenances.ok_or_else(|| anyhow::anyhow!("missing provenances"))?;
    let raw_matches = raw_matches.ok_or_else(|| anyhow::anyhow!("missing raw_matches"))?;
    let ref_jsons = ref_jsons.ok_or_else(|| anyhow::anyhow!("missing ref_jsons"))?;

    let dois = citing_dois.str()?;
    let provs = provenances.str()?;
    let raws = raw_matches.str()?;
    let jsons = ref_jsons.str()?;

    let mut result = GroupedCitationsByProvenance::new();

    for j in 0..citing_dois.len() {
        let doi = dois.get(j).unwrap_or("");
        let provenance = provs.get(j).unwrap_or("mined");
        let ref_json_str = jsons.get(j).unwrap_or("null");

        let reference: serde_json::Value =
            serde_json::from_str(ref_json_str).unwrap_or(serde_json::Value::Null);

        let citation = serde_json::json!({
            "doi": doi,
            "provenance": provenance,
            "raw_match": raws.get(j).unwrap_or(""),
            "reference": reference
        });

        match provenance {
            "publisher" => result.publisher.push(citation),
            "crossref" => result.crossref.push(citation),
            _ => result.mined.push(citation),
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::OutputPaths;
    use crate::streaming::{PartitionRow, SegmentedPartitionWriter};
    use tempfile::tempdir;

    /// The external-sort path writes `_sorted_<n>.parquet` temp files; none of
    /// them may survive a completed partition.
    fn assert_no_sorted_temp_files(partition_path: &Path) {
        let leftovers: Vec<String> = fs::read_dir(partition_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| name.starts_with("_sorted"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "stale sort temp files left behind: {:?}",
            leftovers
        );
    }

    #[test]
    fn test_aggregate_and_validate() {
        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        let valid_output = dir.path().join("valid.jsonl");
        let failed_output = dir.path().join("failed.jsonl");

        // Create test data
        {
            let mut partition_writer = SegmentedPartitionWriter::new(&partition_dir, 100).unwrap();

            // Two citations to same work
            partition_writer
                .write(PartitionRow {
                    citing_doi: "10.1111/citing1".to_string(),
                    ref_index: 0,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "publisher".to_string(),
                    match_type: crate::common::types::MatchType::Arxiv,
                    raw_match: "arXiv:2403.00001".to_string(),
                    ref_json: r#"{"unstructured": "ref 1"}"#.to_string(),
                })
                .unwrap();

            partition_writer
                .write(PartitionRow {
                    citing_doi: "10.1111/citing2".to_string(),
                    ref_index: 0,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "mined".to_string(),
                    match_type: crate::common::types::MatchType::Arxiv,
                    raw_match: "arXiv:2403.00002".to_string(),
                    ref_json: r#"{"unstructured": "ref 2"}"#.to_string(),
                })
                .unwrap();

            partition_writer.flush_all().unwrap();
        }

        // Build a simple FST index containing the cited DOI
        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.1234/cited").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        // Create OutputPaths for test
        let outputs = OutputPaths {
            valid: Some(valid_output.clone()),
            failed: Some(failed_output.clone()),
            publisher: None,
            crossref: None,
            mined: None,
        };

        // Run aggregation
        let stats =
            aggregate_and_validate(&partition_dir, Some(&index), &outputs, false, 0).unwrap();

        assert_eq!(stats.valid_count, 1);
        assert_eq!(stats.failed_count, 0);
        assert_eq!(stats.total_citations, 2);

        // Verify output
        let valid_content = std::fs::read_to_string(&valid_output).unwrap();
        assert!(valid_content.contains("10.1234/cited"));
        assert!(valid_content.contains("citation_count\":2"));

        let record: serde_json::Value =
            serde_json::from_str(valid_content.lines().next().unwrap()).unwrap();
        for entry in record["cited_by"].as_array().unwrap() {
            let raw = entry["raw_match"].as_str().unwrap();
            assert!(
                raw.starts_with("arXiv:2403.0000"),
                "raw_match missing: {}",
                entry
            );
        }
    }

    /// The in-memory path must not inherit the (parallel, arbitrary) segment
    /// order: cited_by has to come out in merge-key order, like the
    /// external-sort path.
    #[test]
    fn test_inmemory_partition_orders_citations_deterministically() {
        use polars::prelude::*;

        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("partitions/10.1234");
        fs::create_dir_all(&partition_path).unwrap();
        let valid_output = dir.path().join("valid.jsonl");

        // Rows land in whatever order the parallel extraction produced, both
        // within a segment and across segments.
        for (seg_num, citers) in [
            (0, ["10.1111/c", "10.1111/a"]),
            (1, ["10.1111/d", "10.1111/b"]),
        ] {
            let mut df = DataFrame::new(vec![
                Column::new("citing_doi".into(), &citers),
                Column::new("ref_index".into(), &[0u32, 0u32]),
                Column::new("cited_id".into(), &["10.1234/target", "10.1234/target"]),
                Column::new("provenance".into(), &["mined", "mined"]),
                Column::new(
                    "raw_match".into(),
                    &["arXiv:0000.00000", "arXiv:0000.00000"],
                ),
                Column::new("ref_json".into(), &[r#"{"t": 1}"#, r#"{"t": 2}"#]),
            ])
            .unwrap();
            let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
            ParquetWriter::new(File::create(&path).unwrap())
                .finish(&mut df)
                .unwrap();
        }

        // Feed them in reverse discovery order too, so only the sort can fix it.
        let mut segments: Vec<_> = (0..2)
            .map(|i| partition_path.join(format!("segment_{:04}.parquet", i)))
            .collect();
        segments.reverse();

        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.1234/target").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        let mut valid_writer = Some(BufWriter::new(File::create(&valid_output).unwrap()));
        let mut stats = AggregationStats::default();

        process_partition_inmemory(
            &segments,
            Some(&index),
            &mut valid_writer,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut stats,
        )
        .unwrap();
        valid_writer.as_mut().unwrap().flush().unwrap();

        let content = fs::read_to_string(&valid_output).unwrap();
        let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        let order: Vec<&str> = record["cited_by"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["doi"].as_str().unwrap())
            .collect();

        assert_eq!(record["citation_count"], 4);
        assert_eq!(
            order,
            vec!["10.1111/a", "10.1111/b", "10.1111/c", "10.1111/d"]
        );
    }

    #[test]
    fn test_sort_segments_to_temp() {
        use polars::prelude::*;

        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions/10.1234");
        fs::create_dir_all(&partition_dir).unwrap();

        // Create multiple segment files with unsorted data
        for (seg_num, cited_suffix) in [(0, "zzz"), (1, "aaa"), (2, "mmm")] {
            let mut df = DataFrame::new(vec![
                Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
                Column::new("ref_index".into(), &[0u32]),
                Column::new("cited_id".into(), &[format!("10.1234/{}", cited_suffix)]),
                Column::new("provenance".into(), &["mined"]),
                Column::new("raw_match".into(), &["arXiv:0000.00000"]),
                Column::new("ref_json".into(), &[r#"{"test": true}"#]),
            ])
            .unwrap();

            let path = partition_dir.join(format!("segment_{:04}.parquet", seg_num));
            let file = File::create(&path).unwrap();
            ParquetWriter::new(file).finish(&mut df).unwrap();
        }

        let segments: Vec<_> = fs::read_dir(&partition_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();

        // Sort each segment to its own temp file
        let sorted_files = sort_segments_to_temp(&segments, &partition_dir).unwrap();
        assert_eq!(sorted_files.len(), segments.len());

        let mut all_cited_ids = Vec::new();
        for sorted_path in &sorted_files {
            assert!(sorted_path.exists());
            let sorted_df = LazyFrame::scan_parquet(sorted_path, Default::default())
                .unwrap()
                .collect()
                .unwrap();
            let ids: Vec<String> = sorted_df
                .column("cited_id")
                .unwrap()
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
            let mut sorted_ids = ids.clone();
            sorted_ids.sort();
            assert_eq!(ids, sorted_ids, "each file must be internally sorted");
            all_cited_ids.extend(ids);
        }
        all_cited_ids.sort();
        assert_eq!(
            all_cited_ids,
            vec!["10.1234/aaa", "10.1234/mmm", "10.1234/zzz"]
        );

        for sorted_path in &sorted_files {
            fs::remove_file(sorted_path).unwrap();
        }
    }

    #[test]
    fn test_process_partition_external_sort() {
        use polars::prelude::*;

        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("partitions/10.1234");
        fs::create_dir_all(&partition_path).unwrap();
        let valid_output = dir.path().join("valid.jsonl");

        // Create 3 segment files (simulating a partition that would trigger external sort)
        for seg_num in 0..3 {
            let mut df = DataFrame::new(vec![
                Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
                Column::new("ref_index".into(), &[0u32]),
                Column::new("cited_id".into(), &["10.1234/target"]),
                Column::new("provenance".into(), &["mined"]),
                Column::new("raw_match".into(), &[format!("arXiv:0000.0000{}", seg_num)]),
                Column::new("ref_json".into(), &[r#"{"seg": "test"}"#]),
            ])
            .unwrap();

            let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
            let file = File::create(&path).unwrap();
            ParquetWriter::new(file).finish(&mut df).unwrap();
        }

        let segments: Vec<_> = fs::read_dir(&partition_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();

        // Build FST index
        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.1234/target").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        let mut valid_writer = Some(BufWriter::new(File::create(&valid_output).unwrap()));
        let mut stats = AggregationStats::default();

        process_partition_external_sort(
            &segments,
            &partition_path,
            Some(&index),
            &mut valid_writer,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut stats,
        )
        .unwrap();

        valid_writer.as_mut().unwrap().flush().unwrap();

        // Verify temp files were cleaned up
        assert_no_sorted_temp_files(&partition_path);

        // Verify output
        let content = fs::read_to_string(&valid_output).unwrap();
        let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert_eq!(record["doi"], "10.1234/target");
        assert_eq!(record["citation_count"], 3);

        // The external-sort path builds cited_by in flush_group, not
        // build_cited_by_array_grouped — it has to carry raw_match too.
        let mut raws: Vec<&str> = record["cited_by"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["raw_match"].as_str().unwrap())
            .collect();
        raws.sort_unstable();
        assert_eq!(
            raws,
            vec!["arXiv:0000.00000", "arXiv:0000.00001", "arXiv:0000.00002"]
        );

        assert_eq!(stats.partitions_processed, 1);
        assert_eq!(stats.valid_count, 1);
        assert_eq!(stats.total_citations, 3);
    }

    #[test]
    fn test_cleanup_stale_temp_files() {
        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        let partition_path = partition_dir.join("10.1234");
        fs::create_dir_all(&partition_path).unwrap();

        // Create a stale temp file (simulating interrupted previous run)
        let stale_temp = partition_path.join("_sorted.parquet");
        fs::write(&stale_temp, "stale data").unwrap();
        assert!(stale_temp.exists());

        // Run cleanup
        cleanup_stale_temp_files(&partition_dir).unwrap();

        // Verify temp file was deleted
        assert!(!stale_temp.exists());
    }

    #[test]
    fn test_aggregate_routes_large_partition_to_external_sort() {
        use polars::prelude::*;

        // This test verifies that partitions with >100 segments use external sort
        // We do this by creating 101 segments and checking the temp file gets created/cleaned
        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        let partition_path = partition_dir.join("10.9999");
        fs::create_dir_all(&partition_path).unwrap();
        let valid_output = dir.path().join("valid.jsonl");

        // Create 101 segment files (exceeds SEGMENT_BATCH_THRESHOLD of 100)
        for seg_num in 0..101 {
            let mut df = DataFrame::new(vec![
                Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
                Column::new("ref_index".into(), &[0u32]),
                Column::new("cited_id".into(), &["10.9999/target"]),
                Column::new("provenance".into(), &["mined"]),
                Column::new("raw_match".into(), &["arXiv:0000.00000"]),
                Column::new("ref_json".into(), &[r#"{"test": true}"#]),
            ])
            .unwrap();

            let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
            let file = File::create(&path).unwrap();
            ParquetWriter::new(file).finish(&mut df).unwrap();
        }

        // Build FST index
        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.9999/target").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        let outputs = OutputPaths {
            valid: Some(valid_output.clone()),
            failed: None,
            publisher: None,
            crossref: None,
            mined: None,
        };

        let stats =
            aggregate_and_validate(&partition_dir, Some(&index), &outputs, false, 0).unwrap();

        // Verify temp files were cleaned up
        assert_no_sorted_temp_files(&partition_path);

        // Verify aggregation worked
        assert_eq!(stats.valid_count, 1);
        assert_eq!(stats.total_citations, 101);

        // Verify output
        let content = fs::read_to_string(&valid_output).unwrap();
        let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(record["citation_count"], 101);
    }

    /// Regression for findings C1/C2: overlapping segments through the
    /// external-sort path must produce exactly one record per cited_id.
    #[test]
    fn test_external_sort_one_record_per_cited_id() {
        use polars::prelude::*;

        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("partitions/10.48550");
        fs::create_dir_all(&partition_path).unwrap();
        let valid_output = dir.path().join("valid.jsonl");

        // 3 segments; every segment cites BOTH targets so key ranges fully overlap.
        for seg_num in 0..3 {
            let mut df = DataFrame::new(vec![
                Column::new(
                    "citing_doi".into(),
                    &[
                        format!("10.1111/citing{}a", seg_num),
                        format!("10.1111/citing{}b", seg_num),
                    ],
                ),
                Column::new("ref_index".into(), &[0u32, 0u32]),
                Column::new(
                    "cited_id".into(),
                    &["10.48550/arxiv.2403.00001", "10.48550/arxiv.2403.00002"],
                ),
                Column::new("provenance".into(), &["mined", "mined"]),
                Column::new(
                    "raw_match".into(),
                    &["arXiv:2403.00001", "arXiv:2403.00002"],
                ),
                Column::new("ref_json".into(), &[r#"{"t":1}"#, r#"{"t":2}"#]),
            ])
            .unwrap();
            let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
            ParquetWriter::new(File::create(&path).unwrap())
                .finish(&mut df)
                .unwrap();
        }

        let segments: Vec<_> = fs::read_dir(&partition_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();

        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.48550/arxiv.2403.00001").unwrap();
            builder.insert("10.48550/arxiv.2403.00002").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        let mut valid_writer = Some(BufWriter::new(File::create(&valid_output).unwrap()));
        let mut stats = AggregationStats::default();

        process_partition_external_sort(
            &segments,
            &partition_path,
            Some(&index),
            &mut valid_writer,
            &mut None,
            &mut None,
            &mut None,
            &mut None,
            &mut stats,
        )
        .unwrap();
        valid_writer.as_mut().unwrap().flush().unwrap();

        let content = fs::read_to_string(&valid_output).unwrap();
        let records: Vec<serde_json::Value> = content
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();

        assert_eq!(records.len(), 2, "exactly one record per cited work");
        for rec in &records {
            assert_eq!(
                rec["citation_count"], 3,
                "all 3 segments' citations grouped"
            );
        }
        assert_eq!(stats.total_citations, 6);
    }
}
