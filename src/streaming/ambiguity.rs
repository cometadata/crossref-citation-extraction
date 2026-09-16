//! Sorted, deduplicated ambiguity output.
//!
//! Extraction writes ambiguous URL evidence as JSONL segments, one row per
//! sighting of a shared landing-page URL that no DOI in the reference could
//! resolve. This module merges every segment in a directory into one output
//! sorted by `(citing_doi, ref_index, url)`, keeping one row per key. Two
//! rows for the same key must agree on their candidate DOIs; anything else
//! means the segments were not produced by one extraction and is an error.
//!
//! Rows are held in memory up to the aggregation budget, spilled as sorted
//! runs, and merged with bounded fan-in. The result is committed through
//! [`super::partition_commit`] so a retry reuses it after verification, and
//! the final file is assembled through a temporary path and a rename.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::checkpoint::AggregationStatsSnapshot;
use super::limits::AggregationLimits;
use super::partition_commit::{
    aggregation_dir, hit_failpoint, remove_stale_staging, sync_parent, tmp_output_path,
    PartitionCommit, PartitionStaging, FAILPOINT_DURING_FINAL_ASSEMBLY,
};

/// Name of the committed directory holding the aggregated ambiguity file.
pub const AMBIGUITY_PARTITION: &str = "ambiguities";
/// The single fragment inside that directory.
const FRAGMENT: &str = "ambiguities.jsonl";
/// Scratch subdirectory for sorted runs inside the staging directory.
const RUNS_DIR: &str = "runs";
/// Extension of ambiguity segment files (an unfinished `.part` never matches).
const SEGMENT_EXTENSION: &str = "jsonl";

/// One ambiguous URL sighting, as extraction writes it and as it is output.
#[derive(Debug, Serialize, Deserialize)]
struct AmbiguityRow {
    citing_doi: String,
    ref_index: u32,
    url: String,
    raw_match: String,
    candidate_dois: Vec<String>,
    reference: Box<RawValue>,
}

impl AmbiguityRow {
    fn same_key(&self, other: &Self) -> bool {
        self.ref_index == other.ref_index
            && self.citing_doi == other.citing_doi
            && self.url == other.url
    }

    /// Bytes charged against the memory budget while the row is buffered.
    fn accounted_bytes(&self) -> usize {
        self.citing_doi.capacity()
            + self.url.capacity()
            + self.raw_match.capacity()
            + self
                .candidate_dois
                .iter()
                .map(|doi| doi.capacity() + size_of::<String>())
                .sum::<usize>()
            + self.reference.get().len()
            + size_of::<Self>()
    }
}

/// The sort key, then the fields that make ties deterministic.
fn compare_rows(a: &AmbiguityRow, b: &AmbiguityRow) -> Ordering {
    a.citing_doi
        .cmp(&b.citing_doi)
        .then_with(|| a.ref_index.cmp(&b.ref_index))
        .then_with(|| a.url.cmp(&b.url))
        .then_with(|| a.candidate_dois.cmp(&b.candidate_dois))
        .then_with(|| a.raw_match.cmp(&b.raw_match))
        .then_with(|| a.reference.get().cmp(b.reference.get()))
}

/// Two sightings of one key must name the same candidates.
fn check_agreement(a: &AmbiguityRow, b: &AmbiguityRow) -> Result<()> {
    ensure!(
        a.candidate_dois == b.candidate_dois,
        "conflicting candidate lists for citing DOI {} reference {} URL {}: {:?} and {:?}",
        a.citing_doi,
        a.ref_index,
        a.url,
        a.candidate_dois,
        b.candidate_dois
    );
    Ok(())
}

/// Sort, check every duplicate key for agreement, and keep one row per key.
fn sort_and_dedup(rows: &mut Vec<AmbiguityRow>) -> Result<()> {
    rows.sort_unstable_by(compare_rows);
    for pair in rows.windows(2) {
        if pair[0].same_key(&pair[1]) {
            check_agreement(&pair[0], &pair[1])?;
        }
    }
    rows.dedup_by(|later, earlier| later.same_key(earlier));
    Ok(())
}

fn write_row<W: Write>(writer: &mut W, row: &AmbiguityRow) -> Result<()> {
    serde_json::to_writer(&mut *writer, row).context("failed to serialize ambiguity row")?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Every committed `*.jsonl` segment in `input_dir`, in name order; none
/// when the directory does not exist.
fn ambiguity_segments(input_dir: &Path) -> Result<Vec<PathBuf>> {
    if !input_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut segments: Vec<PathBuf> = fs::read_dir(input_dir)
        .with_context(|| format!("Failed to list {}", input_dir.display()))?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| {
            path.is_file() && path.extension().and_then(|e| e.to_str()) == Some(SEGMENT_EXTENSION)
        })
        .collect();
    segments.sort();
    Ok(segments)
}

/// A sorted-run reader that yields rows in file order.
struct RunReader {
    path: PathBuf,
    reader: BufReader<File>,
    line: String,
    line_number: usize,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("Failed to open run {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            reader: BufReader::new(file),
            line: String::new(),
            line_number: 0,
        })
    }

    fn next_row(&mut self) -> Result<Option<AmbiguityRow>> {
        loop {
            self.line.clear();
            if self.reader.read_line(&mut self.line)? == 0 {
                return Ok(None);
            }
            self.line_number += 1;
            if self.line.trim().is_empty() {
                continue;
            }
            let row = serde_json::from_str(&self.line)
                .with_context(|| format!("line {} of {}", self.line_number, self.path.display()))?;
            return Ok(Some(row));
        }
    }
}

type Key = (String, u32, String);

fn owned_key(row: &AmbiguityRow) -> Key {
    (row.citing_doi.clone(), row.ref_index, row.url.clone())
}

/// Merge sorted runs into `sink`, one row per key, checking agreement.
/// Returns the number of rows emitted.
fn merge_runs(
    inputs: &[PathBuf],
    sink: &mut dyn FnMut(&AmbiguityRow) -> Result<()>,
) -> Result<u64> {
    let mut readers = inputs
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heads: Vec<Option<AmbiguityRow>> = Vec::with_capacity(readers.len());
    let mut heap: BinaryHeap<Reverse<(Key, usize)>> = BinaryHeap::new();
    for (i, reader) in readers.iter_mut().enumerate() {
        let head = reader.next_row()?;
        if let Some(row) = &head {
            heap.push(Reverse((owned_key(row), i)));
        }
        heads.push(head);
    }

    let mut pending: Option<AmbiguityRow> = None;
    let mut emitted = 0u64;
    while let Some(Reverse((_, i))) = heap.pop() {
        let row = heads[i].take().expect("heap entry implies a buffered row");
        if let Some(next) = readers[i].next_row()? {
            heap.push(Reverse((owned_key(&next), i)));
            heads[i] = Some(next);
        }
        match pending.as_mut() {
            Some(best) if best.same_key(&row) => {
                check_agreement(best, &row)?;
                if compare_rows(&row, best) == Ordering::Less {
                    *best = row;
                }
            }
            _ => {
                if let Some(done) = pending.take() {
                    sink(&done)?;
                    emitted += 1;
                }
                pending = Some(row);
            }
        }
    }
    if let Some(done) = pending {
        sink(&done)?;
        emitted += 1;
    }
    Ok(emitted)
}

/// Copy `input` into `output`, returning `(bytes, newlines)`.
fn copy_counting_lines<R: Read, W: Write>(input: &mut R, output: &mut W) -> Result<(u64, u64)> {
    let mut buffer = vec![0u8; 64 * 1024];
    let mut bytes = 0u64;
    let mut lines = 0u64;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            return Ok((bytes, lines));
        }
        lines += buffer[..read].iter().filter(|b| **b == b'\n').count() as u64;
        output.write_all(&buffer[..read])?;
        bytes += read as u64;
    }
}

fn count_lines(path: &Path) -> Result<u64> {
    let mut file = File::open(path)?;
    let (_, lines) = copy_counting_lines(&mut file, &mut std::io::sink())?;
    Ok(lines)
}

/// Sort and dedup `rows` into a new run file, emptying `rows`.
fn spill_run(rows: &mut Vec<AmbiguityRow>, runs_dir: &Path, index: usize) -> Result<PathBuf> {
    sort_and_dedup(rows)?;
    fs::create_dir_all(runs_dir)?;
    let path = runs_dir.join(format!("run_p0_{index:06}.jsonl"));
    let mut writer = BufWriter::new(File::create(&path)?);
    for row in rows.iter() {
        write_row(&mut writer, row)?;
    }
    writer.flush()?;
    debug!(
        "Spilled ambiguity run {} ({} rows)",
        path.display(),
        rows.len()
    );
    rows.clear();
    Ok(path)
}

/// Read every segment, sort under the budget, and write the deduplicated
/// rows to the staging fragment.
fn sort_segments_into(
    segments: &[PathBuf],
    limits: &AggregationLimits,
    staging: &mut PartitionStaging,
) -> Result<()> {
    let runs_dir = staging.dir().join(RUNS_DIR);
    let mut runs: Vec<PathBuf> = Vec::new();
    let mut rows: Vec<AmbiguityRow> = Vec::new();
    let mut buffered = 0usize;

    for segment in segments {
        let reader = BufReader::new(
            File::open(segment).with_context(|| format!("Failed to open {}", segment.display()))?,
        );
        for (index, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("Failed to read {}", segment.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            let mut row: AmbiguityRow = serde_json::from_str(&line)
                .with_context(|| format!("line {} of {}", index + 1, segment.display()))?;
            row.candidate_dois.sort();
            row.candidate_dois.dedup();
            let cost = row.accounted_bytes();
            ensure!(
                cost <= limits.memory_bytes,
                "ambiguity row for citing DOI {} reference {} needs {cost} bytes, over the {}-byte aggregation memory budget",
                row.citing_doi,
                row.ref_index,
                limits.memory_bytes
            );
            if buffered + cost > limits.memory_bytes {
                runs.push(spill_run(&mut rows, &runs_dir, runs.len())?);
                buffered = 0;
            }
            buffered += cost;
            rows.push(row);
        }
    }

    if runs.is_empty() {
        sort_and_dedup(&mut rows)?;
        if !rows.is_empty() {
            let writer = staging.writer(FRAGMENT)?;
            for row in &rows {
                write_row(writer, row)?;
            }
        }
        return Ok(());
    }
    if !rows.is_empty() {
        runs.push(spill_run(&mut rows, &runs_dir, runs.len())?);
    }
    drop(rows);

    let mut pass = 1;
    while runs.len() > limits.merge_fan_in {
        let mut next = Vec::with_capacity(runs.len().div_ceil(limits.merge_fan_in));
        for (group_index, group) in runs.chunks(limits.merge_fan_in).enumerate() {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let path = runs_dir.join(format!("run_p{pass}_{group_index:06}.jsonl"));
            let mut writer = BufWriter::new(File::create(&path)?);
            let emitted = merge_runs(group, &mut |row| write_row(&mut writer, row))?;
            writer.flush()?;
            drop(writer);
            let lines = count_lines(&path)?;
            ensure!(
                lines == emitted,
                "merged run {} holds {lines} rows but {emitted} were written",
                path.display()
            );
            // The replacement is closed and verified; its inputs are redundant.
            for input in group {
                fs::remove_file(input)
                    .with_context(|| format!("Failed to remove run {}", input.display()))?;
            }
            next.push(path);
        }
        debug!(
            "Ambiguity merge pass {pass} reduced {} runs to {}",
            runs.len(),
            next.len()
        );
        runs = next;
        pass += 1;
    }

    merge_runs(&runs, &mut |row| write_row(staging.writer(FRAGMENT)?, row))?;
    fs::remove_dir_all(&runs_dir)
        .with_context(|| format!("Failed to remove {}", runs_dir.display()))?;
    Ok(())
}

/// Assemble `output` from the committed fragment (or nothing) through a
/// temporary file and a rename. Returns the number of rows written.
fn write_output(fragment: Option<(PathBuf, u64)>, output: &Path) -> Result<u64> {
    let tmp = tmp_output_path(output);
    let mut writer = BufWriter::new(
        File::create(&tmp).with_context(|| format!("Failed to create {}", tmp.display()))?,
    );
    let mut rows = 0;
    if let Some((path, expected_size)) = fragment {
        let mut input =
            File::open(&path).with_context(|| format!("Failed to open {}", path.display()))?;
        let (copied, lines) = copy_counting_lines(&mut input, &mut writer)?;
        ensure!(
            copied == expected_size,
            "fragment {} is {copied} bytes but its commit recorded {expected_size}",
            path.display()
        );
        rows = lines;
    }
    writer.flush()?;
    writer
        .into_inner()
        .map_err(|e| e.into_error())?
        .sync_all()
        .with_context(|| format!("Failed to sync {}", tmp.display()))?;
    hit_failpoint(FAILPOINT_DURING_FINAL_ASSEMBLY)?;
    fs::rename(&tmp, output)
        .with_context(|| format!("Failed to move {} to {}", tmp.display(), output.display()))?;
    sync_parent(output)?;
    Ok(rows)
}

/// Aggregate every `*.jsonl` ambiguity segment in `input_dir` into
/// `output`, sorted by `(citing_doi, ref_index, url)` with one row per key.
///
/// Rows for one key with different candidate lists are an error. The
/// result is committed under `<input_dir>/_aggregation/ambiguities/` so a
/// run with `resume` reuses it once its identity and digest verify; without
/// `resume` any previous commit is discarded first. When no segment exists,
/// `output` is created empty. Returns the number of rows in `output`.
pub fn aggregate_ambiguities(
    input_dir: &Path,
    output: &Path,
    limits: &AggregationLimits,
    identity_sha256: &str,
    resume: bool,
) -> Result<u64> {
    limits.validate()?;
    ensure!(
        !identity_sha256.is_empty(),
        "aggregation identity must not be empty"
    );
    let segments = ambiguity_segments(input_dir)?;
    if segments.is_empty() {
        info!(
            "No ambiguity segments under {}; writing an empty {}",
            input_dir.display(),
            output.display()
        );
        return write_output(None, output);
    }

    let agg_dir = aggregation_dir(input_dir);
    if !resume && agg_dir.exists() {
        info!(
            "Discarding previous ambiguity aggregation under {}",
            agg_dir.display()
        );
        fs::remove_dir_all(&agg_dir)
            .with_context(|| format!("Failed to remove {}", agg_dir.display()))?;
    }
    fs::create_dir_all(&agg_dir)
        .with_context(|| format!("Failed to create {}", agg_dir.display()))?;
    remove_stale_staging(&agg_dir)?;

    let committed = agg_dir.join(AMBIGUITY_PARTITION);
    let commit =
        match PartitionCommit::load_verified(&committed, identity_sha256).with_context(|| {
            format!(
                "verifying committed ambiguity output {}; rerun without resume to discard it",
                committed.display()
            )
        })? {
            Some(commit) => {
                info!("Reusing committed ambiguity output {}", committed.display());
                commit
            }
            None => {
                info!(
                    "Aggregating {} ambiguity segments under {}",
                    segments.len(),
                    input_dir.display()
                );
                let mut staging = PartitionStaging::create(&agg_dir, AMBIGUITY_PARTITION)?;
                sort_segments_into(&segments, limits, &mut staging)?;
                let (_, commit) =
                    staging.commit(identity_sha256, AggregationStatsSnapshot::default())?;
                commit
            }
        };
    let fragment = commit
        .files
        .get(FRAGMENT)
        .map(|file| (committed.join(FRAGMENT), file.size));
    let rows = write_output(fragment, output)?;
    info!("Wrote {rows} ambiguity rows to {}", output.display());
    Ok(rows)
}
