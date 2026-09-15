//! Append-only partitioned segment storage for extracted citation rows.
//!
//! Rows are buffered per partition and written as Parquet segments. Buffering
//! is bounded by a global byte budget across every partition as well as a
//! per-partition row cap, so no single DOI prefix or partition can grow memory
//! unbounded. Each segment is written to a `.part` file, synced, then renamed
//! into place, so a committed segment is either complete or absent.

use anyhow::{bail, ensure, Context, Result};
use log::{debug, info};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use super::limits::WriterLimits;
use super::partitioning::PartitionMode;
use crate::common::types::MatchType;

/// Name of the evidence-kind column in segment files.
pub const MATCH_TYPE_COLUMN: &str = "match_type";

const SEGMENT_PREFIX: &str = "segment_";
const SEGMENT_SUFFIX: &str = ".parquet";
const PART_SUFFIX: &str = ".part";

/// A row for partitioned storage
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub cited_id: String,
    pub provenance: String,
    pub match_type: MatchType,
    pub raw_match: String,
    pub ref_json: String,
}

/// Bytes one row occupies in the column vectors, independent of string
/// contents: five string headers, the reference index, and the match type.
const ROW_SLOT_BYTES: usize = 5 * size_of::<String>() + size_of::<u32>() + size_of::<MatchType>();

impl PartitionRow {
    /// Bytes this row is charged against a buffer budget: the heap capacity
    /// of every string plus its fixed column-slot footprint. Batch budgeting
    /// upstream of the writer should use the same figure.
    pub fn accounted_bytes(&self) -> usize {
        self.heap_bytes() + ROW_SLOT_BYTES
    }

    fn heap_bytes(&self) -> usize {
        self.citing_doi.capacity()
            + self.cited_id.capacity()
            + self.provenance.capacity()
            + self.raw_match.capacity()
            + self.ref_json.capacity()
    }
}

/// File name of committed segment `index` within a partition directory.
pub fn segment_file_name(index: u32) -> String {
    format!("{SEGMENT_PREFIX}{index:04}{SEGMENT_SUFFIX}")
}

/// Segment index of a committed segment file name, if `name` is one.
pub fn segment_index(name: &str) -> Option<u32> {
    let digits = name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Whether `name` is an unfinished segment this writer would have produced.
fn is_owned_part_file(name: &str) -> bool {
    name.strip_suffix(PART_SUFFIX)
        .and_then(segment_index)
        .is_some()
}

/// Committed segment files of one partition directory, in segment order.
pub fn committed_segments(partition_path: &Path) -> Result<Vec<PathBuf>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(partition_path)
        .with_context(|| format!("Failed to list partition {}", partition_path.display()))?
    {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(index) = segment_index(name) {
            segments.push((index, path));
        }
    }
    segments.sort();
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

/// Decode the `match_type` column of a segment frame.
///
/// Segments written before the column existed hold arXiv matches only, so
/// legacy mode fills in [`MatchType::Arxiv`]. Hashed mode is the full DataCite
/// layout, where every segment must carry the column.
pub fn segment_match_types(
    frame: &DataFrame,
    mode: PartitionMode,
    path: &Path,
) -> Result<Vec<MatchType>> {
    if frame.get_column_index(MATCH_TYPE_COLUMN).is_none() {
        return match mode {
            PartitionMode::Legacy => Ok(vec![MatchType::Arxiv; frame.height()]),
            PartitionMode::Hashed { .. } => bail!(
                "segment {} lacks the {MATCH_TYPE_COLUMN} column; full mode rejects an old schema",
                path.display()
            ),
        };
    }
    let values = frame
        .column(MATCH_TYPE_COLUMN)?
        .str()
        .with_context(|| format!("{MATCH_TYPE_COLUMN} column of {}", path.display()))?;
    values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let value = value.with_context(|| {
                format!("null {MATCH_TYPE_COLUMN} at row {i} of {}", path.display())
            })?;
            MatchType::parse(value).with_context(|| format!("row {i} of {}", path.display()))
        })
        .collect()
}

/// Decode every row of a segment frame under `mode` (see
/// [`segment_match_types`] for the schema rules).
pub fn decode_segment_frame(
    frame: &DataFrame,
    mode: PartitionMode,
    path: &Path,
) -> Result<Vec<PartitionRow>> {
    let citing = frame.column("citing_doi")?.str()?;
    let refi = frame.column("ref_index")?.u32()?;
    let cited = frame.column("cited_id")?.str()?;
    let prov = frame.column("provenance")?.str()?;
    let raw = frame.column("raw_match")?.str()?;
    let json = frame.column("ref_json")?.str()?;
    let match_types = segment_match_types(frame, mode, path)?;
    Ok((0..frame.height())
        .map(|i| PartitionRow {
            citing_doi: citing.get(i).unwrap_or("").to_string(),
            ref_index: refi.get(i).unwrap_or(0),
            cited_id: cited.get(i).unwrap_or("").to_string(),
            provenance: prov.get(i).unwrap_or("mined").to_string(),
            match_type: match_types[i],
            raw_match: raw.get(i).unwrap_or("").to_string(),
            ref_json: json.get(i).unwrap_or("null").to_string(),
        })
        .collect())
}

/// Read every row of one committed segment.
pub fn read_segment(path: &Path, mode: PartitionMode) -> Result<Vec<PartitionRow>> {
    let frame = LazyFrame::scan_parquet(path, Default::default())
        .and_then(LazyFrame::collect)
        .with_context(|| format!("Failed to read segment {}", path.display()))?;
    decode_segment_frame(&frame, mode, path)
}

/// Push onto a vector that grows by doubling from one slot, so the slot
/// bytes a push will add are known before it happens (see `slot_growth`).
fn push_reserved<T>(values: &mut Vec<T>, value: T) {
    if values.len() == values.capacity() {
        values.reserve_exact(values.capacity().max(1));
    }
    values.push(value);
}

/// Slot bytes the next `push_reserved` adds to `values`.
fn slot_growth<T>(values: &Vec<T>) -> usize {
    if values.len() == values.capacity() {
        values.capacity().max(1) * size_of::<T>()
    } else {
        0
    }
}

/// Buffer for accumulating rows before writing a segment
#[derive(Default)]
struct SegmentBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    cited_ids: Vec<String>,
    provenances: Vec<String>,
    match_types: Vec<MatchType>,
    raw_matches: Vec<String>,
    ref_jsons: Vec<String>,
    /// Sum of the heap capacities of every buffered string.
    string_bytes: usize,
}

impl SegmentBuffer {
    fn push(&mut self, row: PartitionRow) {
        self.string_bytes += row.heap_bytes();
        push_reserved(&mut self.citing_dois, row.citing_doi);
        push_reserved(&mut self.ref_indices, row.ref_index);
        push_reserved(&mut self.cited_ids, row.cited_id);
        push_reserved(&mut self.provenances, row.provenance);
        push_reserved(&mut self.match_types, row.match_type);
        push_reserved(&mut self.raw_matches, row.raw_match);
        push_reserved(&mut self.ref_jsons, row.ref_json);
    }

    fn len(&self) -> usize {
        self.citing_dois.len()
    }

    /// Bytes reserved for column slots, whether or not they hold a row.
    fn slot_bytes(&self) -> usize {
        self.citing_dois.capacity() * size_of::<String>()
            + self.ref_indices.capacity() * size_of::<u32>()
            + self.cited_ids.capacity() * size_of::<String>()
            + self.provenances.capacity() * size_of::<String>()
            + self.match_types.capacity() * size_of::<MatchType>()
            + self.raw_matches.capacity() * size_of::<String>()
            + self.ref_jsons.capacity() * size_of::<String>()
    }

    /// Everything this buffer is charged against the writer budget.
    fn accounted_bytes(&self) -> usize {
        self.string_bytes + self.slot_bytes()
    }

    /// Slot bytes the next push adds beyond the row's own strings.
    fn growth_bytes(&self) -> usize {
        slot_growth(&self.citing_dois)
            + slot_growth(&self.ref_indices)
            + slot_growth(&self.cited_ids)
            + slot_growth(&self.provenances)
            + slot_growth(&self.match_types)
            + slot_growth(&self.raw_matches)
            + slot_growth(&self.ref_jsons)
    }

    /// Write the buffered rows to `path`, syncing the file before returning.
    fn write_parquet(self, path: &Path) -> Result<()> {
        let match_types: Vec<&str> = self.match_types.iter().map(MatchType::as_str).collect();
        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), self.citing_dois),
            Column::new("ref_index".into(), self.ref_indices),
            Column::new("cited_id".into(), self.cited_ids),
            Column::new("provenance".into(), self.provenances),
            Column::new(MATCH_TYPE_COLUMN.into(), match_types),
            Column::new("raw_match".into(), self.raw_matches),
            Column::new("ref_json".into(), self.ref_jsons),
        ])
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;

        let mut file = File::create(path)
            .with_context(|| format!("Failed to create segment file: {:?}", path))?;

        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .context("Failed to write segment parquet")?;

        file.sync_all()
            .with_context(|| format!("Failed to sync segment file: {:?}", path))?;
        Ok(())
    }
}

/// Make a directory's entries durable after a rename into it.
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("Failed to sync directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Next segment index per partition from the committed segments on disk;
/// removes unfinished `.part` segments left by an interrupted writer.
fn recover_partitions(partition_dir: &Path) -> Result<BTreeMap<String, u32>> {
    let mut next_segment = BTreeMap::new();
    for entry in fs::read_dir(partition_dir)? {
        let partition_path = entry?.path();
        if !partition_path.is_dir() {
            continue;
        }
        let Some(key) = partition_path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let mut highest: Option<u32> = None;
        for file in fs::read_dir(&partition_path)? {
            let file_path = file?.path();
            let Some(name) = file_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(index) = segment_index(name) {
                highest = Some(highest.map_or(index, |h| h.max(index)));
            } else if is_owned_part_file(name) {
                info!(
                    "Removing unfinished segment from an interrupted run: {}",
                    file_path.display()
                );
                fs::remove_file(&file_path).with_context(|| {
                    format!("Failed to remove unfinished segment {:?}", file_path)
                })?;
            }
        }
        if let Some(highest) = highest {
            let next = highest
                .checked_add(1)
                .with_context(|| format!("segment counter overflow in partition {key}"))?;
            next_segment.insert(key.to_string(), next);
        }
    }
    Ok(next_segment)
}

/// Writes partition rows to append-only segment files
pub struct SegmentedPartitionWriter {
    partition_dir: PathBuf,
    limits: WriterLimits,
    buffers: BTreeMap<String, SegmentBuffer>,
    /// Index the next segment of each partition receives; partitions found on
    /// disk at creation start past their committed segments.
    next_segment: BTreeMap<String, u32>,
    buffered_bytes: usize,
    total_rows_written: usize,
}

impl SegmentedPartitionWriter {
    /// Create a legacy prefix-keyed writer that flushes a partition once it
    /// holds `flush_threshold` rows, with no global byte budget.
    pub fn new(partition_dir: &Path, flush_threshold: usize) -> Result<Self> {
        Self::with_limits(
            partition_dir,
            WriterLimits {
                memory_bytes: usize::MAX,
                segment_rows: flush_threshold.max(1),
                mode: PartitionMode::Legacy,
            },
        )
    }

    /// Create a writer under explicit limits, resuming segment numbering from
    /// any segments already committed under `partition_dir`.
    pub fn with_limits(partition_dir: &Path, limits: WriterLimits) -> Result<Self> {
        limits.validate()?;
        fs::create_dir_all(partition_dir).with_context(|| {
            format!("Failed to create partition directory: {:?}", partition_dir)
        })?;
        let next_segment = recover_partitions(partition_dir).with_context(|| {
            format!(
                "Failed to recover partition state under {}",
                partition_dir.display()
            )
        })?;

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            limits,
            buffers: BTreeMap::new(),
            next_segment,
            buffered_bytes: 0,
            total_rows_written: 0,
        })
    }

    /// Write a row to the appropriate partition
    pub fn write(&mut self, row: PartitionRow) -> Result<()> {
        let key = self.limits.mode.key_for(&row.cited_id)?;
        let row_bytes = row.accounted_bytes();
        ensure!(
            row_bytes <= self.limits.memory_bytes,
            "row for {} needs {} bytes but the writer memory budget is {} bytes",
            row.cited_id,
            row_bytes,
            self.limits.memory_bytes
        );

        // Make room before admitting the row: evict the largest buffers until
        // the row's strings and any slot growth fit under the budget.
        let heap_bytes = row.heap_bytes();
        while self
            .buffered_bytes
            .saturating_add(self.admission_cost(&key, heap_bytes))
            > self.limits.memory_bytes
        {
            let victim = self
                .largest_buffer()
                .context("writer budget exceeded with no buffered rows")?;
            self.flush_partition(&victim)?;
        }

        let buffer = self.buffers.entry(key.clone()).or_default();
        let before = buffer.accounted_bytes();
        buffer.push(row);
        let after = buffer.accounted_bytes();
        self.buffered_bytes += after - before;

        if buffer.len() >= self.limits.segment_rows {
            self.flush_partition(&key)?;
        }
        // The projection above is exact for our growth policy; this guards
        // the invariant should an allocator ever reserve more than asked.
        while self.buffered_bytes > self.limits.memory_bytes {
            let victim = self
                .largest_buffer()
                .context("writer budget exceeded with no buffered rows")?;
            self.flush_partition(&victim)?;
        }
        Ok(())
    }

    /// Bytes admitting a row with `heap_bytes` of strings adds to `key`.
    fn admission_cost(&self, key: &str, heap_bytes: usize) -> usize {
        let growth = self
            .buffers
            .get(key)
            .map_or(ROW_SLOT_BYTES, SegmentBuffer::growth_bytes);
        heap_bytes.saturating_add(growth)
    }

    /// Partition holding the most accounted bytes; ties go to the first key.
    fn largest_buffer(&self) -> Option<String> {
        self.buffers
            .iter()
            .max_by(|(a_key, a), (b_key, b)| {
                a.accounted_bytes()
                    .cmp(&b.accounted_bytes())
                    .then_with(|| b_key.cmp(a_key))
            })
            .map(|(key, _)| key.clone())
    }

    /// Flush a specific partition to a new segment file
    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let Some(buffer) = self.buffers.remove(key) else {
            return Ok(());
        };
        let rows_in_segment = buffer.len();
        self.buffered_bytes -= buffer.accounted_bytes();
        if rows_in_segment == 0 {
            return Ok(());
        }

        let segment_num = self.next_segment.get(key).copied().unwrap_or(0);
        let partition_path = self.partition_dir.join(key);
        fs::create_dir_all(&partition_path)?;

        // Write to a .part file and rename so a committed segment is either
        // complete or absent; the directory sync makes the rename durable.
        let segment_path = partition_path.join(segment_file_name(segment_num));
        let part_path =
            partition_path.join(format!("{}{PART_SUFFIX}", segment_file_name(segment_num)));
        buffer.write_parquet(&part_path)?;
        fs::rename(&part_path, &segment_path).with_context(|| {
            format!(
                "Failed to commit segment {} from {}",
                segment_path.display(),
                part_path.display()
            )
        })?;
        sync_dir(&partition_path)?;

        debug!(
            "Wrote segment {} for partition {} ({} rows)",
            segment_num, key, rows_in_segment
        );

        let next = segment_num
            .checked_add(1)
            .with_context(|| format!("segment counter overflow in partition {key}"))?;
        self.next_segment.insert(key.to_string(), next);
        self.total_rows_written += rows_in_segment;

        Ok(())
    }

    /// Flush all remaining buffered data
    pub fn flush_all(&mut self) -> Result<()> {
        while let Some(key) = self.buffers.keys().next().cloned() {
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    /// Accounted bytes currently buffered across all partitions.
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Get total rows written across all partitions
    pub fn total_rows_written(&self) -> usize {
        self.total_rows_written
    }

    /// Get number of partitions
    pub fn partition_count(&self) -> usize {
        self.next_segment.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segmented_writer_creates_segments() {
        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 2).unwrap();

        // Write 5 rows to same partition - should create 2 segments (2+2) + 1 on flush
        for i in 0..5 {
            writer
                .write(PartitionRow {
                    citing_doi: format!("10.1111/citing{}", i),
                    ref_index: i as u32,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "mined".to_string(),
                    match_type: MatchType::Arxiv,
                    raw_match: "arXiv:0000.00000".to_string(),
                    ref_json: format!(r#"{{"idx": {}}}"#, i),
                })
                .unwrap();
        }
        writer.flush_all().unwrap();

        // Check segments exist
        let partition_dir = dir.path().join("10.1234");
        assert!(partition_dir.exists());
        assert!(partition_dir.join("segment_0000.parquet").exists());
        assert!(partition_dir.join("segment_0001.parquet").exists());
        assert!(partition_dir.join("segment_0002.parquet").exists());

        assert_eq!(writer.total_rows_written(), 5);
    }

    #[test]
    fn test_segmented_writer_multiple_partitions() {
        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 10).unwrap();

        writer
            .write(PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: 0,
                cited_id: "10.1234/cited1".to_string(),
                provenance: "publisher".to_string(),
                match_type: MatchType::Arxiv,
                raw_match: "arXiv:0000.00000".to_string(),
                ref_json: r#"{"test": 1}"#.to_string(),
            })
            .unwrap();

        writer
            .write(PartitionRow {
                citing_doi: "10.1111/b".to_string(),
                ref_index: 0,
                cited_id: "10.5678/cited2".to_string(),
                provenance: "mined".to_string(),
                match_type: MatchType::Arxiv,
                raw_match: "arXiv:0000.00000".to_string(),
                ref_json: r#"{"test": 2}"#.to_string(),
            })
            .unwrap();

        writer.flush_all().unwrap();

        assert!(dir.path().join("10.1234").exists());
        assert!(dir.path().join("10.5678").exists());
        assert_eq!(writer.partition_count(), 2);
    }

    #[test]
    fn test_segments_carry_raw_match_column() {
        use polars::prelude::*;

        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 10).unwrap();

        writer
            .write(PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: 0,
                cited_id: "10.48550/arXiv.2403.03542".to_string(),
                provenance: "mined".to_string(),
                match_type: MatchType::Arxiv,
                raw_match: "arXiv:2403.03542".to_string(),
                ref_json: r#"{"unstructured": "see arXiv:2403.03542"}"#.to_string(),
            })
            .unwrap();
        writer.flush_all().unwrap();

        let segment = dir.path().join("2403").join("segment_0000.parquet");
        let df = LazyFrame::scan_parquet(&segment, Default::default())
            .unwrap()
            .collect()
            .unwrap();
        let raws = df.column("raw_match").unwrap().str().unwrap();
        assert_eq!(raws.get(0), Some("arXiv:2403.03542"));
    }

    #[test]
    fn test_segment_file_names_round_trip() {
        assert_eq!(segment_file_name(7), "segment_0007.parquet");
        assert_eq!(segment_index("segment_0007.parquet"), Some(7));
        assert_eq!(segment_index("segment_12345.parquet"), Some(12345));
        assert_eq!(segment_index("segment_0007.parquet.part"), None);
        assert_eq!(segment_index("_sorted_00000.parquet"), None);
        assert_eq!(segment_index("segment_.parquet"), None);
        assert_eq!(segment_index("segment_x.parquet"), None);
        assert!(is_owned_part_file("segment_0007.parquet.part"));
        assert!(!is_owned_part_file("segment_0007.parquet"));
        assert!(!is_owned_part_file("other.part"));
    }

    #[test]
    fn test_flush_leaves_no_part_files_and_budget_is_exact() {
        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 1_000).unwrap();
        let mut charged = 0;
        for i in 0..3 {
            let row = PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: i,
                cited_id: "10.1234/cited".to_string(),
                provenance: "mined".to_string(),
                match_type: MatchType::Arxiv,
                raw_match: "arXiv:0000.00000".to_string(),
                ref_json: "{}".to_string(),
            };
            charged += row.heap_bytes();
            writer.write(row).unwrap();
        }
        // Three rows: slots grow 1 -> 2 -> 4, so four slots are reserved.
        assert_eq!(writer.buffered_bytes(), charged + 4 * ROW_SLOT_BYTES);
        writer.flush_all().unwrap();
        assert_eq!(writer.buffered_bytes(), 0);

        let names: Vec<String> = fs::read_dir(dir.path().join("10.1234"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["segment_0000.parquet"]);
    }
}
