//! K-way streaming merge over individually-sorted parquet segments.

use anyhow::Result;
use polars::prelude::*;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};

use super::partitioning::PartitionMode;
use super::segmented_writer::{decode_segment_frame, PartitionRow};
use crate::common::types::MatchType;

/// One logical row from a sorted segment.
#[derive(Debug)]
pub struct MergeRow {
    pub cited_id: String,
    pub citing_doi: String,
    pub ref_index: u32,
    pub provenance: String,
    pub match_type: MatchType,
    pub raw_match: String,
    pub ref_json: String,
}

impl From<PartitionRow> for MergeRow {
    fn from(row: PartitionRow) -> Self {
        Self {
            cited_id: row.cited_id,
            citing_doi: row.citing_doi,
            ref_index: row.ref_index,
            provenance: row.provenance,
            match_type: row.match_type,
            raw_match: row.raw_match,
            ref_json: row.ref_json,
        }
    }
}

/// Sort/dedup key: (cited_id, citing_doi, ref_index).
pub type RowKey = (String, String, u32);

impl MergeRow {
    pub fn key(&self) -> RowKey {
        (
            self.cited_id.clone(),
            self.citing_doi.clone(),
            self.ref_index,
        )
    }
}

/// Get row count from parquet file metadata (reads only footer, not data).
pub fn get_parquet_row_count(path: &Path) -> Result<usize> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| anyhow::anyhow!("Failed to read parquet metadata: {}", e))?;
    Ok(reader.metadata().file_metadata().num_rows() as usize)
}

/// Buffered cursor over one sorted segment file.
struct SegmentCursor {
    path: PathBuf,
    total_rows: usize,
    next_offset: usize,
    chunk_rows: usize,
    mode: PartitionMode,
    buf: VecDeque<MergeRow>,
}

impl SegmentCursor {
    fn new(path: &Path, chunk_rows: usize, mode: PartitionMode) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            total_rows: get_parquet_row_count(path)?,
            next_offset: 0,
            chunk_rows,
            mode,
            buf: VecDeque::new(),
        })
    }

    fn refill(&mut self) -> Result<()> {
        if self.next_offset >= self.total_rows {
            return Ok(());
        }
        let chunk = LazyFrame::scan_parquet(&self.path, Default::default())?
            .slice(self.next_offset as i64, self.chunk_rows as u32)
            .collect()
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read chunk at offset {} of {}: {}",
                    self.next_offset,
                    self.path.display(),
                    e
                )
            })?;
        let n = chunk.height();
        if n == 0 {
            self.next_offset = self.total_rows;
            return Ok(());
        }
        for row in decode_segment_frame(&chunk, self.mode, &self.path)? {
            self.buf.push_back(MergeRow::from(row));
        }
        self.next_offset += n;
        Ok(())
    }

    fn peek_key(&mut self) -> Result<Option<RowKey>> {
        if self.buf.is_empty() {
            self.refill()?;
        }
        Ok(self.buf.front().map(|r| r.key()))
    }

    fn pop(&mut self) -> Result<Option<MergeRow>> {
        if self.buf.is_empty() {
            self.refill()?;
        }
        Ok(self.buf.pop_front())
    }
}

/// Merge k sorted segment files into one globally-sorted emission stream.
///
/// Each input file must be sorted by (cited_id, citing_doi, ref_index).
/// Exact-duplicate keys (across or within segments) are emitted once.
/// `mode` decides how a segment without a `match_type` column is read:
/// legacy segments default to arXiv evidence, hashed (full) mode rejects them.
/// Returns the number of rows emitted after dedup.
pub fn merge_sorted_segments<F>(
    segments: &[PathBuf],
    chunk_rows: usize,
    mode: PartitionMode,
    mut emit: F,
) -> Result<u64>
where
    F: FnMut(MergeRow) -> Result<()>,
{
    // A zero-row chunk reads nothing, which the EOF check would misread as
    // exhaustion — silently emitting no rows at all. A chunk larger than u32
    // cannot be passed to the parquet slice below without truncating, and a
    // multiple of 2^32 would truncate to zero: the same silent-EOF failure.
    if chunk_rows == 0 || chunk_rows > u32::MAX as usize {
        anyhow::bail!(
            "chunk_rows must be > 0 and <= {} (got {})",
            u32::MAX,
            chunk_rows
        );
    }

    let mut cursors: Vec<SegmentCursor> = segments
        .iter()
        .map(|p| SegmentCursor::new(p, chunk_rows, mode))
        .collect::<Result<_>>()?;

    // Min-heap of (key, cursor index).
    let mut heap: BinaryHeap<Reverse<(RowKey, usize)>> = BinaryHeap::new();
    for (i, cursor) in cursors.iter_mut().enumerate() {
        if let Some(key) = cursor.peek_key()? {
            heap.push(Reverse((key, i)));
        }
    }

    let mut last_key: Option<RowKey> = None;
    let mut emitted = 0u64;

    while let Some(Reverse((_, i))) = heap.pop() {
        let row = cursors[i]
            .pop()?
            .expect("heap entry implies cursor has a row");
        if let Some(next_key) = cursors[i].peek_key()? {
            heap.push(Reverse((next_key, i)));
        }

        let key = row.key();
        if last_key.as_ref() == Some(&key) {
            continue; // exact duplicate of previous row
        }
        last_key = Some(key);
        emit(row)?;
        emitted += 1;
    }

    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Write a sorted segment file with the current 7-column schema.
    /// Rows are (cited_id, citing_doi, ref_index) tuples;
    /// provenance/match_type/raw_match/ref_json filled.
    fn write_segment(path: &Path, rows: &[(&str, &str, u32)]) {
        write_segment_with_schema(path, rows, true);
    }

    /// Write a segment in the pre-`match_type` 6-column layout.
    fn write_old_segment(path: &Path, rows: &[(&str, &str, u32)]) {
        write_segment_with_schema(path, rows, false);
    }

    fn write_segment_with_schema(path: &Path, rows: &[(&str, &str, u32)], with_match_type: bool) {
        let mut columns = vec![
            Column::new(
                "citing_doi".into(),
                rows.iter().map(|r| r.1.to_string()).collect::<Vec<_>>(),
            ),
            Column::new(
                "ref_index".into(),
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "cited_id".into(),
                rows.iter().map(|r| r.0.to_string()).collect::<Vec<_>>(),
            ),
            Column::new(
                "provenance".into(),
                rows.iter().map(|_| "mined".to_string()).collect::<Vec<_>>(),
            ),
            Column::new(
                "raw_match".into(),
                rows.iter()
                    .map(|r| format!("arXiv:{}", r.0))
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "ref_json".into(),
                rows.iter().map(|_| "{}".to_string()).collect::<Vec<_>>(),
            ),
        ];
        if with_match_type {
            columns.push(Column::new(
                "match_type".into(),
                rows.iter().map(|_| "doi").collect::<Vec<_>>(),
            ));
        }
        let mut df = DataFrame::new(columns).unwrap();
        let file = File::create(path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    fn collect_merge(segments: &[PathBuf], chunk_rows: usize) -> Vec<(String, String, u32)> {
        let mut out = Vec::new();
        merge_sorted_segments(segments, chunk_rows, PartitionMode::Legacy, |row| {
            out.push((row.cited_id, row.citing_doi, row.ref_index));
            Ok(())
        })
        .unwrap();
        out
    }

    /// Regression for finding C1: interleaved key ranges across segments must
    /// come out globally sorted, even when each cursor needs many refills.
    #[test]
    fn test_interleaved_segments_merge_sorted() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        // Mirrors the audit trace: A = [1, 2, 9], B = [5, 6, 7].
        write_segment(&a, &[("id1", "c", 0), ("id2", "c", 0), ("id9", "c", 0)]);
        write_segment(&b, &[("id5", "c", 0), ("id6", "c", 0), ("id7", "c", 0)]);

        // chunk_rows = 1 forces a refill on every row — the old code's failure mode.
        let merged = collect_merge(&[a, b], 1);
        let ids: Vec<&str> = merged.iter().map(|r| r.0.as_str()).collect();
        assert_eq!(ids, vec!["id1", "id2", "id5", "id6", "id7", "id9"]);
    }

    /// Exact-duplicate rows appearing in different segments are emitted once
    /// (cross-segment dedup, part of finding C5).
    #[test]
    fn test_cross_segment_duplicates_emitted_once() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_segment(&a, &[("id1", "citer", 0), ("id2", "citer", 1)]);
        write_segment(&b, &[("id1", "citer", 0), ("id3", "citer", 0)]);

        let merged = collect_merge(&[a, b], 2);
        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged,
            vec![
                ("id1".into(), "citer".into(), 0),
                ("id2".into(), "citer".into(), 1),
                ("id3".into(), "citer".into(), 0),
            ]
        );
    }

    /// Same cited_id from different citers is NOT a duplicate.
    #[test]
    fn test_same_cited_different_citers_all_kept() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_segment(&a, &[("id1", "citerA", 0)]);
        write_segment(&b, &[("id1", "citerB", 0)]);

        let merged = collect_merge(&[a, b], 8);
        assert_eq!(merged.len(), 2);
    }

    /// Empty segment files don't wedge or terminate the merge.
    #[test]
    fn test_empty_segment_is_skipped() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_segment(&a, &[]);
        write_segment(&b, &[("id1", "c", 0)]);

        let merged = collect_merge(&[a, b], 8);
        assert_eq!(merged.len(), 1);
    }

    /// `chunk_rows == 0` would read empty chunks forever-looking-like-EOF and
    /// silently emit nothing; it must be rejected instead.
    #[test]
    fn test_zero_chunk_rows_is_rejected() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        write_segment(&a, &[("id1", "c", 0)]);

        let err = merge_sorted_segments(&[a], 0, PartitionMode::Legacy, |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("chunk_rows must be > 0"));
    }

    /// A chunk size past u32 would be truncated by the parquet slice below
    /// (and a multiple of 2^32 truncates to 0, reading as instant EOF), so it
    /// must be rejected rather than silently narrowed.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_oversized_chunk_rows_is_rejected() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        write_segment(&a, &[("id1", "c", 0)]);

        let too_big = u32::MAX as usize + 1;
        let err =
            merge_sorted_segments(&[a], too_big, PartitionMode::Legacy, |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("chunk_rows must be > 0"));
    }

    /// `raw_match` has to survive the merge: the aggregator reads it off
    /// MergeRow to build each cited_by entry.
    #[test]
    fn test_raw_match_survives_merge() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        write_segment(&a, &[("id1", "c", 0), ("id2", "c", 0)]);

        let mut raws = Vec::new();
        merge_sorted_segments(&[a], 1, PartitionMode::Legacy, |row| {
            raws.push(row.raw_match);
            Ok(())
        })
        .unwrap();
        assert_eq!(raws, vec!["arXiv:id1", "arXiv:id2"]);
    }

    /// `match_type` rides along with every merged row in either mode.
    #[test]
    fn test_match_type_survives_merge() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        write_segment(&a, &[("id1", "c", 0), ("id2", "c", 0)]);

        for mode in [PartitionMode::Legacy, PartitionMode::Hashed { count: 1 }] {
            let mut types = Vec::new();
            merge_sorted_segments(std::slice::from_ref(&a), 1, mode, |row| {
                types.push(row.match_type);
                Ok(())
            })
            .unwrap();
            assert_eq!(types, vec![MatchType::Doi, MatchType::Doi]);
        }
    }

    /// A segment without the column is arXiv evidence in legacy mode and a
    /// schema error in hashed (full) mode.
    #[test]
    fn test_old_schema_defaults_in_legacy_and_fails_in_hashed_mode() {
        let dir = tempdir().unwrap();
        let old = dir.path().join("old.parquet");
        write_old_segment(&old, &[("id1", "c", 0)]);

        let mut types = Vec::new();
        merge_sorted_segments(
            std::slice::from_ref(&old),
            8,
            PartitionMode::Legacy,
            |row| {
                types.push(row.match_type);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(types, vec![MatchType::Arxiv]);

        let err = merge_sorted_segments(
            std::slice::from_ref(&old),
            8,
            PartitionMode::Hashed { count: 1 },
            |_| Ok(()),
        )
        .unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("match_type"), "{message}");
        assert!(message.contains("old.parquet"), "{message}");
    }

    #[test]
    fn test_get_parquet_row_count() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("x.parquet");
        write_segment(&p, &[("id1", "c", 0), ("id2", "c", 0)]);
        assert_eq!(get_parquet_row_count(&p).unwrap(), 2);
    }
}
