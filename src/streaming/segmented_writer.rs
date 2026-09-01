use anyhow::{Context, Result};
use log::debug;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use super::partition_key;

/// A row for partitioned storage
#[derive(Debug, Clone)]
pub struct PartitionRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub cited_id: String,
    pub provenance: String,
    pub raw_match: String,
    pub ref_json: String,
}

/// Buffer for accumulating rows before writing a segment
struct SegmentBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    cited_ids: Vec<String>,
    provenances: Vec<String>,
    raw_matches: Vec<String>,
    ref_jsons: Vec<String>,
}

impl SegmentBuffer {
    fn new() -> Self {
        Self {
            citing_dois: Vec::new(),
            ref_indices: Vec::new(),
            cited_ids: Vec::new(),
            provenances: Vec::new(),
            raw_matches: Vec::new(),
            ref_jsons: Vec::new(),
        }
    }

    fn push(&mut self, row: PartitionRow) {
        self.citing_dois.push(row.citing_doi);
        self.ref_indices.push(row.ref_index);
        self.cited_ids.push(row.cited_id);
        self.provenances.push(row.provenance);
        self.raw_matches.push(row.raw_match);
        self.ref_jsons.push(row.ref_json);
    }

    fn len(&self) -> usize {
        self.citing_dois.len()
    }

    fn is_empty(&self) -> bool {
        self.citing_dois.is_empty()
    }

    /// Write the buffered rows to a parquet segment, consuming the buffers
    /// (they are left empty, ready to accumulate the next segment).
    fn write_parquet(&mut self, path: &Path) -> Result<()> {
        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), std::mem::take(&mut self.citing_dois)),
            Column::new("ref_index".into(), std::mem::take(&mut self.ref_indices)),
            Column::new("cited_id".into(), std::mem::take(&mut self.cited_ids)),
            Column::new("provenance".into(), std::mem::take(&mut self.provenances)),
            Column::new("raw_match".into(), std::mem::take(&mut self.raw_matches)),
            Column::new("ref_json".into(), std::mem::take(&mut self.ref_jsons)),
        ])
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;

        let file = File::create(path)
            .with_context(|| format!("Failed to create segment file: {:?}", path))?;

        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .context("Failed to write segment parquet")?;

        Ok(())
    }
}

/// Writes partition rows to append-only segment files
pub struct SegmentedPartitionWriter {
    partition_dir: PathBuf,
    buffers: HashMap<String, SegmentBuffer>,
    segment_counts: HashMap<String, u32>,
    flush_threshold: usize,
    total_rows_written: usize,
}

impl SegmentedPartitionWriter {
    /// Create a new segmented partition writer
    pub fn new(partition_dir: &Path, flush_threshold: usize) -> Result<Self> {
        fs::create_dir_all(partition_dir).with_context(|| {
            format!("Failed to create partition directory: {:?}", partition_dir)
        })?;

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            buffers: HashMap::new(),
            segment_counts: HashMap::new(),
            flush_threshold,
            total_rows_written: 0,
        })
    }

    /// Write a row to the appropriate partition
    pub fn write(&mut self, row: PartitionRow) -> Result<()> {
        let key = partition_key(&row.cited_id);
        let buffer = self
            .buffers
            .entry(key.clone())
            .or_insert_with(SegmentBuffer::new);

        buffer.push(row);

        if buffer.len() >= self.flush_threshold {
            self.flush_partition(&key)?;
        }

        Ok(())
    }

    /// Flush a specific partition to a new segment file
    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let buffer = match self.buffers.get_mut(key) {
            Some(b) if !b.is_empty() => b,
            _ => return Ok(()),
        };

        let segment_num = self.segment_counts.entry(key.to_string()).or_insert(0);

        let partition_path = self.partition_dir.join(key);
        fs::create_dir_all(&partition_path)?;

        let segment_path = partition_path.join(format!("segment_{:04}.parquet", segment_num));
        let rows_in_segment = buffer.len();
        buffer.write_parquet(&segment_path)?;

        debug!(
            "Wrote segment {} for partition {} ({} rows)",
            segment_num, key, rows_in_segment
        );

        *segment_num += 1;
        self.total_rows_written += rows_in_segment;

        Ok(())
    }

    /// Flush all remaining buffered data
    pub fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.buffers.keys().cloned().collect();
        for key in keys {
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    /// Get total rows written across all partitions
    pub fn total_rows_written(&self) -> usize {
        self.total_rows_written
    }

    /// Get number of partitions
    pub fn partition_count(&self) -> usize {
        self.segment_counts.len()
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
}
