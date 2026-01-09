use anyhow::{Context, Result};
use log::{debug, info};
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use super::partition_key;
use crate::extract::Provenance;

/// A single extracted and exploded row ready for partitioning
#[derive(Debug, Clone)]
pub struct ExplodedRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub ref_json: String,
    pub raw_match: String,
    pub cited_id: String,
    pub provenance: Provenance,
}

/// Buffer for a single partition
struct PartitionBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    ref_jsons: Vec<String>,
    raw_matches: Vec<String>,
    cited_ids: Vec<String>,
    provenances: Vec<String>,
    file_path: PathBuf,
    rows_written: usize,
}

impl PartitionBuffer {
    fn new(partition_dir: &Path, partition_name: &str) -> Self {
        let file_path = partition_dir.join(format!("{}.parquet", partition_name));
        Self {
            citing_dois: Vec::new(),
            ref_indices: Vec::new(),
            ref_jsons: Vec::new(),
            raw_matches: Vec::new(),
            cited_ids: Vec::new(),
            provenances: Vec::new(),
            file_path,
            rows_written: 0,
        }
    }

    fn len(&self) -> usize {
        self.citing_dois.len()
    }

    fn push(&mut self, row: ExplodedRow) {
        self.citing_dois.push(row.citing_doi);
        self.ref_indices.push(row.ref_index);
        self.ref_jsons.push(row.ref_json);
        self.raw_matches.push(row.raw_match);
        self.cited_ids.push(row.cited_id);
        self.provenances.push(row.provenance.as_str().to_string());
    }

    fn to_dataframe(&self) -> Result<DataFrame> {
        DataFrame::new(vec![
            Column::new("citing_doi".into(), &self.citing_dois),
            Column::new("ref_index".into(), &self.ref_indices),
            Column::new("ref_json".into(), &self.ref_jsons),
            Column::new("raw_match".into(), &self.raw_matches),
            Column::new("cited_id".into(), &self.cited_ids),
            Column::new("provenance".into(), &self.provenances),
        ])
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))
    }

    fn clear(&mut self) {
        self.citing_dois.clear();
        self.ref_indices.clear();
        self.ref_jsons.clear();
        self.raw_matches.clear();
        self.cited_ids.clear();
        self.provenances.clear();
    }
}

/// Manages writing extracted rows to partitioned Parquet files
pub struct PartitionWriter {
    partition_dir: PathBuf,
    buffers: HashMap<String, PartitionBuffer>,
    flush_threshold: usize,
    total_rows_written: usize,
}

impl PartitionWriter {
    /// Create a new partition writer
    ///
    /// # Arguments
    /// * `partition_dir` - Directory to store partition files
    /// * `flush_threshold` - Number of rows per partition before flushing to disk
    pub fn new(partition_dir: &Path, flush_threshold: usize) -> Result<Self> {
        fs::create_dir_all(partition_dir).with_context(|| {
            format!("Failed to create partition directory: {:?}", partition_dir)
        })?;

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            buffers: HashMap::new(),
            flush_threshold,
            total_rows_written: 0,
        })
    }

    /// Write an exploded row to the appropriate partition
    pub fn write(&mut self, row: ExplodedRow) -> Result<()> {
        let partition = partition_key(&row.cited_id);

        let buffer = self
            .buffers
            .entry(partition.clone())
            .or_insert_with(|| PartitionBuffer::new(&self.partition_dir, &partition));

        buffer.push(row);

        if buffer.len() >= self.flush_threshold {
            self.flush_partition(&partition)?;
        }

        Ok(())
    }

    /// Write multiple rows from a reference extraction (handles exploding)
    pub fn write_extracted_ref(
        &mut self,
        citing_doi: &str,
        ref_index: u32,
        ref_json: &str,
        raw_matches: &[String],
        cited_ids: &[String],
        provenances: &[Provenance],
    ) -> Result<usize> {
        let mut written = 0;
        for ((raw_match, cited_id), provenance) in
            raw_matches.iter().zip(cited_ids.iter()).zip(provenances.iter())
        {
            self.write(ExplodedRow {
                citing_doi: citing_doi.to_string(),
                ref_index,
                ref_json: ref_json.to_string(),
                raw_match: raw_match.clone(),
                cited_id: cited_id.clone(),
                provenance: *provenance,
            })?;
            written += 1;
        }
        Ok(written)
    }

    /// Flush a specific partition to disk
    fn flush_partition(&mut self, partition: &str) -> Result<()> {
        let buffer = self
            .buffers
            .get_mut(partition)
            .ok_or_else(|| anyhow::anyhow!("Partition {} not found", partition))?;

        if buffer.len() == 0 {
            return Ok(());
        }

        let mut df = buffer.to_dataframe()?;
        let rows_in_batch = df.height();

        // Append to existing file or create new one
        if buffer.file_path.exists() {
            // Read existing, concat, and rewrite
            // This is simpler than managing append-mode Parquet
            let existing = LazyFrame::scan_parquet(&buffer.file_path, Default::default())
                .context("Failed to read existing partition file")?
                .collect()
                .context("Failed to collect existing partition data")?;

            df = concat([existing.lazy(), df.lazy()], UnionArgs::default())
                .context("Failed to concat dataframes")?
                .collect()
                .context("Failed to collect concatenated dataframe")?;
        }

        let file = File::create(&buffer.file_path)
            .with_context(|| format!("Failed to create partition file: {:?}", buffer.file_path))?;

        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(100_000))
            .finish(&mut df)
            .context("Failed to write partition parquet")?;

        buffer.rows_written += rows_in_batch;
        self.total_rows_written += rows_in_batch;
        buffer.clear();

        debug!(
            "Flushed partition {} ({} rows, {} total)",
            partition, rows_in_batch, buffer.rows_written
        );

        Ok(())
    }

    /// Flush all partition buffers to disk
    pub fn flush_all(&mut self) -> Result<()> {
        let partitions: Vec<String> = self.buffers.keys().cloned().collect();
        for partition in partitions {
            self.flush_partition(&partition)?;
        }
        info!(
            "Flushed all partitions ({} total rows)",
            self.total_rows_written
        );
        Ok(())
    }

    /// Get count of unique partitions
    #[allow(dead_code)]
    pub fn partition_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_partition_writer_basic() {
        let dir = tempdir().unwrap();
        let mut writer = PartitionWriter::new(dir.path(), 10).unwrap();

        writer
            .write(ExplodedRow {
                citing_doi: "10.1234/test".to_string(),
                ref_index: 0,
                ref_json: "{}".to_string(),
                raw_match: "arXiv:2403.12345".to_string(),
                cited_id: "2403.12345".to_string(),
                provenance: Provenance::Mined,
            })
            .unwrap();

        writer.flush_all().unwrap();

        assert!(dir.path().join("2403.parquet").exists());
    }

    #[test]
    fn test_partition_writer_multiple_partitions() {
        let dir = tempdir().unwrap();
        let mut writer = PartitionWriter::new(dir.path(), 100).unwrap();

        // Modern format
        writer
            .write(ExplodedRow {
                citing_doi: "10.1234/a".to_string(),
                ref_index: 0,
                ref_json: "{}".to_string(),
                raw_match: "arXiv:2403.12345".to_string(),
                cited_id: "2403.12345".to_string(),
                provenance: Provenance::Mined,
            })
            .unwrap();

        // Old format
        writer
            .write(ExplodedRow {
                citing_doi: "10.1234/b".to_string(),
                ref_index: 1,
                ref_json: "{}".to_string(),
                raw_match: "arXiv:hep-ph/9901234".to_string(),
                cited_id: "hep-ph/9901234".to_string(),
                provenance: Provenance::Mined,
            })
            .unwrap();

        writer.flush_all().unwrap();

        assert!(dir.path().join("2403.parquet").exists());
        assert!(dir.path().join("hep-.parquet").exists());
        assert_eq!(writer.partition_count(), 2);
    }

    #[test]
    fn test_write_extracted_ref() {
        let dir = tempdir().unwrap();
        let mut writer = PartitionWriter::new(dir.path(), 100).unwrap();

        let written = writer
            .write_extracted_ref(
                "10.1234/test",
                0,
                "{}",
                &[
                    "arXiv:2403.12345".to_string(),
                    "arXiv:2403.67890".to_string(),
                ],
                &["2403.12345".to_string(), "2403.67890".to_string()],
                &[Provenance::Mined, Provenance::Mined],
            )
            .unwrap();

        assert_eq!(written, 2);
        writer.flush_all().unwrap();
    }

    #[test]
    fn test_partition_writer_with_provenance() {
        let dir = tempdir().unwrap();
        let mut writer = PartitionWriter::new(dir.path(), 10).unwrap();

        writer
            .write(ExplodedRow {
                citing_doi: "10.1234/test".to_string(),
                ref_index: 0,
                ref_json: "{}".to_string(),
                raw_match: "10.5678/cited".to_string(),
                cited_id: "10.5678/cited".to_string(),
                provenance: Provenance::Publisher,
            })
            .unwrap();

        writer.flush_all().unwrap();

        // Verify parquet has provenance column
        // Partition key for DOI is the prefix (10.5678)
        let df = LazyFrame::scan_parquet(dir.path().join("10.5678.parquet"), Default::default())
            .unwrap()
            .collect()
            .unwrap();

        assert!(df.column("provenance").is_ok());
        let prov = df.column("provenance").unwrap().str().unwrap();
        assert_eq!(prov.get(0).unwrap(), "publisher");
    }
}
