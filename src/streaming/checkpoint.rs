//! Checkpoint support for resumable pipeline execution.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::aggregator::AggregationStats;

/// Checkpoint for extraction phase - tracks completed tar.gz entries.
/// Uses a simple line-per-filename format for efficient append operations.
pub struct ExtractionCheckpoint {
    path: PathBuf,
    completed: HashSet<String>,
    writer: BufWriter<File>,
}

impl ExtractionCheckpoint {
    /// Create or load an extraction checkpoint.
    /// If the file exists, loads completed filenames into memory.
    /// Opens file in append mode for marking new completions.
    pub fn new(path: &Path) -> Result<Self> {
        let mut completed = HashSet::new();

        // Load existing checkpoint if present
        if path.exists() {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let filename = line?;
                if !filename.is_empty() {
                    completed.insert(filename);
                }
            }
        }

        // Open in append mode for writing new completions
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path: path.to_path_buf(),
            completed,
            writer,
        })
    }

    /// Check if a filename has already been processed.
    #[inline]
    pub fn is_completed(&self, filename: &str) -> bool {
        self.completed.contains(filename)
    }

    /// Mark a filename as completed and persist to disk.
    pub fn mark_completed(&mut self, filename: &str) -> Result<()> {
        if self.completed.insert(filename.to_string()) {
            writeln!(self.writer, "{}", filename)?;
            self.writer.flush()?;
        }
        Ok(())
    }

    /// Get count of completed files.
    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }

    /// Clone the completed set (for sharing with a reader thread).
    pub fn completed_snapshot(&self) -> HashSet<String> {
        self.completed.clone()
    }

    /// Delete the checkpoint file (called on successful completion).
    pub fn cleanup(self) -> Result<()> {
        drop(self.writer);
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

/// Checkpoint for aggregation phase - tracks completed partitions and stats.
/// Uses JSON format for atomic writes with full state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationCheckpoint {
    pub completed_partitions: Vec<String>,
    pub stats: AggregationStatsSnapshot,
}

/// Serializable snapshot of aggregation stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregationStatsSnapshot {
    pub partitions_processed: usize,
    pub valid_count: usize,
    pub failed_count: usize,
    pub total_citations: usize,
    pub publisher_citations: usize,
    pub crossref_citations: usize,
    pub mined_citations: usize,
}

impl From<&AggregationStats> for AggregationStatsSnapshot {
    fn from(stats: &AggregationStats) -> Self {
        Self {
            partitions_processed: stats.partitions_processed,
            valid_count: stats.valid_count,
            failed_count: stats.failed_count,
            total_citations: stats.total_citations,
            publisher_citations: stats.publisher_citations,
            crossref_citations: stats.crossref_citations,
            mined_citations: stats.mined_citations,
        }
    }
}

impl From<AggregationStatsSnapshot> for AggregationStats {
    fn from(snapshot: AggregationStatsSnapshot) -> Self {
        Self {
            partitions_processed: snapshot.partitions_processed,
            valid_count: snapshot.valid_count,
            failed_count: snapshot.failed_count,
            total_citations: snapshot.total_citations,
            publisher_citations: snapshot.publisher_citations,
            crossref_citations: snapshot.crossref_citations,
            mined_citations: snapshot.mined_citations,
        }
    }
}

impl AggregationCheckpoint {
    /// Load checkpoint from file if it exists.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(path)?;
        let checkpoint: AggregationCheckpoint = serde_json::from_reader(file)?;
        Ok(Some(checkpoint))
    }

    /// Save checkpoint atomically (write to .tmp, then rename).
    pub fn save(
        path: &Path,
        completed_partitions: &[String],
        stats: &AggregationStats,
    ) -> Result<()> {
        let checkpoint = AggregationCheckpoint {
            completed_partitions: completed_partitions.to_vec(),
            stats: AggregationStatsSnapshot::from(stats),
        };

        let tmp_path = path.with_extension("checkpoint.tmp");
        let file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(file, &checkpoint)?;
        fs::rename(&tmp_path, path)?;

        Ok(())
    }

    /// Delete checkpoint file (called on successful completion).
    pub fn cleanup(path: &Path) -> Result<()> {
        if path.exists() {
            fs::remove_file(path)?;
        }
        // Also clean up any stale .tmp file
        let tmp_path = path.with_extension("checkpoint.tmp");
        if tmp_path.exists() {
            fs::remove_file(&tmp_path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_extraction_checkpoint_new_file() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("extraction.checkpoint");

        let checkpoint = ExtractionCheckpoint::new(&checkpoint_path).unwrap();
        assert_eq!(checkpoint.completed_count(), 0);
        assert!(!checkpoint.is_completed("file1.json"));
    }

    #[test]
    fn test_extraction_checkpoint_mark_completed() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("extraction.checkpoint");

        {
            let mut checkpoint = ExtractionCheckpoint::new(&checkpoint_path).unwrap();
            checkpoint.mark_completed("file1.json").unwrap();
            checkpoint.mark_completed("file2.json").unwrap();
            assert_eq!(checkpoint.completed_count(), 2);
            assert!(checkpoint.is_completed("file1.json"));
            assert!(checkpoint.is_completed("file2.json"));
            assert!(!checkpoint.is_completed("file3.json"));
        }

        // Reload and verify persistence
        let checkpoint = ExtractionCheckpoint::new(&checkpoint_path).unwrap();
        assert_eq!(checkpoint.completed_count(), 2);
        assert!(checkpoint.is_completed("file1.json"));
        assert!(checkpoint.is_completed("file2.json"));
    }

    #[test]
    fn test_extraction_checkpoint_no_duplicates() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("extraction.checkpoint");

        let mut checkpoint = ExtractionCheckpoint::new(&checkpoint_path).unwrap();
        checkpoint.mark_completed("file1.json").unwrap();
        checkpoint.mark_completed("file1.json").unwrap(); // Duplicate
        assert_eq!(checkpoint.completed_count(), 1);
    }

    #[test]
    fn test_extraction_checkpoint_cleanup() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("extraction.checkpoint");

        let mut checkpoint = ExtractionCheckpoint::new(&checkpoint_path).unwrap();
        checkpoint.mark_completed("file1.json").unwrap();
        assert!(checkpoint_path.exists());

        checkpoint.cleanup().unwrap();
        assert!(!checkpoint_path.exists());
    }

    #[test]
    fn test_aggregation_checkpoint_roundtrip() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("aggregation.checkpoint");

        let stats = AggregationStats {
            partitions_processed: 100,
            valid_count: 50,
            failed_count: 10,
            total_citations: 1000,
            publisher_citations: 400,
            crossref_citations: 300,
            mined_citations: 300,
        };
        let completed = vec!["10.1234".to_string(), "10.5678".to_string()];

        AggregationCheckpoint::save(&checkpoint_path, &completed, &stats).unwrap();

        let loaded = AggregationCheckpoint::load(&checkpoint_path)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.completed_partitions, completed);
        assert_eq!(loaded.stats.partitions_processed, 100);
        assert_eq!(loaded.stats.valid_count, 50);
        assert_eq!(loaded.stats.total_citations, 1000);
    }

    #[test]
    fn test_aggregation_checkpoint_load_missing() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("nonexistent.checkpoint");

        let loaded = AggregationCheckpoint::load(&checkpoint_path).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_aggregation_checkpoint_atomic_write() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("aggregation.checkpoint");
        let tmp_path = checkpoint_path.with_extension("checkpoint.tmp");

        let stats = AggregationStats::default();
        let completed = vec!["10.1234".to_string()];

        AggregationCheckpoint::save(&checkpoint_path, &completed, &stats).unwrap();

        // Tmp file should not exist after successful save
        assert!(!tmp_path.exists());
        assert!(checkpoint_path.exists());
    }

    #[test]
    fn test_aggregation_checkpoint_cleanup() {
        let dir = tempdir().unwrap();
        let checkpoint_path = dir.path().join("aggregation.checkpoint");

        let stats = AggregationStats::default();
        AggregationCheckpoint::save(&checkpoint_path, &[], &stats).unwrap();
        assert!(checkpoint_path.exists());

        AggregationCheckpoint::cleanup(&checkpoint_path).unwrap();
        assert!(!checkpoint_path.exists());
    }
}
