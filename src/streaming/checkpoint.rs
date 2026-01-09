use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Pipeline phase for checkpoint tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PipelinePhase {
    /// Phase 1: Converting and extracting from tar.gz
    ConvertExtract,
    /// Phase 2: Inverting partitions
    Invert,
    /// Pipeline completed successfully
    Complete,
}

/// Checkpoint data for resuming pipeline execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Unique identifier for this run
    pub run_id: String,
    /// Current phase of the pipeline
    pub phase: PipelinePhase,
    /// Tar entries processed (for resume in phase 1)
    pub tar_entries_processed: usize,
    /// Partitions that have been fully written (phase 1 complete marker)
    pub partitions_written: HashSet<String>,
    /// Partitions that have been inverted (phase 2 progress)
    pub partitions_inverted: HashSet<String>,
    /// Statistics collected during processing
    pub stats: CheckpointStats,
}

/// Statistics tracked in checkpoint
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointStats {
    pub json_files_processed: usize,
    pub total_records: usize,
    pub total_references: usize,
    pub references_with_matches: usize,
    pub total_arxiv_ids_extracted: usize,
}

impl Checkpoint {
    /// Create a new checkpoint with the given run ID
    pub fn new(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_string(),
            phase: PipelinePhase::ConvertExtract,
            tar_entries_processed: 0,
            partitions_written: HashSet::new(),
            partitions_inverted: HashSet::new(),
            stats: CheckpointStats::default(),
        }
    }

    /// Save checkpoint to file
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self).context("Failed to serialize checkpoint")?;
        fs::write(path, json)
            .with_context(|| format!("Failed to write checkpoint to {:?}", path))?;
        Ok(())
    }

    /// Load checkpoint from file, returning None if file doesn't exist
    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let json = fs::read_to_string(path)
            .with_context(|| format!("Failed to read checkpoint from {:?}", path))?;
        let checkpoint: Self =
            serde_json::from_str(&json).context("Failed to deserialize checkpoint")?;
        Ok(Some(checkpoint))
    }

    /// Mark a partition as inverted (phase 2 complete for this partition)
    pub fn mark_partition_inverted(&mut self, partition: &str) {
        self.partitions_inverted.insert(partition.to_string());
    }

    /// Check if a partition has been inverted
    pub fn is_partition_inverted(&self, partition: &str) -> bool {
        self.partitions_inverted.contains(partition)
    }

    /// Transition to invert phase
    pub fn start_invert_phase(&mut self) {
        self.phase = PipelinePhase::Invert;
    }

    /// Mark pipeline as complete
    pub fn mark_complete(&mut self) {
        self.phase = PipelinePhase::Complete;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_new() {
        let cp = Checkpoint::new("test123");
        assert_eq!(cp.run_id, "test123");
        assert_eq!(cp.phase, PipelinePhase::ConvertExtract);
        assert!(cp.partitions_written.is_empty());
        assert!(cp.partitions_inverted.is_empty());
    }

    #[test]
    fn test_checkpoint_save_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut cp = Checkpoint::new("run123");
        cp.partitions_written.insert("2403".to_string());
        cp.stats.total_references = 1000;
        cp.save(&path).unwrap();

        let loaded = Checkpoint::load(&path).unwrap().unwrap();
        assert_eq!(loaded.run_id, "run123");
        assert!(loaded.partitions_written.contains("2403"));
        assert_eq!(loaded.stats.total_references, 1000);
    }

    #[test]
    fn test_checkpoint_load_nonexistent() {
        let result = Checkpoint::load(Path::new("/nonexistent/path.json")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_partition_tracking() {
        let mut cp = Checkpoint::new("test");

        cp.partitions_written.insert("2403".to_string());
        cp.mark_partition_inverted("2403");

        assert!(cp.partitions_written.contains("2403"));
        assert!(cp.is_partition_inverted("2403"));
        assert!(!cp.is_partition_inverted("2404"));
    }
}
