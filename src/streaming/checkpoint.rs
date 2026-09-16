//! Checkpoint support for resumable pipeline execution.

use anyhow::{bail, ensure, Context, Result};
use log::warn;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::aggregator::AggregationStats;

/// The `kind` every [`MemberCheckpointHeader`] carries.
pub const MEMBER_CHECKPOINT_KIND: &str = "datacite-extraction-checkpoint";

/// First line of a [`MemberCheckpoint`]: what the checkpointed rows were
/// produced under. A resume whose header differs cannot reuse the rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberCheckpointHeader {
    pub kind: String,
    pub identity_sha256: String,
    /// Selected output kinds, sorted.
    pub outputs: Vec<String>,
    /// Provenance filter, lowercase and sorted; empty means all.
    pub provenance: Vec<String>,
}

/// One line of a [`MemberCheckpoint`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum MemberRecord<S> {
    Header(MemberCheckpointHeader),
    Member { filename: String, stats: S },
    Complete,
}

/// Append-only checkpoint of completed source members for full-mode
/// extraction: a header line, one line per member with its counters, and a
/// final completion line once every member is durable.
///
/// A member line is appended only after the member's rows are synced, so
/// the counters of a finished run are exactly the sum over these lines. A
/// crash may leave a torn final line; it is dropped on load and the file is
/// truncated back to the last complete record before anything is appended.
pub struct MemberCheckpoint<S> {
    path: PathBuf,
    header: MemberCheckpointHeader,
    members: Vec<(String, S)>,
    names: HashSet<String>,
    complete: bool,
    file: File,
}

impl<S: Serialize + DeserializeOwned> MemberCheckpoint<S> {
    /// Start a fresh checkpoint at `path`, replacing any existing file.
    pub fn create(path: &Path, header: MemberCheckpointHeader) -> Result<Self> {
        let mut file = File::create(path)
            .with_context(|| format!("Failed to create checkpoint {}", path.display()))?;
        let line = record_line(&MemberRecord::<S>::Header(header.clone()))?;
        file.write_all(line.as_bytes())
            .and_then(|_| file.sync_all())
            .with_context(|| format!("Failed to write checkpoint {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            header,
            members: Vec::new(),
            names: HashSet::new(),
            complete: false,
            file: open_append(path)?,
        })
    }

    /// Load the checkpoint at `path` for a resumed run. The stored header
    /// must equal `header`; a missing or empty file starts fresh.
    pub fn open(path: &Path, header: MemberCheckpointHeader) -> Result<Self> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e).with_context(|| format!("Failed to read {}", path.display())),
        };
        if bytes.is_empty() {
            return Self::create(path, header);
        }
        let text = std::str::from_utf8(&bytes)
            .with_context(|| format!("{} is not UTF-8", path.display()))?;

        // Everything after the last newline is a torn record; a final
        // complete line that does not parse is treated the same way. Both
        // are dropped and the file truncated so appends start on a clean
        // line. Anything unparsable earlier is corruption.
        let mut keep = text.rfind('\n').map_or(0, |i| i + 1);
        if keep < text.len() {
            warn!(
                "Ignoring incomplete trailing checkpoint record in {}",
                path.display()
            );
        }
        let mut lines: Vec<&str> = text[..keep].split_terminator('\n').collect();
        if let Some(last) = lines.last() {
            if serde_json::from_str::<MemberRecord<S>>(last).is_err() {
                warn!(
                    "Ignoring unparsable final checkpoint record in {}",
                    path.display()
                );
                keep -= last.len() + 1;
                lines.pop();
            }
        }
        let records = lines
            .iter()
            .enumerate()
            .map(|(index, line)| {
                serde_json::from_str::<MemberRecord<S>>(line).with_context(|| {
                    format!(
                        "corrupt checkpoint record {} in {}",
                        index + 1,
                        path.display()
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if keep < text.len() {
            File::options()
                .write(true)
                .open(path)
                .and_then(|file| file.set_len(keep as u64))
                .with_context(|| format!("Failed to truncate {}", path.display()))?;
        }

        let mut records = records.into_iter();
        match records.next() {
            Some(MemberRecord::Header(stored)) => ensure!(
                stored == header,
                "checkpoint {} was written for a different identity or selection \
                 (stored {:?}, current {:?}); rerun without --resume to discard it",
                path.display(),
                stored,
                header
            ),
            None => return Self::create(path, header),
            Some(_) => bail!("checkpoint {} does not start with a header", path.display()),
        }
        let mut members = Vec::new();
        let mut names = HashSet::new();
        let mut complete = false;
        for record in records {
            ensure!(
                !complete,
                "checkpoint {} has records after its completion marker",
                path.display()
            );
            match record {
                MemberRecord::Member { filename, stats } => {
                    names.insert(filename.clone());
                    members.push((filename, stats));
                }
                MemberRecord::Complete => complete = true,
                MemberRecord::Header(_) => {
                    bail!("checkpoint {} holds a second header", path.display())
                }
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            header,
            members,
            names,
            complete,
            file: open_append(path)?,
        })
    }

    pub fn header(&self) -> &MemberCheckpointHeader {
        &self.header
    }

    /// Whether `filename` already has a durable record.
    pub fn is_completed(&self, filename: &str) -> bool {
        self.names.contains(filename)
    }

    /// Copy of the completed names, for a thread that must skip them.
    pub fn completed_snapshot(&self) -> HashSet<String> {
        self.names.clone()
    }

    /// Every recorded member with its counters, in record order.
    pub fn members(&self) -> &[(String, S)] {
        &self.members
    }

    /// Whether every member of the source was recorded.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Record `filename` as durable. Call only after its rows are synced;
    /// [`Self::sync`] makes the record itself durable.
    pub fn append_member(&mut self, filename: &str, stats: S) -> Result<()> {
        ensure!(
            !self.complete,
            "checkpoint {} is already complete",
            self.path.display()
        );
        let record = MemberRecord::Member {
            filename: filename.to_string(),
            stats,
        };
        let line = record_line(&record)?;
        self.append_line(&line)?;
        let MemberRecord::Member { filename, stats } = record else {
            unreachable!("record was built as a member");
        };
        // Duplicate names inside one archive both get recorded: each was
        // processed, and both are skipped on resume.
        self.names.insert(filename.clone());
        self.members.push((filename, stats));
        Ok(())
    }

    /// Record that the whole source was processed, durably.
    pub fn mark_complete(&mut self) -> Result<()> {
        if self.complete {
            return Ok(());
        }
        let line = record_line(&MemberRecord::<S>::Complete)?;
        self.append_line(&line)?;
        self.sync()?;
        self.complete = true;
        Ok(())
    }

    /// Make every appended record durable.
    pub fn sync(&self) -> Result<()> {
        self.file
            .sync_all()
            .with_context(|| format!("Failed to sync checkpoint {}", self.path.display()))
    }

    /// Delete the checkpoint file (after the run's outputs are persisted).
    pub fn cleanup(self) -> Result<()> {
        drop(self.file);
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("Failed to remove {}", self.path.display())),
        }
    }

    fn append_line(&mut self, line: &str) -> Result<()> {
        self.file
            .write_all(line.as_bytes())
            .with_context(|| format!("Failed to append to checkpoint {}", self.path.display()))
    }
}

fn record_line<S: Serialize>(record: &MemberRecord<S>) -> Result<String> {
    let mut line =
        serde_json::to_string(record).context("Failed to serialize checkpoint record")?;
    line.push('\n');
    Ok(line)
}

fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open checkpoint {}", path.display()))
}

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
///
/// Also the per-partition record inside a full-mode
/// [`super::partition_commit::PartitionCommit`], where it describes one
/// freshly computed partition. The path counters default so checkpoints
/// written before they existed still load.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregationStatsSnapshot {
    pub partitions_processed: usize,
    pub valid_count: usize,
    pub failed_count: usize,
    pub total_citations: usize,
    pub publisher_citations: usize,
    pub crossref_citations: usize,
    pub mined_citations: usize,
    /// Partitions aggregated entirely in memory.
    #[serde(default)]
    pub in_memory_partitions: usize,
    /// Partitions aggregated through sorted runs and merges.
    #[serde(default)]
    pub external_partitions: usize,
    /// Partitions reused from verified commits instead of recomputed.
    #[serde(default)]
    pub reused_partitions: usize,
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
            in_memory_partitions: stats.in_memory_partitions,
            external_partitions: stats.external_partitions,
            reused_partitions: stats.reused_partitions,
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
            in_memory_partitions: snapshot.in_memory_partitions,
            external_partitions: snapshot.external_partitions,
            reused_partitions: snapshot.reused_partitions,
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

    fn member_header(identity: &str) -> MemberCheckpointHeader {
        MemberCheckpointHeader {
            kind: MEMBER_CHECKPOINT_KIND.to_string(),
            identity_sha256: identity.to_string(),
            outputs: vec!["failed".to_string(), "valid".to_string()],
            provenance: vec![],
        }
    }

    #[test]
    fn member_checkpoint_round_trips_members_and_completion() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("members.checkpoint");
        {
            let mut cp = MemberCheckpoint::<u64>::create(&path, member_header("id")).unwrap();
            cp.append_member("0.json", 5).unwrap();
            cp.append_member("1.json", 7).unwrap();
            cp.sync().unwrap();
            assert!(cp.is_completed("0.json"));
            assert!(!cp.is_completed("2.json"));
            assert!(!cp.is_complete());
        }
        {
            let mut cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
            assert_eq!(
                cp.members(),
                &[("0.json".to_string(), 5), ("1.json".to_string(), 7)]
            );
            assert!(!cp.is_complete());
            cp.mark_complete().unwrap();
        }
        let mut cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert!(cp.is_complete());
        assert_eq!(cp.members().len(), 2);
        assert!(cp.append_member("2.json", 1).is_err(), "complete is final");
        cp.cleanup().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn member_checkpoint_rejects_a_different_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("members.checkpoint");
        MemberCheckpoint::<u64>::create(&path, member_header("id"))
            .unwrap()
            .append_member("0.json", 1)
            .unwrap();
        let err = MemberCheckpoint::<u64>::open(&path, member_header("other"))
            .err()
            .expect("a different identity must be rejected");
        assert!(err.to_string().contains("different identity"), "{err}");
        let mut header = member_header("id");
        header.outputs = vec!["valid".to_string()];
        assert!(MemberCheckpoint::<u64>::open(&path, header).is_err());
        // Same header still opens.
        assert!(MemberCheckpoint::<u64>::open(&path, member_header("id")).is_ok());
    }

    #[test]
    fn member_checkpoint_drops_torn_trailing_records_and_truncates() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("members.checkpoint");
        {
            let mut cp = MemberCheckpoint::<u64>::create(&path, member_header("id")).unwrap();
            cp.append_member("0.json", 1).unwrap();
        }
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"record\":\"member\",\"filename\":\"1.js")
            .unwrap();
        drop(file);
        let mut cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert_eq!(cp.members().len(), 1, "the torn record is ignored");
        assert!(!cp.is_completed("1.json"));
        cp.append_member("1.json", 2).unwrap();
        drop(cp);
        let cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert_eq!(
            cp.members(),
            &[("0.json".to_string(), 1), ("1.json".to_string(), 2)],
            "the file was truncated before the append"
        );
        // A complete final line that does not parse is dropped the same way;
        // corruption before that is an error.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"garbage\n").unwrap();
        drop(file);
        let cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert_eq!(cp.members().len(), 2);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"garbage\n{\"record\":\"complete\"}\n")
            .unwrap();
        drop(file);
        let err = MemberCheckpoint::<u64>::open(&path, member_header("id"))
            .err()
            .expect("corruption before the final record must be rejected");
        assert!(
            err.to_string().contains("corrupt checkpoint record"),
            "{err}"
        );
    }

    #[test]
    fn member_checkpoint_starts_fresh_when_missing_or_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("members.checkpoint");
        let cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert!(cp.members().is_empty());
        drop(cp);
        fs::write(&path, b"").unwrap();
        let cp = MemberCheckpoint::<u64>::open(&path, member_header("id")).unwrap();
        assert!(cp.members().is_empty());
        assert_eq!(cp.header().identity_sha256, "id");
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
            ..AggregationStats::default()
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
    fn test_snapshot_without_path_counters_still_loads() {
        let text = r#"{"partitions_processed":3,"valid_count":1,"failed_count":0,
            "total_citations":4,"publisher_citations":1,"crossref_citations":1,
            "mined_citations":2}"#;
        let snapshot: AggregationStatsSnapshot = serde_json::from_str(text).unwrap();
        assert_eq!(snapshot.partitions_processed, 3);
        assert_eq!(snapshot.in_memory_partitions, 0);
        assert_eq!(snapshot.external_partitions, 0);
        assert_eq!(snapshot.reused_partitions, 0);
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
