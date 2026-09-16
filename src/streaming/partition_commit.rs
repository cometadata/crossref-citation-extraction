//! Crash-safe per-partition aggregation commits.
//!
//! A partition's output fragments are written into a staging directory
//! under the aggregation directory, synced, described by a `_COMMIT.json`
//! marker recording every fragment's size and SHA-256, and then promoted
//! into place by one directory rename. A committed directory is therefore
//! either complete and verifiable or absent. A retry reuses only commits
//! whose identity and digests verify and recomputes the rest, so a crash
//! anywhere between writing output and recording completion can never
//! duplicate a partition in the final outputs.
//!
//! Layout under a partition root `<partition_dir>`:
//!
//! ```text
//! <partition_dir>/_aggregation/<partition>/                 committed
//! <partition_dir>/_aggregation/<partition>/_COMMIT.json     marker
//! <partition_dir>/_aggregation/<partition>/<kind>.jsonl     fragments
//! <partition_dir>/_aggregation/<partition>.staging-<uuid>/  in progress
//! ```

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{bail, ensure, Context, Result};
use log::info;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::checkpoint::AggregationStatsSnapshot;
use crate::index::datacite::IndexFile;

/// Directory under a partition root that holds every aggregation commit.
pub const AGGREGATION_DIR: &str = "_aggregation";
/// The commit marker inside a committed partition directory.
pub const COMMIT_FILE: &str = "_COMMIT.json";
const COMMIT_TMP_FILE: &str = "_COMMIT.json.tmp";
const STAGING_INFIX: &str = ".staging-";

/// Failpoint checked just before a staging directory is renamed into place.
pub const FAILPOINT_BEFORE_RENAME: &str = "before_rename";
/// Failpoint checked after the rename, before the caller records progress.
pub const FAILPOINT_AFTER_RENAME_BEFORE_PROGRESS: &str = "after_rename_before_progress";
/// Failpoint checked while final outputs are assembled from fragments,
/// after a temporary output is complete and before it replaces the target.
pub const FAILPOINT_DURING_FINAL_ASSEMBLY: &str = "during_final_assembly";

static FAILPOINT: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// Arm (`Some(name)`) or clear (`None`) the failpoint that aborts the next
/// pass through the named commit boundary, simulating a crash there.
///
/// A test hook only: nothing in the CLI or the environment can set it, and
/// it stays armed until cleared, so tests that use it must serialize.
#[doc(hidden)]
pub fn set_failpoint(name: Option<&str>) {
    let slot = FAILPOINT.get_or_init(|| Mutex::new(None));
    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = name.map(str::to_owned);
}

/// Fail if the failpoint `name` is armed.
pub(crate) fn hit_failpoint(name: &str) -> Result<()> {
    let Some(slot) = FAILPOINT.get() else {
        return Ok(());
    };
    let armed = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if armed.as_deref() == Some(name) {
        bail!("failpoint {name} triggered");
    }
    Ok(())
}

/// The aggregation directory owned by the aggregators under a partition
/// root. It survives a successful run so a retry can reuse its commits;
/// the pipeline removes it with the partitions once the derived outputs
/// are persisted.
pub fn aggregation_dir(partition_dir: &Path) -> PathBuf {
    partition_dir.join(AGGREGATION_DIR)
}

/// `<target>.tmp`, the file a final output is assembled in before rename.
pub(crate) fn tmp_output_path(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// Make a directory's entries durable after a rename into it.
#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("Failed to sync directory {}", path.display()))
}

#[cfg(not(unix))]
pub(crate) fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Sync the directory holding `path`, when it names one.
pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => sync_dir(parent),
        _ => Ok(()),
    }
}

/// A single path component that names a regular file.
fn is_plain_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

/// Remove staging directories left behind by an interrupted run.
pub fn remove_stale_staging(aggregation_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(aggregation_dir)
        .with_context(|| format!("Failed to list {}", aggregation_dir.display()))?
    {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if path.is_dir() && name.contains(STAGING_INFIX) {
            info!(
                "Removing unfinished aggregation staging from an interrupted run: {}",
                path.display()
            );
            fs::remove_dir_all(&path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
        }
    }
    Ok(())
}

/// The marker recording one committed partition: the identity it was
/// aggregated under, every fragment's size and SHA-256, and its stats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCommit {
    pub partition: String,
    pub identity_sha256: String,
    pub files: BTreeMap<String, IndexFile>,
    pub stats: AggregationStatsSnapshot,
}

impl PartitionCommit {
    /// Load the commit in `dir` if one exists and verify it against this
    /// run's identity and every fragment's size and SHA-256.
    ///
    /// Returns `Ok(None)` when there is no marker. A marker that fails any
    /// check is an error, never `None`: a corrupt commit must be discarded
    /// deliberately (a run without resume), not recomputed over silently.
    pub fn load_verified(dir: &Path, identity_sha256: &str) -> Result<Option<Self>> {
        let marker = dir.join(COMMIT_FILE);
        let bytes = match fs::read(&marker) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", marker.display()))
            }
        };
        let commit: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("malformed {}", marker.display()))?;
        ensure!(
            commit.identity_sha256 == identity_sha256,
            "{}: commit belongs to identity {} but this run is {}",
            marker.display(),
            commit.identity_sha256,
            identity_sha256
        );
        let dir_name = dir.file_name().and_then(|n| n.to_str());
        ensure!(
            dir_name == Some(commit.partition.as_str()),
            "{}: commit names partition {:?} but lives in {:?}",
            marker.display(),
            commit.partition,
            dir_name
        );
        for (name, expected) in &commit.files {
            ensure!(
                is_plain_file_name(name) && name != COMMIT_FILE,
                "{}: invalid fragment name {name:?}",
                marker.display()
            );
            let path = dir.join(name);
            let actual = IndexFile::of_file(&path)?;
            ensure!(
                actual.size == expected.size,
                "{}: fragment {name} is {} bytes but the commit recorded {}",
                dir.display(),
                actual.size,
                expected.size
            );
            ensure!(
                actual.sha256 == expected.sha256,
                "{}: fragment {name} SHA-256 {} does not match the commit's {}",
                dir.display(),
                actual.sha256,
                expected.sha256
            );
        }
        Ok(Some(commit))
    }

    /// Write the marker into `dir` through a synced temporary file and a
    /// rename, so it is either complete or absent.
    pub fn save_atomic(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join(COMMIT_TMP_FILE);
        let marker = dir.join(COMMIT_FILE);
        let mut bytes = serde_json::to_vec_pretty(self).context("failed to serialize commit")?;
        bytes.push(b'\n');
        let mut file =
            File::create(&tmp).with_context(|| format!("failed to create {}", tmp.display()))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        drop(file);
        fs::rename(&tmp, &marker)
            .with_context(|| format!("failed to commit {}", marker.display()))?;
        sync_dir(dir)
    }
}

/// The staging directory of one partition under aggregation: fragment
/// writers are opened on demand, and [`PartitionStaging::commit`] syncs,
/// checksums, marks, and promotes the directory in one step.
///
/// An abandoned staging directory is left in place, exactly as a crash
/// would leave it; the next run removes it through
/// [`remove_stale_staging`].
pub struct PartitionStaging {
    partition: String,
    aggregation_dir: PathBuf,
    dir: PathBuf,
    writers: BTreeMap<String, BufWriter<File>>,
}

impl PartitionStaging {
    /// Create a fresh staging directory for `partition` under
    /// `aggregation_dir`.
    pub fn create(aggregation_dir: &Path, partition: &str) -> Result<Self> {
        ensure!(
            is_plain_file_name(partition) && !partition.contains(STAGING_INFIX),
            "invalid partition name {partition:?}"
        );
        let dir = aggregation_dir.join(format!(
            "{partition}{STAGING_INFIX}{}",
            Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create staging directory {}", dir.display()))?;
        Ok(Self {
            partition: partition.to_owned(),
            aggregation_dir: aggregation_dir.to_path_buf(),
            dir,
            writers: BTreeMap::new(),
        })
    }

    /// The partition being staged.
    pub fn partition(&self) -> &str {
        &self.partition
    }

    /// The staging directory; scratch subdirectories may be created here
    /// but must be removed before [`PartitionStaging::commit`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The writer for the fragment file `fragment`, created on first use so
    /// a fragment exists only when something was written to it.
    pub fn writer(&mut self, fragment: &str) -> Result<&mut BufWriter<File>> {
        if !self.writers.contains_key(fragment) {
            ensure!(
                is_plain_file_name(fragment)
                    && fragment != COMMIT_FILE
                    && fragment != COMMIT_TMP_FILE,
                "invalid fragment name {fragment:?}"
            );
            let path = self.dir.join(fragment);
            let file = File::create(&path)
                .with_context(|| format!("Failed to create fragment {}", path.display()))?;
            self.writers
                .insert(fragment.to_owned(), BufWriter::new(file));
        }
        Ok(self
            .writers
            .get_mut(fragment)
            .expect("writer inserted above"))
    }

    /// Flush and sync every fragment, record their digests and `stats` in
    /// the marker, and promote the directory to its committed name.
    ///
    /// Fails if the partition is already committed; the staging directory
    /// is left for the next run to remove.
    pub fn commit(
        self,
        identity_sha256: &str,
        stats: AggregationStatsSnapshot,
    ) -> Result<(PathBuf, PartitionCommit)> {
        let Self {
            partition,
            aggregation_dir,
            dir: staging_dir,
            writers,
        } = self;
        let mut files = BTreeMap::new();
        for (name, mut writer) in writers {
            let path = staging_dir.join(&name);
            writer
                .flush()
                .with_context(|| format!("Failed to flush fragment {}", path.display()))?;
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()
                .with_context(|| format!("Failed to sync fragment {}", path.display()))?;
            drop(file);
            files.insert(name, IndexFile::of_file(&path)?);
        }
        let commit = PartitionCommit {
            partition: partition.clone(),
            identity_sha256: identity_sha256.to_owned(),
            files,
            stats,
        };
        commit.save_atomic(&staging_dir)?;

        let committed_dir = aggregation_dir.join(&partition);
        hit_failpoint(FAILPOINT_BEFORE_RENAME)?;
        // Promotion boundary: the marker and fragments are already synced here.
        anyhow::ensure!(!committed_dir.exists(), "partition already committed");
        std::fs::rename(&staging_dir, &committed_dir)?;
        sync_dir(&aggregation_dir)?;
        hit_failpoint(FAILPOINT_AFTER_RENAME_BEFORE_PROGRESS)?;
        Ok((committed_dir, commit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn commit_round_trips_and_verifies_fragments() {
        let dir = tempdir().unwrap();
        let mut staging = PartitionStaging::create(dir.path(), "h0001").unwrap();
        assert!(staging.dir().starts_with(dir.path()));
        staging
            .writer("valid.jsonl")
            .unwrap()
            .write_all(b"{\"doi\":\"10.1/a\"}\n")
            .unwrap();
        let stats = AggregationStatsSnapshot {
            partitions_processed: 1,
            valid_count: 1,
            ..Default::default()
        };
        let (committed, commit) = staging.commit("id", stats).unwrap();
        assert_eq!(committed, dir.path().join("h0001"));
        assert!(committed.join(COMMIT_FILE).exists());
        assert!(!committed.join(COMMIT_TMP_FILE).exists());
        assert_eq!(commit.files.len(), 1);
        assert_eq!(commit.files["valid.jsonl"].size, 17);

        let loaded = PartitionCommit::load_verified(&committed, "id")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, commit);
        assert!(PartitionCommit::load_verified(&committed, "other").is_err());
        assert!(
            PartitionCommit::load_verified(&dir.path().join("h0002"), "id")
                .unwrap()
                .is_none()
        );

        // A second commit of the same partition hits the promotion guard.
        let staging = PartitionStaging::create(dir.path(), "h0001").unwrap();
        let err = staging
            .commit("id", AggregationStatsSnapshot::default())
            .unwrap_err();
        assert!(err.to_string().contains("already committed"));
    }

    #[test]
    fn stale_staging_is_removed_and_commits_are_kept() {
        let dir = tempdir().unwrap();
        let stale = dir.path().join("h0001.staging-deadbeef");
        fs::create_dir_all(stale.join("runs")).unwrap();
        fs::write(stale.join("valid.jsonl"), b"x").unwrap();
        fs::create_dir_all(dir.path().join("h0002")).unwrap();
        fs::write(dir.path().join("note.txt"), b"keep").unwrap();

        remove_stale_staging(dir.path()).unwrap();
        assert!(!stale.exists());
        assert!(dir.path().join("h0002").exists());
        assert!(dir.path().join("note.txt").exists());
    }

    #[test]
    fn failpoints_abort_only_when_armed() {
        assert!(hit_failpoint(FAILPOINT_BEFORE_RENAME).is_ok());
        set_failpoint(Some(FAILPOINT_DURING_FINAL_ASSEMBLY));
        assert!(hit_failpoint(FAILPOINT_BEFORE_RENAME).is_ok());
        let err = hit_failpoint(FAILPOINT_DURING_FINAL_ASSEMBLY).unwrap_err();
        assert!(err.to_string().contains(FAILPOINT_DURING_FINAL_ASSEMBLY));
        set_failpoint(None);
        assert!(hit_failpoint(FAILPOINT_DURING_FINAL_ASSEMBLY).is_ok());
        assert_eq!(
            tmp_output_path(Path::new("/out/valid.jsonl")),
            PathBuf::from("/out/valid.jsonl.tmp")
        );
    }
}
