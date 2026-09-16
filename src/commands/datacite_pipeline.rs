//! Full DataCite mode of the `pipeline` command (`--datacite-index`).
//!
//! Every Crossref member is streamed through the general reference matcher
//! against the loaded index bundle. Confirmed matches become hashed
//! partition rows, shared landing-page URLs that no evidence could settle
//! become ambiguity rows, and both travel from parse workers to the single
//! writer in bounded batches. A member is checkpointed only after its rows
//! are synced; the run's counters are the sum of those checkpoint records,
//! so a replayed member is never counted twice. Once every member is
//! durable the partitions and ambiguity segments are aggregated and
//! `extraction-report.json` is written.
//!
//! Durability is committed in batches: every buffered partition row and the
//! open ambiguity segment are synced, then the members completed since the
//! last commit are recorded. A commit is triggered when the writer holds at
//! least half of `--writer-memory-mb` (so each commit writes segments worth
//! about half the budget spread over the partitions) or, secondarily, when
//! `--checkpoint-interval` members are pending (2000 unless given). After a
//! crash, `--resume` replays at most one commit's worth of members; the
//! aggregators deduplicate replayed rows by their stable keys.
//!
//! Failure rules: an unreadable or oversized member, invalid top-level
//! JSON, a lookup over the candidate budget, or a row over the batch or
//! writer budget aborts the run with no report; malformed references and
//! unusable field values only count. A worker error reaches the writer as
//! an event, and a writer error drops the receiver so blocked senders fail
//! instead of waiting on a queue nobody drains.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, ensure, Context, Result};
use crossbeam_channel::{Receiver, SendError, Sender};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cli::{should_include_provenance_filter, OutputType, PipelineArgs};
use crate::common::identifiers::normalize_doi;
use crate::common::{extract_input_stem, MatchType, OutputPaths};
use crate::extract::{match_reference, Provenance};
use crate::index::datacite::{DataciteIndex, FORMAT_VERSION, NORMALIZATION_VERSION};
use crate::input::{
    detect_crossref_input, visit_crossref_entries_limited, CrossrefEntry, CrossrefInput,
};
use crate::streaming::checkpoint::{
    AggregationStatsSnapshot, MemberCheckpoint, MemberCheckpointHeader, MEMBER_CHECKPOINT_KIND,
};
use crate::streaming::partition_commit::{aggregation_dir, sync_parent, AGGREGATION_DIR};
use crate::streaming::partitioning::HASH_ALGORITHM;
use crate::streaming::{
    aggregate_ambiguities, aggregate_datacite, AggregationLimits, PartitionMode, PartitionRow,
    SegmentedPartitionWriter, WriterLimits,
};

/// The report written to `--output-dir` after a complete run.
pub const REPORT_FILE: &str = "extraction-report.json";
/// `kind` of [`ExtractionReport`].
pub const REPORT_KIND: &str = "datacite-extraction-report";
/// `schema_version` of [`ExtractionReport`].
pub const REPORT_SCHEMA_VERSION: u32 = 2;
/// Version of this extraction logic, part of the derived identity.
pub const EXTRACTION_VERSION: u32 = 2;
/// Checkpoint file under `--temp-dir`; distinct from the legacy
/// `extraction.checkpoint` and never mistakable for the report.
pub const CHECKPOINT_FILE: &str = "datacite-extraction.checkpoint";
/// Hashed partition root under `--temp-dir`.
pub const PARTITIONS_DIR: &str = "partitions";
/// Ambiguity segment directory under `--temp-dir`.
pub const AMBIGUITIES_DIR: &str = "ambiguities";
/// Suffix of the ambiguity output file, after the input stem.
pub const AMBIGUITY_OUTPUT_SUFFIX: &str = "_ambiguous_urls.jsonl";

const MIB: usize = 1024 * 1024;
/// Raw members buffered between the reader and the parse workers.
const ENTRY_QUEUE_DEPTH: usize = 4;
/// Per-partition row cap; the byte budget flushes long before this.
const SEGMENT_ROWS: usize = 1_000_000;
/// Pending members that force a commit when `--checkpoint-interval` is not
/// given; the buffered-byte trigger normally fires first.
const DEFAULT_CHECKPOINT_MEMBERS: usize = 2000;
/// Members between progress events.
const PROGRESS_MEMBER_INTERVAL: u64 = 100;
/// Fixed charge per ambiguity row on top of its string content.
const AMBIGUITY_ROW_OVERHEAD: usize = 128;
const SEGMENT_PREFIX: &str = "segment_";
const SEGMENT_SUFFIX: &str = ".jsonl";
const PART_SUFFIX: &str = ".part";

/// Counters for one source member, and (summed) for the run.
///
/// `confirmed_matches` counts every confirmed cited DOI before the
/// self-citation and provenance filters; `doi_matches + url_matches +
/// arxiv_matches` equals it. `filtered_self_citations` and
/// `filtered_provenance` count confirmed or failed-arXiv rows dropped by
/// those filters.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStats {
    pub items: u64,
    pub references: u64,
    pub confirmed_matches: u64,
    pub doi_matches: u64,
    pub url_matches: u64,
    pub arxiv_matches: u64,
    pub ambiguous_urls: u64,
    pub invalid_arxiv: u64,
    pub unmatched_dois: u64,
    pub unmatched_urls: u64,
    pub malformed_references: u64,
    pub malformed_fields: u64,
    pub filtered_self_citations: u64,
    pub filtered_provenance: u64,
}

impl FileStats {
    /// Add every counter of `other` into `self`.
    pub fn add(&mut self, other: &FileStats) {
        self.items += other.items;
        self.references += other.references;
        self.confirmed_matches += other.confirmed_matches;
        self.doi_matches += other.doi_matches;
        self.url_matches += other.url_matches;
        self.arxiv_matches += other.arxiv_matches;
        self.ambiguous_urls += other.ambiguous_urls;
        self.invalid_arxiv += other.invalid_arxiv;
        self.unmatched_dois += other.unmatched_dois;
        self.unmatched_urls += other.unmatched_urls;
        self.malformed_references += other.malformed_references;
        self.malformed_fields += other.malformed_fields;
        self.filtered_self_citations += other.filtered_self_citations;
        self.filtered_provenance += other.filtered_provenance;
    }
}

/// `extraction-report.json`: written only after extraction and
/// aggregation both finished, so `completed` is always `true` and
/// `fatal_source_errors` always `0` in a report that exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractionReport {
    pub kind: String,
    pub schema_version: u32,
    pub identity_sha256: String,
    pub completed: bool,
    pub files_processed: u64,
    pub fatal_source_errors: u64,
    pub stats: FileStats,
    pub aggregation: AggregationStatsSnapshot,
}

/// What parse workers send to the writer.
enum WorkerEvent {
    /// Confirmed and failed-arXiv rows, at most one batch budget of them.
    Rows(Vec<PartitionRow>),
    /// Ambiguity rows, under the same budget.
    Ambiguous(Vec<Value>),
    /// Sent after a member's last batch; the writer checkpoints it once
    /// everything before it is durable.
    FileComplete { filename: String, stats: FileStats },
    /// A worker's fatal error, delivered in order so the writer stops.
    Failed(anyhow::Error),
}

/// Validated, unit-converted full-mode settings.
struct FullModeConfig {
    partitions: u32,
    writer_memory_bytes: usize,
    result_batch_bytes: usize,
    queue_batches: usize,
    aggregation_memory_bytes: usize,
    merge_fan_in: usize,
    sort_workers: usize,
    lookup_bytes: usize,
    max_entry_bytes: usize,
    /// Buffered partition bytes at which pending members are committed.
    commit_bytes: usize,
    /// Pending members at which they are committed regardless of bytes.
    checkpoint_members: usize,
    outputs: HashSet<OutputType>,
    /// Lowercase, sorted, distinct; empty means every provenance.
    provenance_filter: Vec<String>,
}

/// Paths under `--temp-dir` that the pipeline owns.
struct Workspace {
    temp_dir: PathBuf,
    partitions: PathBuf,
    ambiguities: PathBuf,
    checkpoint: PathBuf,
}

impl Workspace {
    fn new(temp_dir: PathBuf) -> Self {
        Self {
            partitions: temp_dir.join(PARTITIONS_DIR),
            ambiguities: temp_dir.join(AMBIGUITIES_DIR),
            checkpoint: temp_dir.join(CHECKPOINT_FILE),
            temp_dir,
        }
    }
}

/// Run the full DataCite pipeline for `args` (which must carry
/// `--datacite-index`).
pub fn run_datacite_pipeline(args: &PipelineArgs) -> Result<()> {
    let started = Instant::now();
    info!(
        "event=phase phase=pipeline status=started mode=datacite input={}",
        args.input
    );
    let cfg = validate_full_mode_args(args)?;
    let bundle_dir = args
        .datacite_index
        .as_deref()
        .ok_or_else(|| anyhow!("--datacite-index is required for DataCite mode"))?;
    let input = detect_crossref_input(&args.input)?;
    let input_size = input_size_bytes(&input)?;

    info!("event=phase phase=index_load status=started bundle={bundle_dir}");
    let index = DataciteIndex::load(Path::new(bundle_dir))
        .with_context(|| format!("loading DataCite index bundle {bundle_dir}"))?;
    info!(
        "event=phase phase=index_load status=completed release={} bundle_manifest_sha256={} \
         elapsed_s={:.1}",
        index.manifest().release,
        index.manifest_sha256(),
        started.elapsed().as_secs_f64()
    );
    let (identity, identity_sha256) = resolve_identity(args, &cfg, &index, input_size)?;
    info!(
        "event=identity identity_sha256={identity_sha256} source={} identity={}",
        if args.run_identity.is_some() {
            "file"
        } else {
            "derived"
        },
        canonical_json(&identity)
    );

    // Nothing above touched the filesystem; everything below may.
    let output_dir = args
        .output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;
    let temp_dir = args
        .temp_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("datacite-extraction-{}", Uuid::new_v4()))
        });
    fs::create_dir_all(&temp_dir)
        .with_context(|| format!("creating temp directory {}", temp_dir.display()))?;
    let ws = Workspace::new(temp_dir);
    let report_path = output_dir.join(REPORT_FILE);
    remove_file_if_exists(&report_path)?;

    let header = MemberCheckpointHeader {
        kind: MEMBER_CHECKPOINT_KIND.to_string(),
        identity_sha256: identity_sha256.clone(),
        outputs: output_names(&cfg.outputs),
        provenance: cfg.provenance_filter.clone(),
    };
    let mut checkpoint = if args.resume && ws.checkpoint.exists() {
        let checkpoint = MemberCheckpoint::<FileStats>::open(&ws.checkpoint, header)?;
        // The records promise rows on disk; without the directories that
        // hold them a resume would silently publish a partial run.
        for dir in [&ws.partitions, &ws.ambiguities] {
            ensure!(
                checkpoint.members().is_empty() || dir.is_dir(),
                "checkpoint {} lists {} completed members but {} is missing; rerun without \
                 --resume",
                ws.checkpoint.display(),
                checkpoint.members().len(),
                dir.display()
            );
        }
        info!(
            "event=resume members_completed={} extraction_complete={}",
            checkpoint.members().len(),
            checkpoint.is_complete()
        );
        checkpoint
    } else {
        if args.resume {
            info!("event=resume members_completed=0 reason=no_checkpoint");
        }
        remove_pipeline_entries(&ws)?;
        MemberCheckpoint::create(&ws.checkpoint, header)?
    };

    if checkpoint.is_complete() {
        info!(
            "event=phase phase=extraction status=skipped reason=checkpoint_complete members={}",
            checkpoint.members().len()
        );
    } else {
        // Commits from an earlier aggregation attempt cannot cover the
        // members processed now.
        for dir in [
            aggregation_dir(&ws.partitions),
            aggregation_dir(&ws.ambiguities),
        ] {
            remove_dir_if_exists(&dir)?;
        }
        run_extraction(&cfg, &ws, input, &index, &mut checkpoint)?;
    }
    let (files_processed, stats) = checkpoint_totals(&checkpoint);
    info!(
        "event=phase phase=extraction status=completed files_processed={files_processed} {}",
        stats_fields(&stats)
    );

    let stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&cfg.outputs, &output_dir, &stem);
    let ambiguity_output = output_dir.join(format!("{stem}{AMBIGUITY_OUTPUT_SUFFIX}"));
    let limits = AggregationLimits {
        memory_bytes: cfg.aggregation_memory_bytes,
        merge_fan_in: cfg.merge_fan_in,
        sort_workers: cfg.sort_workers,
    };
    info!(
        "event=phase phase=aggregation status=started partitions={} memory_bytes={} \
         merge_fan_in={} sort_workers={}",
        cfg.partitions, limits.memory_bytes, limits.merge_fan_in, limits.sort_workers
    );
    let aggregation = aggregate_datacite(
        &ws.partitions,
        &index,
        &outputs,
        limits,
        &identity_sha256,
        args.resume,
    )?
    .snapshot();
    let ambiguity_rows = aggregate_ambiguities(
        &ws.ambiguities,
        &ambiguity_output,
        &limits,
        &identity_sha256,
        args.resume,
    )?;
    info!(
        "event=phase phase=aggregation status=completed valid={} failed={} citations={} \
         publisher={} crossref={} mined={} ambiguity_rows={ambiguity_rows} reused_partitions={}",
        aggregation.valid_count,
        aggregation.failed_count,
        aggregation.total_citations,
        aggregation.publisher_citations,
        aggregation.crossref_citations,
        aggregation.mined_citations,
        aggregation.reused_partitions
    );

    let report = ExtractionReport {
        kind: REPORT_KIND.to_string(),
        schema_version: REPORT_SCHEMA_VERSION,
        identity_sha256: identity_sha256.clone(),
        completed: true,
        files_processed,
        fatal_source_errors: 0,
        stats,
        aggregation,
    };
    write_report(&report_path, &report)?;
    info!(
        "event=phase phase=report status=written path={}",
        report_path.display()
    );

    if args.keep_intermediates {
        info!(
            "event=cleanup status=kept temp_dir={}",
            ws.temp_dir.display()
        );
    } else {
        checkpoint.cleanup()?;
        remove_pipeline_entries(&ws)?;
        remove_dir_if_empty(&ws.temp_dir);
        info!(
            "event=cleanup status=removed temp_dir={}",
            ws.temp_dir.display()
        );
    }
    info!(
        "event=phase phase=pipeline status=completed identity_sha256={identity_sha256} \
         elapsed_s={:.1}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

// ------------------------------------------------------------ validation

fn validate_full_mode_args(args: &PipelineArgs) -> Result<FullModeConfig> {
    ensure!(
        args.arxiv_fst.is_none() && args.arxiv_records.is_none(),
        "--datacite-index selects DataCite mode and cannot be combined with --arxiv-fst or \
         --arxiv-records"
    );
    ensure!(
        args.partitions_dir.is_none(),
        "--partitions-dir is not supported in DataCite mode; use --resume to reuse extraction"
    );
    ensure!(
        args.partitions.is_power_of_two(),
        "--partitions must be a positive power of two (got {})",
        args.partitions
    );
    ensure!(args.queue_batches >= 1, "--queue-batches must be positive");
    ensure!(args.merge_fan_in >= 2, "--merge-fan-in must be at least 2");
    ensure!(args.sort_workers >= 1, "--sort-workers must be positive");
    let writer_memory_bytes = mib_to_bytes("--writer-memory-mb", args.writer_memory_mb)?;
    let result_batch_bytes = mib_to_bytes("--result-batch-mb", args.result_batch_mb)?;
    let aggregation_memory_bytes =
        mib_to_bytes("--aggregation-memory-mb", args.aggregation_memory_mb)?;
    ensure!(
        aggregation_memory_bytes >= writer_memory_bytes,
        "--aggregation-memory-mb ({}) must be at least --writer-memory-mb ({}) so every segment \
         can be sorted",
        args.aggregation_memory_mb,
        args.writer_memory_mb
    );
    let lookup_bytes = mib_to_bytes("--lookup-memory-mb", args.lookup_memory_mb)?;
    let max_entry_bytes = mib_to_bytes("--max-entry-mb", args.max_entry_mb)?;
    let outputs = args.parse_outputs().map_err(|e| anyhow!(e))?;
    let mut provenance_filter = Vec::with_capacity(args.provenance.len());
    for value in &args.provenance {
        let lower = value.to_ascii_lowercase();
        ensure!(
            matches!(lower.as_str(), "publisher" | "crossref" | "mined"),
            "unknown provenance {value:?}; expected publisher, crossref, or mined"
        );
        provenance_filter.push(lower);
    }
    provenance_filter.sort();
    provenance_filter.dedup();
    let checkpoint_members = args
        .checkpoint_interval
        .unwrap_or(DEFAULT_CHECKPOINT_MEMBERS);
    ensure!(
        checkpoint_members > 0,
        "--checkpoint-interval must be positive in DataCite mode"
    );
    Ok(FullModeConfig {
        partitions: args.partitions,
        writer_memory_bytes,
        result_batch_bytes,
        queue_batches: args.queue_batches,
        aggregation_memory_bytes,
        merge_fan_in: args.merge_fan_in,
        sort_workers: args.sort_workers,
        lookup_bytes,
        max_entry_bytes,
        commit_bytes: (writer_memory_bytes / 2).max(1),
        checkpoint_members,
        outputs,
        provenance_filter,
    })
}

fn mib_to_bytes(flag: &str, value: usize) -> Result<usize> {
    ensure!(value > 0, "{flag} must be positive");
    value
        .checked_mul(MIB)
        .ok_or_else(|| anyhow!("{flag} of {value} MiB overflows the addressable size"))
}

fn output_name(kind: OutputType) -> &'static str {
    match kind {
        OutputType::Valid => "valid",
        OutputType::Failed => "failed",
        OutputType::Publisher => "publisher",
        OutputType::Crossref => "crossref",
        OutputType::Mined => "mined",
    }
}

fn output_names(outputs: &HashSet<OutputType>) -> Vec<String> {
    outputs
        .iter()
        .map(|kind| output_name(*kind))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(String::from)
        .collect()
}

fn input_size_bytes(input: &CrossrefInput) -> Result<u64> {
    let size_of = |path: &Path| -> Result<u64> {
        fs::metadata(path)
            .map(|m| m.len())
            .with_context(|| format!("reading the size of {}", path.display()))
    };
    match input {
        CrossrefInput::TarGz(path) | CrossrefInput::SingleJson(path) => size_of(path),
        CrossrefInput::Directory(dir) => {
            let mut total = 0u64;
            for entry in fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
                let path = entry?.path();
                if path.extension().is_some_and(|ext| ext == "json") {
                    total += size_of(&path)?;
                }
            }
            Ok(total)
        }
    }
}

// -------------------------------------------------------------- identity

/// Compact JSON with every object's keys sorted: the canonical form whose
/// SHA-256 is the run identity. Python produces the same bytes with
/// `json.dumps(obj, sort_keys=True, separators=(",", ":"))`.
pub fn canonical_json(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered: BTreeMap<&String, &Value> = map.iter().collect();
                Value::Object(
                    ordered
                        .into_iter()
                        .map(|(key, value)| (key.clone(), sorted(value)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&sorted(value)).expect("a JSON value serializes")
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(out, "{byte:02x}").expect("writing to a String cannot fail");
            out
        })
}

/// The identity object and its digest: the supplied `--run-identity` file
/// after validation against this run, or the derived local identity.
fn resolve_identity(
    args: &PipelineArgs,
    cfg: &FullModeConfig,
    index: &DataciteIndex,
    input_size: u64,
) -> Result<(Value, String)> {
    let identity = match &args.run_identity {
        Some(path) => {
            let bytes = fs::read(path).with_context(|| format!("reading --run-identity {path}"))?;
            let value: Value = serde_json::from_slice(&bytes)
                .with_context(|| format!("--run-identity {path} is not valid JSON"))?;
            ensure!(
                value.is_object(),
                "--run-identity {path} must hold a JSON object"
            );
            let expect_str = |field: &str, expected: &str| -> Result<()> {
                let actual = value.get(field).and_then(Value::as_str);
                ensure!(
                    actual == Some(expected),
                    "--run-identity {path}: {field} is {actual:?}, expected {expected:?}"
                );
                Ok(())
            };
            expect_str("extraction_mode", "datacite")?;
            expect_str("partition_algorithm", HASH_ALGORITHM)?;
            expect_str("bundle_manifest_sha256", index.manifest_sha256())?;
            let partitions = value.get("partitions").and_then(Value::as_u64);
            ensure!(
                partitions == Some(u64::from(cfg.partitions)),
                "--run-identity {path}: partitions is {partitions:?}, expected {}",
                cfg.partitions
            );
            value
        }
        None => json!({
            "bundle_manifest_sha256": index.manifest_sha256(),
            "extraction_mode": "datacite",
            "extraction_version": EXTRACTION_VERSION,
            "index_version": FORMAT_VERSION,
            "normalization_version": NORMALIZATION_VERSION,
            "partition_algorithm": HASH_ALGORITHM,
            "partitions": cfg.partitions,
            "input_size_bytes": input_size,
        }),
    };
    let digest = sha256_hex(canonical_json(&identity).as_bytes());
    Ok((identity, digest))
}

// ------------------------------------------------------------- workspace

fn is_hashed_partition_name(name: &str) -> bool {
    name.len() > 1
        && name.starts_with('h')
        && name[1..]
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

/// Remove every entry the pipeline creates under `--temp-dir`, and nothing
/// else: hashed partition directories, aggregation directories, ambiguity
/// segments, `.part` files, and the checkpoint. Directories left empty are
/// removed; `--temp-dir` itself is not.
fn remove_pipeline_entries(ws: &Workspace) -> Result<()> {
    remove_owned_children(&ws.partitions, |name, is_dir| {
        if is_dir {
            name == AGGREGATION_DIR || is_hashed_partition_name(name)
        } else {
            name.ends_with(PART_SUFFIX)
        }
    })?;
    remove_owned_children(&ws.ambiguities, |name, is_dir| {
        if is_dir {
            name == AGGREGATION_DIR
        } else {
            name.starts_with(SEGMENT_PREFIX)
                && (name.ends_with(SEGMENT_SUFFIX)
                    || name.ends_with(&format!("{SEGMENT_SUFFIX}{PART_SUFFIX}")))
        }
    })?;
    remove_file_if_exists(&ws.checkpoint)?;
    remove_dir_if_empty(&ws.partitions);
    remove_dir_if_empty(&ws.ambiguities);
    Ok(())
}

fn remove_owned_children(dir: &Path, owned: impl Fn(&str, bool) -> bool) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        let path = entry.path();
        let is_dir = entry.file_type()?.is_dir();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !owned(name, is_dir) {
            continue;
        }
        if is_dir {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        }
        .with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => {
            info!("Removed stale {}", path.display());
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

/// Remove `path` only when it is an empty directory; anything else stays.
fn remove_dir_if_empty(path: &Path) {
    if let Err(e) = fs::remove_dir(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            debug!("Leaving {} in place: {e}", path.display());
        }
    }
}

// ----------------------------------------------------- ambiguity segments

/// Writer of `ambiguities/segment_<n>.jsonl`: rows go to a `.part` file
/// that is synced and renamed at commit, so a segment is complete or
/// absent. Sequence numbers continue past any committed segment.
struct AmbiguitySegments {
    dir: PathBuf,
    next_index: u32,
    open: Option<OpenSegment>,
}

struct OpenSegment {
    part_path: PathBuf,
    final_path: PathBuf,
    writer: BufWriter<File>,
    rows: u64,
}

impl AmbiguitySegments {
    fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)
            .with_context(|| format!("creating ambiguity directory {}", dir.display()))?;
        let mut next_index = 0u32;
        for entry in fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(PART_SUFFIX) {
                fs::remove_file(&path)
                    .with_context(|| format!("removing unfinished {}", path.display()))?;
            } else if let Some(index) = name
                .strip_prefix(SEGMENT_PREFIX)
                .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
                .and_then(|digits| digits.parse::<u32>().ok())
            {
                next_index = next_index.max(index + 1);
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            next_index,
            open: None,
        })
    }

    fn append(&mut self, rows: &[Value]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        if self.open.is_none() {
            let name = format!("{SEGMENT_PREFIX}{:04}{SEGMENT_SUFFIX}", self.next_index);
            let final_path = self.dir.join(&name);
            let part_path = self.dir.join(format!("{name}{PART_SUFFIX}"));
            let file = File::create(&part_path)
                .with_context(|| format!("creating {}", part_path.display()))?;
            self.open = Some(OpenSegment {
                part_path,
                final_path,
                writer: BufWriter::new(file),
                rows: 0,
            });
        }
        let segment = self.open.as_mut().expect("segment opened above");
        for row in rows {
            serde_json::to_writer(&mut segment.writer, row)
                .with_context(|| format!("writing {}", segment.part_path.display()))?;
            segment
                .writer
                .write_all(b"\n")
                .with_context(|| format!("writing {}", segment.part_path.display()))?;
            segment.rows += 1;
        }
        Ok(())
    }

    /// Sync and rename the open segment, if any; returns its row count.
    fn commit(&mut self) -> Result<u64> {
        let Some(segment) = self.open.take() else {
            return Ok(0);
        };
        let file = segment
            .writer
            .into_inner()
            .map_err(|e| e.into_error())
            .with_context(|| format!("flushing {}", segment.part_path.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", segment.part_path.display()))?;
        fs::rename(&segment.part_path, &segment.final_path).with_context(|| {
            format!(
                "committing {} as {}",
                segment.part_path.display(),
                segment.final_path.display()
            )
        })?;
        sync_parent(&segment.final_path)?;
        self.next_index += 1;
        debug!(
            "Committed {} ambiguity rows to {}",
            segment.rows,
            segment.final_path.display()
        );
        Ok(segment.rows)
    }
}

// ------------------------------------------------------------ extraction

/// Everything a parse worker needs, shared read-only.
struct WorkerContext<'a> {
    index: &'a DataciteIndex,
    provenance_filter: &'a [String],
    lookup_bytes: usize,
    batch_limit: usize,
}

/// A worker-local batch under the result budget.
struct Batch<T> {
    batch: Vec<T>,
    batch_bytes: usize,
    batch_limit: usize,
    wrap: fn(Vec<T>) -> WorkerEvent,
}

impl<T> Batch<T> {
    fn new(batch_limit: usize, wrap: fn(Vec<T>) -> WorkerEvent) -> Self {
        Self {
            batch: Vec::new(),
            batch_bytes: 0,
            batch_limit,
            wrap,
        }
    }

    /// Send the batch first when `row` would push it over the limit; a row
    /// that cannot fit an empty batch is an explicit error.
    fn push(&mut self, tx: &Sender<WorkerEvent>, row: T, row_bytes: usize) -> Result<()> {
        let Self {
            batch,
            batch_bytes,
            batch_limit,
            wrap,
        } = self;
        if batch_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| anyhow::anyhow!("batch size overflow"))?
            > *batch_limit
            && !batch.is_empty()
        {
            tx.send(wrap(std::mem::take(batch)))?;
            *batch_bytes = 0;
        }
        anyhow::ensure!(
            row_bytes <= *batch_limit,
            "reference exceeds result batch budget"
        );
        batch.push(row);
        *batch_bytes += row_bytes;
        Ok(())
    }

    fn flush(&mut self, tx: &Sender<WorkerEvent>) -> Result<()> {
        if !self.batch.is_empty() {
            tx.send((self.wrap)(std::mem::take(&mut self.batch)))?;
            self.batch_bytes = 0;
        }
        Ok(())
    }
}

/// The parts of a Crossref file the extractor reads; every item and every
/// reference stays an unparsed slice until it is looked at.
#[derive(Deserialize)]
struct CrossrefFile<'a> {
    #[serde(default, borrow)]
    items: Vec<&'a RawValue>,
}

#[derive(Deserialize)]
struct CrossrefItem<'a> {
    #[serde(rename = "DOI", default, borrow)]
    doi: Option<&'a RawValue>,
    #[serde(rename = "reference", default, borrow)]
    reference: Option<&'a RawValue>,
}

/// One reference being mined: the citing work, the position, the parsed
/// object, and its compact serialization once any evidence is retained.
struct ReferenceScope<'a> {
    citing: &'a str,
    ref_index: u32,
    reference: &'a Value,
    ref_json: Option<String>,
}

impl ReferenceScope<'_> {
    fn ref_json(&mut self) -> &str {
        self.ref_json
            .get_or_insert_with(|| self.reference.to_string())
    }

    fn row(
        &mut self,
        cited_id: String,
        provenance: Provenance,
        match_type: MatchType,
        raw_match: String,
    ) -> PartitionRow {
        PartitionRow {
            citing_doi: self.citing.to_string(),
            ref_index: self.ref_index,
            cited_id,
            provenance: provenance.as_str().to_string(),
            match_type,
            raw_match,
            ref_json: self.ref_json().to_string(),
        }
    }
}

/// Provenance of a failed arXiv candidate: the asserted value only when
/// the structured `DOI` field is that same canonical DOI, as for confirmed
/// matches (an asserted version suffix is not stripped).
fn failed_arxiv_provenance(reference: &Value, canonical: &str) -> Provenance {
    let asserted_same = reference
        .get("DOI")
        .and_then(Value::as_str)
        .and_then(normalize_doi)
        .is_some_and(|doi| doi == canonical);
    if !asserted_same {
        return Provenance::Mined;
    }
    match reference.get("doi-asserted-by").and_then(Value::as_str) {
        Some(value) if value.eq_ignore_ascii_case("publisher") => Provenance::Publisher,
        Some(value) if value.eq_ignore_ascii_case("crossref") => Provenance::Crossref,
        _ => Provenance::Mined,
    }
}

/// Mine one reference object: count what the matcher found, retain
/// confirmed and failed-arXiv rows that pass the self-citation and
/// provenance filters, and queue ambiguity rows.
fn mine_reference(
    scope: &mut ReferenceScope<'_>,
    ctx: &WorkerContext<'_>,
    tx: &Sender<WorkerEvent>,
    stats: &mut FileStats,
    rows: &mut Batch<PartitionRow>,
    ambiguous: &mut Batch<Value>,
) -> Result<()> {
    let matches = match_reference(scope.reference, ctx.index, ctx.lookup_bytes)?;
    stats.confirmed_matches += matches.confirmed.len() as u64;
    for found in &matches.confirmed {
        match found.match_type {
            MatchType::Doi => stats.doi_matches += 1,
            MatchType::Url => stats.url_matches += 1,
            MatchType::Arxiv => stats.arxiv_matches += 1,
        }
    }
    stats.ambiguous_urls += matches.ambiguous.len() as u64;
    stats.invalid_arxiv += matches.failed_arxiv.len() as u64;
    stats.unmatched_dois += matches.unmatched_dois;
    stats.unmatched_urls += matches.unmatched_urls;
    stats.malformed_fields += matches.malformed_fields;

    let failed = matches
        .failed_arxiv
        .into_iter()
        .map(|arxiv| {
            let canonical = normalize_doi(&arxiv.arxiv_doi)
                .ok_or_else(|| anyhow!("arXiv DOI {:?} failed normalization", arxiv.arxiv_doi))?;
            let provenance = failed_arxiv_provenance(scope.reference, &canonical);
            Ok((canonical, provenance, MatchType::Arxiv, arxiv.raw))
        })
        .collect::<Result<Vec<_>>>()?;
    let confirmed = matches.confirmed.into_iter().map(|found| {
        (
            found.doi,
            found.provenance,
            found.match_type,
            found.raw_match,
        )
    });
    for (cited, provenance, match_type, raw_match) in confirmed.chain(failed) {
        if cited == scope.citing {
            stats.filtered_self_citations += 1;
            continue;
        }
        if !should_include_provenance_filter(ctx.provenance_filter, provenance.as_str()) {
            stats.filtered_provenance += 1;
            continue;
        }
        let row = scope.row(cited, provenance, match_type, raw_match);
        let row_bytes = row.accounted_bytes();
        rows.push(tx, row, row_bytes)?;
    }
    for unresolved in matches.ambiguous {
        let row_bytes = scope.ref_json().len()
            + scope.citing.len()
            + unresolved.url.len()
            + unresolved.raw_match.len()
            + unresolved
                .candidate_dois
                .iter()
                .map(String::len)
                .sum::<usize>()
            + AMBIGUITY_ROW_OVERHEAD;
        let row = json!({
            "citing_doi": scope.citing,
            "ref_index": scope.ref_index,
            "url": unresolved.url,
            "raw_match": unresolved.raw_match,
            "candidate_dois": unresolved.candidate_dois,
            "reference": scope.reference,
        });
        ambiguous.push(tx, row, row_bytes)?;
    }
    Ok(())
}

/// Mine one member, sending its rows in bounded batches; the returned
/// counters describe the whole member.
fn extract_member(
    entry: &CrossrefEntry,
    ctx: &WorkerContext<'_>,
    tx: &Sender<WorkerEvent>,
) -> Result<FileStats> {
    // A struct also deserializes from a JSON array (positional fields), so
    // the object shape is checked explicitly: a member is `{"items": [...]}`.
    let first = entry
        .bytes
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .copied();
    ensure!(
        first == Some(b'{'),
        "invalid top-level JSON in member {}: not a JSON object",
        entry.filename
    );
    let file: CrossrefFile = serde_json::from_slice(&entry.bytes)
        .with_context(|| format!("invalid top-level JSON in member {}", entry.filename))?;
    let mut stats = FileStats::default();
    let mut rows = Batch::new(ctx.batch_limit, WorkerEvent::Rows);
    let mut ambiguous = Batch::new(ctx.batch_limit, WorkerEvent::Ambiguous);
    for raw_item in &file.items {
        stats.items += 1;
        let Ok(item) = serde_json::from_str::<CrossrefItem>(raw_item.get()) else {
            stats.malformed_fields += 1;
            continue;
        };
        let citing = item
            .doi
            .and_then(|raw| serde_json::from_str::<String>(raw.get()).ok())
            .and_then(|doi| normalize_doi(&doi));
        let Some(citing) = citing else {
            stats.malformed_fields += 1;
            continue;
        };
        let Some(raw_references) = item.reference else {
            continue;
        };
        let Ok(references) = serde_json::from_str::<Vec<&RawValue>>(raw_references.get()) else {
            stats.malformed_fields += 1;
            continue;
        };
        for (index, raw_reference) in references.iter().enumerate() {
            stats.references += 1;
            // The member already parsed as JSON, so a reference that cannot
            // become a `Value` (nesting past serde_json's recursion limit, a
            // number out of range) is one malformed reference, not a fatal
            // source error.
            let Ok(reference) = serde_json::from_str::<Value>(raw_reference.get()) else {
                stats.malformed_references += 1;
                continue;
            };
            if !reference.is_object() {
                stats.malformed_references += 1;
                continue;
            }
            let ref_index = u32::try_from(index)
                .with_context(|| format!("work {citing} has more references than fit u32"))?;
            let mut scope = ReferenceScope {
                citing: &citing,
                ref_index,
                reference: &reference,
                ref_json: None,
            };
            mine_reference(&mut scope, ctx, tx, &mut stats, &mut rows, &mut ambiguous)
                .with_context(|| {
                    format!("member {} work {citing} reference {index}", entry.filename)
                })?;
        }
    }
    rows.flush(tx)?;
    ambiguous.flush(tx)?;
    Ok(stats)
}

fn run_worker(
    entries: Receiver<CrossrefEntry>,
    tx: Sender<WorkerEvent>,
    ctx: &WorkerContext<'_>,
) -> Result<()> {
    for entry in entries.iter() {
        match extract_member(&entry, ctx, &tx) {
            Ok(stats) => {
                let event = WorkerEvent::FileComplete {
                    filename: entry.filename,
                    stats,
                };
                if tx.send(event).is_err() {
                    return Ok(()); // the writer stopped; its error is the cause
                }
            }
            Err(error) => {
                return match tx.send(WorkerEvent::Failed(error)) {
                    Ok(()) => Ok(()),
                    Err(SendError(WorkerEvent::Failed(error))) => Err(error),
                    Err(_) => Ok(()),
                };
            }
        }
    }
    Ok(())
}

/// State owned by the writer thread.
struct WriterState<'a> {
    partitions: SegmentedPartitionWriter,
    ambiguities: AmbiguitySegments,
    checkpoint: &'a mut MemberCheckpoint<FileStats>,
    pending: Vec<(String, FileStats)>,
    /// Counters of members completed by this run, for progress only.
    live: FileStats,
    members: u64,
    rows_received: u64,
    ambiguity_rows_received: u64,
    started: Instant,
    commit_bytes: usize,
    checkpoint_members: usize,
}

/// Make everything durable in order, then record the pending members:
/// partition segments, the ambiguity segment, the checkpoint lines.
/// `trigger` names what caused the commit, for the checkpoint event.
fn commit_pending(state: &mut WriterState<'_>, trigger: &str) -> Result<()> {
    if state.pending.is_empty() {
        return Ok(());
    }
    let flushed_bytes = state.partitions.buffered_bytes();
    state.partitions.flush_all()?;
    state.ambiguities.commit()?;
    let count = state.pending.len();
    for (filename, stats) in state.pending.drain(..) {
        state.checkpoint.append_member(&filename, stats)?;
    }
    state.checkpoint.sync()?;
    info!(
        "event=checkpoint trigger={trigger} members_committed={count} members_total={} \
         flushed_bytes={flushed_bytes} rows_written={}",
        state.checkpoint.members().len(),
        state.partitions.total_rows_written()
    );
    Ok(())
}

/// Commit once the writer holds at least half its budget, so segments stay
/// large, or once enough members are pending, which bounds what a crash
/// replays. Nothing is committed while no member is complete: the writer's
/// own budget still bounds memory by flushing its largest buffers.
fn maybe_commit(state: &mut WriterState<'_>) -> Result<()> {
    if state.pending.is_empty() {
        return Ok(());
    }
    if state.pending.len() >= state.checkpoint_members {
        commit_pending(state, "members")
    } else if state.partitions.buffered_bytes() >= state.commit_bytes {
        commit_pending(state, "bytes")
    } else {
        Ok(())
    }
}

fn stats_fields(stats: &FileStats) -> String {
    format!(
        "items={} references={} confirmed={} doi={} url={} arxiv={} ambiguous={} \
         invalid_arxiv={} unmatched_dois={} unmatched_urls={} malformed_references={} \
         malformed_fields={} self_citations={} filtered_provenance={}",
        stats.items,
        stats.references,
        stats.confirmed_matches,
        stats.doi_matches,
        stats.url_matches,
        stats.arxiv_matches,
        stats.ambiguous_urls,
        stats.invalid_arxiv,
        stats.unmatched_dois,
        stats.unmatched_urls,
        stats.malformed_references,
        stats.malformed_fields,
        stats.filtered_self_citations,
        stats.filtered_provenance
    )
}

fn log_progress(state: &WriterState<'_>) {
    let elapsed = state.started.elapsed().as_secs_f64().max(f64::EPSILON);
    info!(
        "event=progress members={} {} rows={} ambiguity_rows={} writer_buffered_bytes={} \
         elapsed_s={elapsed:.1} references_per_s={:.0} members_per_s={:.2}",
        state.members,
        stats_fields(&state.live),
        state.rows_received,
        state.ambiguity_rows_received,
        state.partitions.buffered_bytes(),
        state.live.references as f64 / elapsed,
        state.members as f64 / elapsed
    );
}

/// The writer loop. Takes the receiver by value so that returning early
/// drops it and every blocked sender fails instead of waiting.
fn run_writer(events: Receiver<WorkerEvent>, state: &mut WriterState<'_>) -> Result<()> {
    for event in events {
        if let WorkerEvent::Failed(error) = event {
            return Err(error.context("extraction worker failed"));
        }
        handle_event(state, event).context("extraction writer failed")?;
    }
    Ok(())
}

fn handle_event(state: &mut WriterState<'_>, event: WorkerEvent) -> Result<()> {
    match event {
        WorkerEvent::Rows(batch) => {
            state.rows_received += batch.len() as u64;
            for row in batch {
                state.partitions.write(row)?;
            }
            maybe_commit(state)?;
        }
        WorkerEvent::Ambiguous(batch) => {
            state.ambiguity_rows_received += batch.len() as u64;
            state.ambiguities.append(&batch)?;
        }
        WorkerEvent::FileComplete { filename, stats } => {
            state.live.add(&stats);
            state.members += 1;
            state.pending.push((filename, stats));
            if state.members.is_multiple_of(PROGRESS_MEMBER_INTERVAL) {
                log_progress(state);
            }
            maybe_commit(state)?;
        }
        WorkerEvent::Failed(error) => return Err(error),
    }
    Ok(())
}

fn run_extraction(
    cfg: &FullModeConfig,
    ws: &Workspace,
    input: CrossrefInput,
    index: &DataciteIndex,
    checkpoint: &mut MemberCheckpoint<FileStats>,
) -> Result<()> {
    let partitions = SegmentedPartitionWriter::with_limits(
        &ws.partitions,
        WriterLimits {
            memory_bytes: cfg.writer_memory_bytes,
            segment_rows: SEGMENT_ROWS,
            mode: PartitionMode::Hashed {
                count: cfg.partitions,
            },
        },
    )?;
    let ambiguities = AmbiguitySegments::open(&ws.ambiguities)?;
    let completed = checkpoint.completed_snapshot();
    let n_workers = num_cpus::get().saturating_sub(2).max(1);

    // Peak estimate: raw members queued or in flight, queued batches, each
    // worker's two open batches, and the writer's buffers. The writer's
    // budget is the hard cap; commits normally fire at `commit_bytes`, so
    // the buffers usually hold that plus at most one batch.
    let entry_queue_bytes = (ENTRY_QUEUE_DEPTH + n_workers + 1).saturating_mul(cfg.max_entry_bytes);
    let result_queue_bytes = cfg.queue_batches.saturating_mul(cfg.result_batch_bytes);
    let worker_local_bytes = n_workers.saturating_mul(2 * cfg.result_batch_bytes);
    info!(
        "event=resource_estimate workers={n_workers} entry_queue_bytes={entry_queue_bytes} \
         result_queue_bytes={result_queue_bytes} worker_local_bytes={worker_local_bytes} \
         writer_bytes={} commit_bytes={} checkpoint_members={} total_bytes={}",
        cfg.writer_memory_bytes,
        cfg.commit_bytes,
        cfg.checkpoint_members,
        entry_queue_bytes
            .saturating_add(result_queue_bytes)
            .saturating_add(worker_local_bytes)
            .saturating_add(cfg.writer_memory_bytes)
    );
    info!(
        "event=phase phase=extraction status=started workers={n_workers} \
         members_already_completed={} partitions={} result_batch_bytes={} queue_batches={} \
         commit_bytes={} checkpoint_members={}",
        completed.len(),
        cfg.partitions,
        cfg.result_batch_bytes,
        cfg.queue_batches,
        cfg.commit_bytes,
        cfg.checkpoint_members
    );

    let (entry_tx, entry_rx) = crossbeam_channel::bounded::<CrossrefEntry>(ENTRY_QUEUE_DEPTH);
    let (event_tx, event_rx) = crossbeam_channel::bounded::<WorkerEvent>(cfg.queue_batches);
    let ctx = WorkerContext {
        index,
        provenance_filter: &cfg.provenance_filter,
        lookup_bytes: cfg.lookup_bytes,
        batch_limit: cfg.result_batch_bytes,
    };
    let mut state = WriterState {
        partitions,
        ambiguities,
        checkpoint,
        pending: Vec::new(),
        live: FileStats::default(),
        members: 0,
        rows_received: 0,
        ambiguity_rows_received: 0,
        started: Instant::now(),
        commit_bytes: cfg.commit_bytes,
        checkpoint_members: cfg.checkpoint_members,
    };
    let max_entry_bytes = cfg.max_entry_bytes;

    let (writer_result, reader_result, worker_results) = std::thread::scope(|s| {
        // Reader: owns the source; skips checkpointed members before they
        // are buffered.
        let reader = s.spawn(move || -> Result<u64> {
            let mut skipped = 0u64;
            visit_crossref_entries_limited(input, max_entry_bytes, |entry| {
                if completed.contains(&entry.filename) {
                    skipped += 1;
                    return Ok(());
                }
                entry_tx.send(entry).map_err(|_| {
                    anyhow!("extraction workers stopped before the source was fully read")
                })
            })?;
            Ok(skipped)
        });
        let workers: Vec<_> = (0..n_workers)
            .map(|_| {
                let entries = entry_rx.clone();
                let tx = event_tx.clone();
                let ctx = &ctx;
                s.spawn(move || run_worker(entries, tx, ctx))
            })
            .collect();
        drop(entry_rx);
        drop(event_tx); // the writer's receiver closes once every worker exits

        let writer_result = run_writer(event_rx, &mut state);
        let reader_result = reader
            .join()
            .map_err(|_| anyhow!("reader thread panicked"))
            .and_then(|result| result);
        let worker_results: Vec<Result<()>> = workers
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow!("worker thread panicked"))
                    .and_then(|result| result)
            })
            .collect();
        (writer_result, reader_result, worker_results)
    });
    writer_result?;
    // Members the writer completed are durable progress even when the
    // source or a worker failed afterwards; record them so a resume does
    // not replay them, but let the failure itself be what is reported.
    let progress = commit_pending(&mut state, "final");
    let skipped = reader_result.context("reading the Crossref source failed")?;
    for result in worker_results {
        result.context("extraction worker failed")?;
    }
    progress?;
    state.checkpoint.mark_complete()?;
    log_progress(&state);
    info!(
        "event=phase phase=extraction status=members_durable members={} skipped={skipped} \
         rows={} ambiguity_rows={} elapsed_s={:.1}",
        state.members,
        state.rows_received,
        state.ambiguity_rows_received,
        state.started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Run totals from the checkpoint's member records, so a member replayed
/// after a crash is counted once.
fn checkpoint_totals(checkpoint: &MemberCheckpoint<FileStats>) -> (u64, FileStats) {
    let mut totals = FileStats::default();
    for (_, stats) in checkpoint.members() {
        totals.add(stats);
    }
    (checkpoint.members().len() as u64, totals)
}

// ---------------------------------------------------------------- report

/// Write the report through a temporary file and a rename. Only a complete
/// run has a report, so an incomplete one is refused here as well.
fn write_report(path: &Path, report: &ExtractionReport) -> Result<()> {
    ensure!(
        report.completed && report.fatal_source_errors == 0,
        "refusing to write a report for an incomplete run"
    );
    let mut bytes = serde_json::to_vec_pretty(report).context("serializing the report")?;
    bytes.push(b'\n');
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("moving {} to {}", tmp.display(), path.display()))?;
    sync_parent(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_sorts_keys_and_uses_compact_separators() {
        let value = json!({"b": 1, "a": {"d": [1, {"z": true, "y": null}], "c": "x"}});
        assert_eq!(
            canonical_json(&value),
            r#"{"a":{"c":"x","d":[1,{"y":null,"z":true}]},"b":1}"#
        );
        // Known vector: SHA-256 of "abc".
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hashed_partition_names_are_recognized_exactly() {
        assert!(is_hashed_partition_name("h00ff"));
        assert!(is_hashed_partition_name("h0"));
        assert!(!is_hashed_partition_name("h"));
        assert!(!is_hashed_partition_name("H00ff"));
        assert!(!is_hashed_partition_name("h00FF"));
        assert!(!is_hashed_partition_name("hxyz"));
        assert!(!is_hashed_partition_name("_aggregation"));
        assert!(!is_hashed_partition_name("10.1234"));
    }

    #[test]
    fn failed_arxiv_provenance_needs_the_same_canonical_doi() {
        let canonical = "10.48550/arxiv.2403.03542";
        let asserted = json!({"DOI": "10.48550/arXiv.2403.03542", "doi-asserted-by": "publisher"});
        assert_eq!(
            failed_arxiv_provenance(&asserted, canonical),
            Provenance::Publisher
        );
        let crossref = json!({"DOI": "https://doi.org/10.48550/arXiv.2403.03542", "doi-asserted-by": "Crossref"});
        assert_eq!(
            failed_arxiv_provenance(&crossref, canonical),
            Provenance::Crossref
        );
        let versioned =
            json!({"DOI": "10.48550/arXiv.2403.03542v2", "doi-asserted-by": "publisher"});
        assert_eq!(
            failed_arxiv_provenance(&versioned, canonical),
            Provenance::Mined
        );
        let unasserted = json!({"DOI": "10.48550/arXiv.2403.03542"});
        assert_eq!(
            failed_arxiv_provenance(&unasserted, canonical),
            Provenance::Mined
        );
        let text_only = json!({"unstructured": "arXiv:2403.03542"});
        assert_eq!(
            failed_arxiv_provenance(&text_only, canonical),
            Provenance::Mined
        );
    }

    #[test]
    fn batches_flush_before_exceeding_the_limit_and_reject_oversized_rows() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut batch = Batch::new(10, WorkerEvent::Ambiguous);
        batch.push(&tx, json!(1), 4).unwrap();
        batch.push(&tx, json!(2), 6).unwrap();
        assert!(rx.try_recv().is_err(), "exactly at the limit stays local");
        batch.push(&tx, json!(3), 1).unwrap();
        match rx.try_recv().unwrap() {
            WorkerEvent::Ambiguous(rows) => assert_eq!(rows, vec![json!(1), json!(2)]),
            _ => panic!("expected the first two rows"),
        }
        let err = batch.push(&tx, json!(4), 11).unwrap_err();
        assert!(err.to_string().contains("result batch budget"));
        batch.flush(&tx).unwrap();
        match rx.try_recv().unwrap() {
            WorkerEvent::Ambiguous(rows) => assert_eq!(rows, vec![json!(3)]),
            _ => panic!("expected the remaining row"),
        }
        batch.flush(&tx).unwrap();
        assert!(rx.try_recv().is_err(), "an empty batch sends nothing");
    }

    #[test]
    fn ambiguity_segments_commit_atomically_and_resume_numbering() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("ambiguities");
        let mut segments = AmbiguitySegments::open(&root).unwrap();
        assert_eq!(
            segments.commit().unwrap(),
            0,
            "nothing open, nothing written"
        );
        segments
            .append(&[json!({"a": 1}), json!({"b": 2})])
            .unwrap();
        assert!(root.join("segment_0000.jsonl.part").exists());
        assert!(!root.join("segment_0000.jsonl").exists());
        assert_eq!(segments.commit().unwrap(), 2);
        assert!(!root.join("segment_0000.jsonl.part").exists());
        assert_eq!(
            fs::read_to_string(root.join("segment_0000.jsonl")).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
        segments.append(&[json!(3)]).unwrap();
        drop(segments); // simulate a crash with an open part file
        assert!(root.join("segment_0001.jsonl.part").exists());
        let mut segments = AmbiguitySegments::open(&root).unwrap();
        assert!(!root.join("segment_0001.jsonl.part").exists());
        segments.append(&[json!(4)]).unwrap();
        segments.commit().unwrap();
        assert!(root.join("segment_0001.jsonl").exists());
    }
}
