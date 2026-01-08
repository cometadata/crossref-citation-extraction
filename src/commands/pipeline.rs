use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::{debug, info, warn};
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tar::Archive;
use uuid::Uuid;

use crate::cli::{PipelineArgs, ValidateArgs};
use crate::commands::validate;
use crate::common::{format_elapsed, setup_logging, ValidateStats};
use crate::common::ArxivMatch;
use crate::extract::extract_arxiv_matches_from_text;
use crate::streaming::{invert_partitions, Checkpoint, InvertStats, PartitionWriter, PipelinePhase};

/// Statistics from the fused convert+extract step
#[derive(Debug, Clone, Default)]
pub struct FusedStats {
    pub json_files_processed: usize,
    pub total_records: usize,
    pub total_references: usize,
    pub references_with_hint: usize,
    pub references_with_matches: usize,
    pub total_arxiv_ids_extracted: usize,
}

/// Combined pipeline statistics
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    pub fused: FusedStats,
    pub invert: InvertStats,
    pub validate: Option<ValidateStats>,
}

/// Pipeline context for managing directories and cleanup
struct PipelineContext {
    partition_dir: PathBuf,
    output_parquet: PathBuf,
    output_jsonl: PathBuf,
    checkpoint_path: PathBuf,
    keep_intermediates: bool,
}

impl PipelineContext {
    fn new(args: &PipelineArgs) -> Result<Self> {
        let run_id = &Uuid::new_v4().to_string()[..8];

        let temp_dir = args.temp_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir());

        let partition_dir = temp_dir.join(format!("partitions_{}", run_id));
        fs::create_dir_all(&partition_dir)
            .with_context(|| format!("Failed to create partition directory: {}", partition_dir.display()))?;

        let output_parquet = temp_dir.join(format!("inverted_{}.parquet", run_id));
        let output_jsonl = temp_dir.join(format!("inverted_{}.jsonl", run_id));
        let checkpoint_path = partition_dir.join("checkpoint.json");

        Ok(Self {
            partition_dir,
            output_parquet,
            output_jsonl,
            checkpoint_path,
            keep_intermediates: args.keep_intermediates,
        })
    }

    fn cleanup(&self) -> Result<()> {
        if self.keep_intermediates {
            info!("Keeping intermediate files:");
            info!("  Partition directory: {}", self.partition_dir.display());
            info!("  Inverted parquet: {}", self.output_parquet.display());
            info!("  Inverted JSONL: {}", self.output_jsonl.display());
            return Ok(());
        }

        info!("Cleaning up intermediate files...");

        // Remove partition directory and all contents
        if self.partition_dir.exists() {
            fs::remove_dir_all(&self.partition_dir)
                .with_context(|| format!("Failed to remove partition directory: {}", self.partition_dir.display()))?;
        }

        // Remove intermediate parquet (keep JSONL if it's the final output)
        if self.output_parquet.exists() {
            fs::remove_file(&self.output_parquet)
                .with_context(|| format!("Failed to remove: {}", self.output_parquet.display()))?;
        }

        Ok(())
    }
}

impl Drop for PipelineContext {
    fn drop(&mut self) {
        if !self.keep_intermediates {
            let _ = fs::remove_dir_all(&self.partition_dir);
            let _ = fs::remove_file(&self.output_parquet);
        }
    }
}

/// Check if a reference might contain an arXiv ID (fast pre-filter)
fn check_arxiv_hint(
    doi: Option<&str>,
    unstructured: Option<&str>,
    article_title: Option<&str>,
    journal_title: Option<&str>,
    url: Option<&str>,
) -> bool {
    let check = |s: Option<&str>| s.map(|t| t.to_lowercase().contains("arxiv")).unwrap_or(false);
    let doi_check = doi.map(|d| d.to_lowercase().contains("10.48550")).unwrap_or(false);

    check(unstructured) || check(article_title) || check(journal_title) || check(url) || doi_check
}

/// Extract arXiv matches from a reference, returning (raw_matches, normalized_ids)
fn extract_from_reference(reference: &Value) -> Option<(Vec<String>, Vec<String>)> {
    let fields = [
        reference.get("unstructured").and_then(|v| v.as_str()),
        reference.get("DOI").and_then(|v| v.as_str()),
        reference.get("URL").and_then(|v| v.as_str()),
        reference.get("article-title").and_then(|v| v.as_str()),
        reference.get("journal-title").and_then(|v| v.as_str()),
    ];

    let mut all_matches: Vec<ArxivMatch> = Vec::new();
    for field in fields.iter().filter_map(|f| *f) {
        all_matches.extend(extract_arxiv_matches_from_text(field));
    }

    if all_matches.is_empty() {
        return None;
    }

    // Deduplicate by normalized ID
    let mut seen = std::collections::HashSet::new();
    let mut raw_matches = Vec::new();
    let mut normalized_ids = Vec::new();

    for m in all_matches {
        if seen.insert(m.id.clone()) {
            raw_matches.push(m.raw);
            normalized_ids.push(m.id);
        }
    }

    Some((raw_matches, normalized_ids))
}

/// Phase 1: Stream through tar.gz, extract arXiv references, and write to partitions
fn run_fused_convert_extract(
    input_path: &str,
    writer: &mut PartitionWriter,
    checkpoint: &mut Checkpoint,
    progress_callback: impl Fn(usize, usize),
) -> Result<FusedStats> {
    let start = Instant::now();
    info!("Opening tar.gz archive: {}", input_path);

    let tar_file = File::open(input_path)
        .with_context(|| format!("Failed to open input file: {}", input_path))?;
    let gz_decoder = GzDecoder::new(tar_file);
    let mut archive = Archive::new(gz_decoder);

    let mut stats = FusedStats::default();
    let mut entry_count = 0;

    for entry_result in archive.entries()? {
        let mut entry = entry_result.context("Failed to read tar entry")?;
        let path = entry.path()?.to_path_buf();

        if !path.extension().map_or(false, |ext| ext == "json") {
            continue;
        }

        entry_count += 1;

        // Skip already-processed entries (resume support)
        if entry_count <= checkpoint.tar_entries_processed {
            continue;
        }

        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let mut content = String::new();
        entry
            .read_to_string(&mut content)
            .with_context(|| format!("Failed to read content from: {}", path.display()))?;

        let json_data: Value = match serde_json::from_str(&content) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to parse JSON in {}: {}", path.display(), e);
                continue;
            }
        };

        drop(content); // Free memory

        let items = match json_data.get("items") {
            Some(Value::Array(items)) => items,
            _ => {
                warn!("No 'items' array found in {}", path.display());
                continue;
            }
        };

        stats.json_files_processed += 1;

        for record in items {
            stats.total_records += 1;

            let doi = match record.get("DOI").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => continue,
            };

            let references = match record.get("reference") {
                Some(Value::Array(refs)) => refs,
                _ => continue,
            };

            for (idx, reference) in references.iter().enumerate() {
                stats.total_references += 1;

                // Quick hint check
                let ref_doi = reference.get("DOI").and_then(|v| v.as_str());
                let unstructured = reference.get("unstructured").and_then(|v| v.as_str());
                let article_title = reference.get("article-title").and_then(|v| v.as_str());
                let journal_title = reference.get("journal-title").and_then(|v| v.as_str());
                let url = reference.get("URL").and_then(|v| v.as_str());

                let has_hint = check_arxiv_hint(ref_doi, unstructured, article_title, journal_title, url);
                if !has_hint {
                    continue;
                }

                stats.references_with_hint += 1;

                // Extract arXiv IDs
                if let Some((raw_matches, normalized_ids)) = extract_from_reference(reference) {
                    let ref_json = serde_json::to_string(reference).unwrap_or_default();

                    let written = writer.write_extracted_ref(
                        doi,
                        idx as u32,
                        &ref_json,
                        &raw_matches,
                        &normalized_ids,
                    )?;

                    stats.references_with_matches += 1;
                    stats.total_arxiv_ids_extracted += written;
                }
            }
        }

        // Update checkpoint periodically
        if stats.json_files_processed % 100 == 0 {
            checkpoint.tar_entries_processed = entry_count;
            checkpoint.stats.json_files_processed = stats.json_files_processed;
            checkpoint.stats.total_records = stats.total_records;
            checkpoint.stats.total_references = stats.total_references;
            checkpoint.stats.references_with_matches = stats.references_with_matches;
            checkpoint.stats.total_arxiv_ids_extracted = stats.total_arxiv_ids_extracted;

            progress_callback(stats.json_files_processed, stats.total_references);
        }

        debug!(
            "Processed {}: {} items",
            file_name,
            items.len()
        );
    }

    // Final flush
    writer.flush_all()?;

    info!(
        "Fused convert+extract complete in {}: {} refs -> {} matches ({} arxiv IDs)",
        format_elapsed(start.elapsed()),
        stats.total_references,
        stats.references_with_matches,
        stats.total_arxiv_ids_extracted
    );

    Ok(stats)
}

pub fn run_pipeline(args: PipelineArgs) -> Result<PipelineStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation pipeline (Fused Streaming)");
    info!("Input: {}", args.input);
    info!("DataCite records: {}", args.records);
    info!("Output: {}", args.output);

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    let ctx = PipelineContext::new(&args)?;
    info!("Partition directory: {}", ctx.partition_dir.display());

    // Load or create checkpoint
    let mut checkpoint = Checkpoint::load(&ctx.checkpoint_path)?
        .unwrap_or_else(|| {
            let run_id = ctx.partition_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("run");
            Checkpoint::new(run_id)
        });

    let mut stats = PipelineStats::default();

    // Phase 1: Fused Convert + Extract (if not already complete)
    if checkpoint.phase == PipelinePhase::ConvertExtract {
        info!("");
        info!("=== PHASE 1/3: Fused Convert + Extract + Partition ===");
        info!("");

        let flush_threshold = args.batch_size / 50; // ~100K rows per partition flush
        let mut writer = PartitionWriter::new(&ctx.partition_dir, flush_threshold.max(10_000))?;

        let fused_stats = run_fused_convert_extract(
            &args.input,
            &mut writer,
            &mut checkpoint,
            |files, refs| {
                debug!("Progress: {} files, {} references", files, refs);
            },
        )?;

        stats.fused = fused_stats;

        info!("Phase 1 complete:");
        info!("  JSON files processed: {}", stats.fused.json_files_processed);
        info!("  Total records: {}", stats.fused.total_records);
        info!("  Total references: {}", stats.fused.total_references);
        info!("  References with matches: {}", stats.fused.references_with_matches);
        info!("  Total arXiv IDs: {}", stats.fused.total_arxiv_ids_extracted);
        info!("  Partitions created: {}", writer.partition_count());

        checkpoint.start_invert_phase();
        checkpoint.save(&ctx.checkpoint_path)?;
    } else {
        info!("Resuming from phase: {:?}", checkpoint.phase);
        // Restore stats from checkpoint
        stats.fused = FusedStats {
            json_files_processed: checkpoint.stats.json_files_processed,
            total_records: checkpoint.stats.total_records,
            total_references: checkpoint.stats.total_references,
            references_with_hint: 0, // Not tracked in checkpoint
            references_with_matches: checkpoint.stats.references_with_matches,
            total_arxiv_ids_extracted: checkpoint.stats.total_arxiv_ids_extracted,
        };
    }

    // Phase 2: Parallel Partition Invert
    if checkpoint.phase == PipelinePhase::Invert {
        info!("");
        info!("=== PHASE 2/3: Parallel Partition Invert ===");
        info!("");

        let invert_stats = invert_partitions(
            &ctx.partition_dir,
            &ctx.output_parquet,
            Some(&ctx.output_jsonl),
            &mut checkpoint,
        )?;

        stats.invert = invert_stats;

        info!("Phase 2 complete:");
        info!("  Partitions processed: {}", stats.invert.partitions_processed);
        info!("  Unique arXiv works: {}", stats.invert.unique_arxiv_works);
        info!("  Total citations: {}", stats.invert.total_citations);

        checkpoint.mark_complete();
        checkpoint.save(&ctx.checkpoint_path)?;
    }

    // Phase 3: Validate (optional, but part of original pipeline)
    info!("");
    info!("=== PHASE 3/3: Validating citations ===");
    info!("");

    let validate_args = ValidateArgs {
        input: ctx.output_jsonl.to_string_lossy().to_string(),
        records: args.records.clone(),
        output_valid: args.output.clone(),
        output_failed: args.output_failed.clone().unwrap_or_else(|| {
            ctx.partition_dir.join("failed_temp.jsonl").to_string_lossy().to_string()
        }),
        concurrency: args.concurrency,
        timeout: args.timeout,
        log_level: "OFF".to_string(),
    };

    let rt = tokio::runtime::Runtime::new()?;
    let validate_stats = rt.block_on(validate::run_validate_async(validate_args))
        .context("Validate step failed")?;

    setup_logging(&args.log_level)?;

    info!("Phase 3 complete:");
    info!("  Matched in DataCite: {}", validate_stats.matched_in_datacite);
    info!("  Resolution resolved: {}", validate_stats.resolution_resolved);
    info!("  Resolution failed: {}", validate_stats.resolution_failed);
    info!("  Total valid: {}", validate_stats.total_valid);
    info!("  Total failed: {}", validate_stats.total_failed);

    stats.validate = Some(validate_stats.clone());

    // Cleanup temp failed file if not requested
    if args.output_failed.is_none() {
        let temp_failed = ctx.partition_dir.join("failed_temp.jsonl");
        if temp_failed.exists() {
            let _ = fs::remove_file(temp_failed);
        }
    }

    ctx.cleanup()?;

    let total_time = start_time.elapsed();

    info!("");
    info!("==================== PIPELINE COMPLETE ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("");
    info!("Fused Convert+Extract:");
    info!("  JSON files processed: {}", stats.fused.json_files_processed);
    info!("  Total records scanned: {}", stats.fused.total_records);
    info!("  Total references: {}", stats.fused.total_references);
    info!("  References with matches: {}", stats.fused.references_with_matches);
    info!("  Total arXiv IDs extracted: {}", stats.fused.total_arxiv_ids_extracted);
    info!("");
    info!("Invert:");
    info!("  Partitions processed: {}", stats.invert.partitions_processed);
    info!("  Unique arXiv works: {}", stats.invert.unique_arxiv_works);
    info!("  Total citations: {}", stats.invert.total_citations);
    info!("");
    if let Some(ref vs) = stats.validate {
        info!("Validate:");
        info!("  Matched in DataCite: {}", vs.matched_in_datacite);
        info!("  Total valid: {}", vs.total_valid);
        info!("  Total failed: {}", vs.total_failed);
        info!("");
    }
    info!("Output: {}", args.output);
    if let Some(ref failed) = args.output_failed {
        info!("Output failed: {}", failed);
    }
    info!("===========================================================");

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_arxiv_hint() {
        assert!(check_arxiv_hint(None, Some("arXiv:2403.12345"), None, None, None));
        assert!(check_arxiv_hint(Some("10.48550/arXiv.2403.12345"), None, None, None, None));
        assert!(check_arxiv_hint(None, None, None, None, Some("https://arxiv.org/abs/2403.12345")));
        assert!(!check_arxiv_hint(Some("10.1234/test"), Some("Regular ref"), None, None, None));
    }

    #[test]
    fn test_extract_from_reference() {
        let ref_json = serde_json::json!({
            "unstructured": "See arXiv:2403.12345 for details"
        });

        let result = extract_from_reference(&ref_json);
        assert!(result.is_some());

        let (raw, norm) = result.unwrap();
        assert_eq!(norm.len(), 1);
        assert_eq!(norm[0], "2403.12345");
        assert!(raw[0].contains("2403.12345"));
    }

    #[test]
    fn test_extract_from_reference_no_match() {
        let ref_json = serde_json::json!({
            "unstructured": "A regular citation"
        });

        let result = extract_from_reference(&ref_json);
        assert!(result.is_none());
    }
}
