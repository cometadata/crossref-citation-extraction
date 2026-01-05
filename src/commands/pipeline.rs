use anyhow::{Context, Result};
use log::info;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

use crate::cli::{ExtractArgs, InvertArgs, PipelineArgs, ValidateArgs};
use crate::commands::{extract, invert, validate};
use crate::common::{format_elapsed, setup_logging, ExtractStats, InvertStats, ValidateStats};

/// Context for managing pipeline state and temp files
struct PipelineContext {
    temp_dir: PathBuf,
    extract_output: PathBuf,
    invert_output: PathBuf,
    keep_intermediates: bool,
}

impl PipelineContext {
    fn new(args: &PipelineArgs) -> Result<Self> {
        let run_id = &Uuid::new_v4().to_string()[..8];

        let temp_dir = args.temp_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir());

        fs::create_dir_all(&temp_dir)
            .with_context(|| format!("Failed to create temp directory: {}", temp_dir.display()))?;

        let extract_output = temp_dir.join(format!("arxiv_refs_{}.jsonl", run_id));
        let invert_output = temp_dir.join(format!("arxiv_citations_{}.jsonl", run_id));

        Ok(Self {
            temp_dir,
            extract_output,
            invert_output,
            keep_intermediates: args.keep_intermediates,
        })
    }

    fn cleanup(&self) -> Result<()> {
        if self.keep_intermediates {
            info!("Keeping intermediate files:");
            info!("  Extract output: {}", self.extract_output.display());
            info!("  Invert output: {}", self.invert_output.display());
            return Ok(());
        }

        info!("Cleaning up intermediate files...");

        if self.extract_output.exists() {
            fs::remove_file(&self.extract_output)
                .with_context(|| format!("Failed to remove: {}", self.extract_output.display()))?;
        }
        if self.invert_output.exists() {
            fs::remove_file(&self.invert_output)
                .with_context(|| format!("Failed to remove: {}", self.invert_output.display()))?;
        }

        Ok(())
    }
}

impl Drop for PipelineContext {
    fn drop(&mut self) {
        // Best-effort cleanup on drop (e.g., if pipeline panics)
        if !self.keep_intermediates {
            let _ = fs::remove_file(&self.extract_output);
            let _ = fs::remove_file(&self.invert_output);
        }
    }
}

/// Run the full pipeline: extract -> invert -> validate
pub fn run_pipeline(args: PipelineArgs) -> Result<(ExtractStats, InvertStats, ValidateStats)> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation pipeline");
    info!("Input: {}", args.input);
    info!("DataCite records: {}", args.records);
    info!("Output: {}", args.output);
    if let Some(ref failed) = args.output_failed {
        info!("Output failed: {}", failed);
    }

    let ctx = PipelineContext::new(&args)?;

    info!("Temp directory: {}", ctx.temp_dir.display());

    info!("");
    info!("=== STEP 1/3: Extracting arXiv references ===");
    info!("");

    let extract_args = ExtractArgs {
        input: args.input.clone(),
        output: ctx.extract_output.to_string_lossy().to_string(),
        log_level: "OFF".to_string(), // Disable logging for sub-steps (we already set up logging)
        threads: args.threads,
        batch_size: args.batch_size,
        stats_interval: 60,
    };

    // Re-setup logging after extract (it may have reset it)
    let extract_stats = extract::run_extract(extract_args)
        .context("Extract step failed")?;

    setup_logging(&args.log_level)?;
    info!("Extract complete: {} DOIs with arXiv references", extract_stats.arxiv_matches);

    info!("");
    info!("=== STEP 2/3: Inverting references ===");
    info!("");

    let invert_args = InvertArgs {
        input: ctx.extract_output.to_string_lossy().to_string(),
        output: ctx.invert_output.to_string_lossy().to_string(),
        log_level: "OFF".to_string(),
    };

    let invert_stats = invert::run_invert(invert_args)
        .context("Invert step failed")?;

    setup_logging(&args.log_level)?;
    info!("Invert complete: {} unique arXiv works", invert_stats.unique_arxiv_works);

    info!("");
    info!("=== STEP 3/3: Validating citations ===");
    info!("");

    let validate_args = ValidateArgs {
        input: ctx.invert_output.to_string_lossy().to_string(),
        records: args.records.clone(),
        output_valid: args.output.clone(),
        output_failed: args.output_failed.clone().unwrap_or_else(|| {
            // If no failed output specified, use a temp file that we'll delete
            ctx.temp_dir.join("failed_temp.jsonl").to_string_lossy().to_string()
        }),
        concurrency: args.concurrency,
        timeout: args.timeout,
        log_level: "OFF".to_string(),
    };

    let rt = tokio::runtime::Runtime::new()?;
    let validate_stats = rt.block_on(validate::run_validate_async(validate_args))
        .context("Validate step failed")?;

    setup_logging(&args.log_level)?;
    info!("Validate complete: {} valid, {} failed", validate_stats.total_valid, validate_stats.total_failed);

    if args.output_failed.is_none() {
        let temp_failed = ctx.temp_dir.join("failed_temp.jsonl");
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
    info!("Extract step:");
    info!("  JSON files processed: {}", extract_stats.json_files_processed);
    info!("  Total records scanned: {}", extract_stats.total_records);
    info!("  DOIs with arXiv references: {}", extract_stats.arxiv_matches);
    info!("");
    info!("Invert step:");
    info!("  Records processed: {}", invert_stats.records_processed);
    if invert_stats.records_failed > 0 {
        info!("  Records failed to parse: {}", invert_stats.records_failed);
    }
    info!("  Unique arXiv works: {}", invert_stats.unique_arxiv_works);
    info!("  Total references: {}", invert_stats.total_references);
    info!("  Unique citing works: {}", invert_stats.unique_citing_works);
    info!("");
    info!("Validate step:");
    info!("  Matched in DataCite: {}", validate_stats.matched_in_datacite);
    info!("  Resolution resolved: {}", validate_stats.resolution_resolved);
    info!("  Resolution failed: {}", validate_stats.resolution_failed);
    info!("  Total valid: {}", validate_stats.total_valid);
    info!("  Total failed: {}", validate_stats.total_failed);
    info!("");
    info!("Output: {}", args.output);
    if let Some(ref failed) = args.output_failed {
        info!("Output failed: {}", failed);
    }
    info!("===========================================================");

    Ok((extract_stats, invert_stats, validate_stats))
}
