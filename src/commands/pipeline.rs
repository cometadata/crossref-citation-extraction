use anyhow::{Context, Result};
use log::info;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

use crate::cli::{ConvertArgs, ExtractArgs, InvertArgs, PipelineArgs, ValidateArgs};
use crate::commands::{convert, extract, invert, validate};
use crate::common::{format_elapsed, setup_logging, ConvertStats, ExtractStats, InvertStats, ValidateStats};

struct PipelineContext {
    temp_dir: PathBuf,
    convert_output: PathBuf,
    extract_output: PathBuf,
    invert_output: PathBuf,
    invert_jsonl_output: PathBuf,
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

        let convert_output = temp_dir.join(format!("references_{}.parquet", run_id));
        let extract_output = temp_dir.join(format!("extracted_{}.parquet", run_id));
        let invert_output = temp_dir.join(format!("inverted_{}.parquet", run_id));
        let invert_jsonl_output = temp_dir.join(format!("inverted_{}.jsonl", run_id));

        Ok(Self {
            temp_dir,
            convert_output,
            extract_output,
            invert_output,
            invert_jsonl_output,
            keep_intermediates: args.keep_intermediates,
        })
    }

    fn cleanup(&self) -> Result<()> {
        if self.keep_intermediates {
            info!("Keeping intermediate files:");
            info!("  Convert output: {}", self.convert_output.display());
            info!("  Extract output: {}", self.extract_output.display());
            info!("  Invert output: {}", self.invert_output.display());
            info!("  Invert JSONL: {}", self.invert_jsonl_output.display());
            return Ok(());
        }

        info!("Cleaning up intermediate files...");

        for path in [&self.convert_output, &self.extract_output, &self.invert_output, &self.invert_jsonl_output] {
            if path.exists() {
                fs::remove_file(path)
                    .with_context(|| format!("Failed to remove: {}", path.display()))?;
            }
        }

        Ok(())
    }
}

impl Drop for PipelineContext {
    fn drop(&mut self) {
        // Best-effort cleanup on drop (e.g., if pipeline panics)
        if !self.keep_intermediates {
            let _ = fs::remove_file(&self.convert_output);
            let _ = fs::remove_file(&self.extract_output);
            let _ = fs::remove_file(&self.invert_output);
            let _ = fs::remove_file(&self.invert_jsonl_output);
        }
    }
}

pub fn run_pipeline(args: PipelineArgs) -> Result<(ConvertStats, ExtractStats, InvertStats, ValidateStats)> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation pipeline (Polars)");
    info!("Input: {}", args.input);
    info!("DataCite records: {}", args.records);
    info!("Output: {}", args.output);
    if let Some(ref failed) = args.output_failed {
        info!("Output failed: {}", failed);
    }

    let ctx = PipelineContext::new(&args)?;

    info!("Temp directory: {}", ctx.temp_dir.display());

    // STEP 1: Convert tar.gz to Parquet
    info!("");
    info!("=== STEP 1/4: Converting tar.gz to Parquet ===");
    info!("");

    let convert_args = ConvertArgs {
        input: args.input.clone(),
        output: ctx.convert_output.to_string_lossy().to_string(),
        row_group_size: 250_000,
        log_level: "OFF".to_string(),
    };

    let convert_stats = convert::run_convert(convert_args)
        .context("Convert step failed")?;

    setup_logging(&args.log_level)?;
    info!("Convert complete: {} references with arXiv hints", convert_stats.references_with_hint);

    // STEP 2: Extract arXiv references from Parquet
    info!("");
    info!("=== STEP 2/4: Extracting arXiv references ===");
    info!("");

    let extract_args = ExtractArgs {
        input: ctx.convert_output.to_string_lossy().to_string(),
        output: ctx.extract_output.to_string_lossy().to_string(),
        log_level: "OFF".to_string(),
    };

    let extract_stats = extract::run_extract(extract_args)
        .context("Extract step failed")?;

    setup_logging(&args.log_level)?;
    info!("Extract complete: {} references with arXiv matches", extract_stats.references_with_matches);

    // STEP 3: Invert references
    info!("");
    info!("=== STEP 3/4: Inverting references ===");
    info!("");

    let invert_args = InvertArgs {
        input: ctx.extract_output.to_string_lossy().to_string(),
        output: ctx.invert_output.to_string_lossy().to_string(),
        log_level: "OFF".to_string(),
        output_jsonl: Some(ctx.invert_jsonl_output.to_string_lossy().to_string()),
    };

    let invert_stats = invert::run_invert(invert_args)
        .context("Invert step failed")?;

    setup_logging(&args.log_level)?;
    info!("Invert complete: {} unique arXiv works", invert_stats.unique_arxiv_works);

    // STEP 4: Validate citations
    info!("");
    info!("=== STEP 4/4: Validating citations ===");
    info!("");

    let validate_args = ValidateArgs {
        input: ctx.invert_jsonl_output.to_string_lossy().to_string(),
        records: args.records.clone(),
        output_valid: args.output.clone(),
        output_failed: args.output_failed.clone().unwrap_or_else(|| {
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
    info!("Convert step:");
    info!("  JSON files processed: {}", convert_stats.json_files_processed);
    info!("  Total records scanned: {}", convert_stats.total_records);
    info!("  Total references: {}", convert_stats.total_references);
    info!("  References with arXiv hint: {}", convert_stats.references_with_hint);
    info!("");
    info!("Extract step:");
    info!("  Total references scanned: {}", extract_stats.total_references);
    info!("  References with arXiv matches: {}", extract_stats.references_with_matches);
    info!("  Total arXiv IDs extracted: {}", extract_stats.total_arxiv_ids);
    info!("");
    info!("Invert step:");
    info!("  Total rows processed: {}", invert_stats.total_rows_processed);
    info!("  Unique arXiv works: {}", invert_stats.unique_arxiv_works);
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

    Ok((convert_stats, extract_stats, invert_stats, validate_stats))
}
