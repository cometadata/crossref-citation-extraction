use anyhow::Result;
use log::info;
use std::path::Path;

use crate::cli::{Source, ValidateArgs};
use crate::common::setup_logging;
use crate::index::{build_index_from_jsonl_gz, load_index_from_parquet, DoiIndex};
use crate::validation::{
    validate_citations, write_arxiv_validation_results_with_split,
    write_validation_results_with_split,
};

pub fn run_validate(args: ValidateArgs) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_validate_async(args))
}

pub async fn run_validate_async(args: ValidateArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting standalone validation");
    info!("Input: {}", args.input);
    info!("Source: {}", args.source);

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // Load indexes based on source
    let crossref_index: Option<DoiIndex> = if let Some(ref path) = args.crossref_index {
        info!("Loading Crossref index from: {}", path);
        Some(load_index_from_parquet(path)?)
    } else {
        None
    };

    let datacite_index: Option<DoiIndex> = if let Some(ref path) = args.datacite_records {
        info!("Building DataCite index from: {}", path);
        Some(build_index_from_jsonl_gz(path, "id")?)
    } else {
        None
    };

    // Validate required indexes
    match args.source {
        Source::Crossref => {
            if crossref_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "Crossref validation requires --crossref-index or --http-fallback"
                ));
            }
        }
        Source::Datacite | Source::Arxiv => {
            if datacite_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "DataCite/arXiv validation requires --datacite-records or --http-fallback"
                ));
            }
        }
        Source::All => {
            if crossref_index.is_none() && datacite_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "Validation requires at least one index or --http-fallback"
                ));
            }
        }
    }

    // Run validation
    let results = validate_citations(
        &args.input,
        crossref_index.as_ref(),
        datacite_index.as_ref(),
        args.source,
        args.http_fallback,
        args.concurrency,
        args.timeout,
    )
    .await?;

    // Write results with provenance split
    match args.source {
        Source::Arxiv => {
            write_arxiv_validation_results_with_split(
                &results,
                &args.output_valid,
                Some(&args.output_failed),
            )?;
        }
        _ => {
            write_validation_results_with_split(
                &results.valid,
                &results.failed,
                &args.output_valid,
                Some(&args.output_failed),
            )?;
        }
    }

    info!("==================== VALIDATION COMPLETE ====================");
    info!("Total records: {}", results.stats.total_records);
    info!("Valid: {}", results.valid.len());
    info!("Failed: {}", results.failed.len());
    info!("Output valid: {}", args.output_valid);
    info!("Output failed: {}", args.output_failed);
    info!("=============================================================");

    Ok(())
}
