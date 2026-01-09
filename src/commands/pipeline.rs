use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::{debug, info, warn};
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use tar::Archive;
use uuid::Uuid;

use crate::cli::{PipelineArgs, Source};
use crate::common::setup_logging;
use crate::extract::{extract_arxiv_matches_from_text, extract_doi_matches_from_text};
use crate::index::{
    build_index_from_jsonl_gz, load_index_from_parquet, save_index_to_parquet, DoiIndex,
};
use crate::streaming::{invert_partitions, Checkpoint, OutputMode, PartitionWriter};
use crate::validation::{
    validate_citations, write_arxiv_validation_results, write_split_validation_results,
    write_validation_results,
};

/// Progress logging interval (every N files)
const PROGRESS_LOG_INTERVAL: usize = 100;
/// Divisor for computing flush threshold from batch size
const FLUSH_THRESHOLD_DIVISOR: usize = 100;

/// Check if a citation should be included (filters out self-citations)
fn should_include_citation(citing_doi: &str, cited_id: &str) -> bool {
    // Remove self-citations
    citing_doi.to_lowercase() != cited_id.to_lowercase()
}

struct PipelineIndexes {
    crossref: Option<DoiIndex>,
    datacite: Option<DoiIndex>,
}

/// Statistics from the extraction phase
#[derive(Debug, Clone, Default)]
pub struct ExtractionStats {
    pub files_processed: usize,
    pub items_processed: usize,
    pub refs_with_matches: usize,
    pub total_matches: usize,
    pub crossref_dois_indexed: usize,
}

fn load_indexes(args: &PipelineArgs) -> Result<PipelineIndexes> {
    let mut indexes = PipelineIndexes {
        crossref: None,
        datacite: None,
    };

    // Load or defer Crossref index (built during streaming)
    if let Some(ref path) = args.load_crossref_index {
        info!("Loading Crossref index from: {}", path);
        indexes.crossref = Some(load_index_from_parquet(path)?);
    }

    // Load or build DataCite index
    if let Some(ref path) = args.load_datacite_index {
        info!("Loading DataCite index from: {}", path);
        indexes.datacite = Some(load_index_from_parquet(path)?);
    } else if let Some(ref path) = args.datacite_records {
        info!("Building DataCite index from: {}", path);
        indexes.datacite = Some(build_index_from_jsonl_gz(path, "id")?);
    }

    Ok(indexes)
}

/// Determine if we should build the Crossref index during extraction
fn should_build_crossref_index(args: &PipelineArgs) -> bool {
    // Build the index if:
    // 1. We're extracting DOIs (not arxiv mode) AND
    // 2. We don't already have a loaded index AND
    // 3. We need the index for validation (crossref or all mode)
    args.load_crossref_index.is_none() && matches!(args.source, Source::All | Source::Crossref)
}

/// Run the extraction phase: stream through tar.gz, extract references, build Crossref index
fn run_extraction(
    args: &PipelineArgs,
    indexes: &mut PipelineIndexes,
    partition_dir: &Path,
) -> Result<ExtractionStats> {
    let mut stats = ExtractionStats::default();
    let build_crossref_index = should_build_crossref_index(args);

    // Initialize Crossref index if we're building it
    if build_crossref_index && indexes.crossref.is_none() {
        info!("Will build Crossref index during extraction");
        indexes.crossref = Some(DoiIndex::new());
    }

    // Create partition writer
    let flush_threshold = args.batch_size / FLUSH_THRESHOLD_DIVISOR;
    let mut writer = PartitionWriter::new(partition_dir, flush_threshold.max(10000))?;

    // Open and stream the tar.gz
    let file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);

    info!("Streaming through Crossref archive...");

    for entry_result in archive.entries()? {
        let entry = entry_result.context("Failed to read tar entry")?;
        let path = entry.path()?.to_path_buf();

        // Skip non-JSON files
        let path_str = path.to_string_lossy();
        if !path_str.ends_with(".json") {
            continue;
        }

        debug!("Processing: {}", path_str);

        // Read and parse JSON
        let reader = BufReader::new(entry);
        let json: Value = match serde_json::from_reader(reader) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to parse JSON in {}: {}", path_str, e);
                continue;
            }
        };

        // Process items array
        if let Some(items) = json.get("items").and_then(|v| v.as_array()) {
            for item in items {
                stats.items_processed += 1;

                // Extract the work's DOI
                let work_doi = match item.get("DOI").and_then(|v| v.as_str()) {
                    Some(doi) => doi.to_lowercase(),
                    None => continue, // Skip items without DOI
                };

                // Add to Crossref index if building
                if build_crossref_index {
                    if let Some(ref mut index) = indexes.crossref {
                        index.insert(&work_doi);
                        stats.crossref_dois_indexed += 1;
                    }
                }

                // Process references
                if let Some(references) = item.get("reference").and_then(|v| v.as_array()) {
                    for (ref_idx, reference) in references.iter().enumerate() {
                        let ref_json = reference.to_string();

                        // Collect text to search for matches
                        let mut search_text = String::new();

                        // Include the DOI field if present
                        if let Some(doi) = reference.get("DOI").and_then(|v| v.as_str()) {
                            search_text.push_str(doi);
                            search_text.push(' ');
                        }

                        // Include unstructured text if present
                        if let Some(unstructured) =
                            reference.get("unstructured").and_then(|v| v.as_str())
                        {
                            search_text.push_str(unstructured);
                        }

                        if search_text.is_empty() {
                            continue;
                        }

                        // Extract matches based on source mode
                        let (raw_matches, cited_ids): (Vec<String>, Vec<String>) = match args.source
                        {
                            Source::Arxiv => {
                                // Extract arXiv IDs
                                let matches = extract_arxiv_matches_from_text(&search_text);
                                let raws: Vec<String> =
                                    matches.iter().map(|m| m.raw.clone()).collect();
                                let ids: Vec<String> =
                                    matches.iter().map(|m| m.arxiv_doi.clone()).collect();
                                (raws, ids)
                            }
                            Source::All | Source::Crossref | Source::Datacite => {
                                // Extract DOIs
                                let matches = extract_doi_matches_from_text(&search_text);
                                let raws: Vec<String> =
                                    matches.iter().map(|m| m.raw.clone()).collect();
                                let ids: Vec<String> =
                                    matches.iter().map(|m| m.doi.clone()).collect();
                                (raws, ids)
                            }
                        };

                        if !cited_ids.is_empty() {
                            // Filter out self-citations
                            let (filtered_raw_matches, filtered_cited_ids): (Vec<_>, Vec<_>) =
                                raw_matches
                                    .iter()
                                    .zip(cited_ids.iter())
                                    .filter(|(_, cited_id)| {
                                        should_include_citation(&work_doi, cited_id)
                                    })
                                    .map(|(raw, cited)| (raw.clone(), cited.clone()))
                                    .unzip();

                            if !filtered_cited_ids.is_empty() {
                                stats.refs_with_matches += 1;
                                stats.total_matches += filtered_cited_ids.len();

                                writer.write_extracted_ref(
                                    &work_doi,
                                    ref_idx as u32,
                                    &ref_json,
                                    &filtered_raw_matches,
                                    &filtered_cited_ids,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        stats.files_processed += 1;

        // Log progress periodically
        if stats.files_processed % PROGRESS_LOG_INTERVAL == 0 {
            info!(
                "Progress: {} files, {} items, {} matches",
                stats.files_processed, stats.items_processed, stats.total_matches
            );
        }
    }

    // Flush remaining data
    writer.flush_all()?;

    info!("Extraction complete:");
    info!("  Files processed: {}", stats.files_processed);
    info!("  Items processed: {}", stats.items_processed);
    info!("  References with matches: {}", stats.refs_with_matches);
    info!("  Total matches: {}", stats.total_matches);
    if build_crossref_index {
        info!("  Crossref DOIs indexed: {}", stats.crossref_dois_indexed);
    }

    Ok(stats)
}

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // Phase 1: Load indexes
    info!("");
    info!("=== Loading Indexes ===");
    let mut indexes = load_indexes(&args)?;

    // Set up partition directory
    let partition_dir = if let Some(ref dir) = args.temp_dir {
        let path = PathBuf::from(dir);
        std::fs::create_dir_all(&path)?;
        path
    } else {
        // Create a unique temp directory
        let temp_base = std::env::temp_dir();
        let unique_dir = temp_base.join(format!("crossref-extract-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&unique_dir).context("Failed to create temp directory")?;
        unique_dir
    };
    let cleanup_temp = args.temp_dir.is_none() && !args.keep_intermediates;
    info!("Partition directory: {}", partition_dir.display());

    // Phase 2: Extract and build Crossref index
    info!("");
    info!("=== Extraction Phase ===");
    let extraction_stats = run_extraction(&args, &mut indexes, &partition_dir)?;

    if extraction_stats.total_matches == 0 {
        warn!("No matches found during extraction");
    }

    // Phase 3: Invert partitions
    info!("");
    info!("=== Aggregating Citations ===");

    let output_mode = match args.source {
        Source::Arxiv => OutputMode::Arxiv,
        _ => OutputMode::Generic,
    };

    // Determine output paths
    let output_parquet = partition_dir.join("inverted.parquet");
    let output_jsonl = match args.source {
        Source::Arxiv => args.output_arxiv.as_ref().map(PathBuf::from),
        Source::Crossref => args.output_crossref.as_ref().map(PathBuf::from),
        Source::Datacite => args.output_datacite.as_ref().map(PathBuf::from),
        Source::All => None, // Will handle separately in validation phase
    };

    let mut checkpoint = Checkpoint::new(&format!("pipeline-{}", Uuid::new_v4()));

    let invert_stats = invert_partitions(
        &partition_dir,
        &output_parquet,
        output_jsonl.as_deref(),
        &mut checkpoint,
        output_mode,
    )?;

    info!("Aggregation complete:");
    info!(
        "  Partitions processed: {}",
        invert_stats.partitions_processed
    );
    info!("  Unique cited works: {}", invert_stats.unique_cited_works);
    info!("  Total citations: {}", invert_stats.total_citations);

    // Phase 4: Validate
    info!("");
    info!("=== Validating Citations ===");

    let http_fallback_enabled = args
        .http_fallback
        .iter()
        .any(|s| s == "crossref" || s == "datacite" || s == "all");

    // Only run validation if we have an index to validate against and JSONL output
    if indexes.crossref.is_some() || indexes.datacite.is_some() {
        if let Some(ref jsonl_path) = output_jsonl {
            let validation_input = jsonl_path.to_string_lossy().to_string();

            let rt = tokio::runtime::Runtime::new()?;
            let validation_results = rt.block_on(validate_citations(
                &validation_input,
                indexes.crossref.as_ref(),
                indexes.datacite.as_ref(),
                args.source,
                http_fallback_enabled,
                args.concurrency,
                args.timeout,
            ))?;

            info!("Validation results:");
            info!(
                "  Total records: {}",
                validation_results.stats.total_records
            );
            info!(
                "  Crossref matched: {}",
                validation_results.stats.crossref_matched
            );
            info!(
                "  DataCite matched: {}",
                validation_results.stats.datacite_matched
            );
            if http_fallback_enabled {
                info!(
                    "  HTTP resolved: {} crossref, {} datacite",
                    validation_results.stats.crossref_http_resolved,
                    validation_results.stats.datacite_http_resolved
                );
            }
            info!("  Total valid: {}", validation_results.valid.len());
            info!("  Total failed: {}", validation_results.failed.len());

            // Write outputs based on source mode
            match args.source {
                Source::All => {
                    let (crossref_written, datacite_written) = write_split_validation_results(
                        &validation_results,
                        args.output_crossref.as_deref(),
                        args.output_datacite.as_deref(),
                        args.output_crossref_failed.as_deref(),
                        args.output_datacite_failed.as_deref(),
                    )?;
                    info!(
                        "Output written: {} Crossref, {} DataCite",
                        crossref_written, datacite_written
                    );
                }
                Source::Crossref => {
                    write_validation_results(
                        &validation_results,
                        args.output_crossref.as_ref().unwrap(),
                        args.output_crossref_failed.as_deref(),
                    )?;
                }
                Source::Datacite => {
                    write_validation_results(
                        &validation_results,
                        args.output_datacite.as_ref().unwrap(),
                        args.output_datacite_failed.as_deref(),
                    )?;
                }
                Source::Arxiv => {
                    write_arxiv_validation_results(
                        &validation_results,
                        args.output_arxiv.as_ref().unwrap(),
                        args.output_arxiv_failed.as_deref(),
                    )?;
                }
            }
        } else {
            info!("No JSONL output specified, skipping validation...");
        }
    } else {
        info!("No indexes available for validation, skipping...");
    }

    // Save indexes if requested
    if let Some(ref path) = args.save_crossref_index {
        if let Some(ref index) = indexes.crossref {
            save_index_to_parquet(index, path)?;
        }
    }
    if let Some(ref path) = args.save_datacite_index {
        if let Some(ref index) = indexes.datacite {
            save_index_to_parquet(index, path)?;
        }
    }

    // Cleanup temp directory if needed
    if cleanup_temp {
        info!("Cleaning up temp directory: {}", partition_dir.display());
        if let Err(e) = std::fs::remove_dir_all(&partition_dir) {
            warn!("Failed to cleanup temp directory: {}", e);
        }
    }

    Ok(())
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    match args.source {
        Source::All => {
            if args.output_crossref.is_none() || args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'all' requires both --output-crossref and --output-datacite"
                ));
            }
        }
        Source::Crossref => {
            if args.output_crossref.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'crossref' requires --output-crossref"
                ));
            }
        }
        Source::Datacite => {
            if args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --output-datacite"
                ));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
        Source::Arxiv => {
            if args.output_arxiv.is_none() {
                return Err(anyhow::anyhow!("Source 'arxiv' requires --output-arxiv"));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PipelineArgs;

    fn default_args() -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            datacite_records: None,
            source: Source::All,
            output_crossref: None,
            output_datacite: None,
            output_arxiv: None,
            output_crossref_failed: None,
            output_datacite_failed: None,
            output_arxiv_failed: None,
            http_fallback: vec![],
            load_crossref_index: None,
            save_crossref_index: None,
            load_datacite_index: None,
            save_datacite_index: None,
            log_level: "INFO".to_string(),
            concurrency: 50,
            timeout: 5,
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
        }
    }

    #[test]
    fn test_validate_args_all_requires_both_outputs() {
        let args = default_args();
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--output-crossref"));
    }

    #[test]
    fn test_validate_args_all_with_both_outputs() {
        let mut args = default_args();
        args.output_crossref = Some("crossref.jsonl".to_string());
        args.output_datacite = Some("datacite.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_crossref_requires_output() {
        let mut args = default_args();
        args.source = Source::Crossref;
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--output-crossref"));
    }

    #[test]
    fn test_validate_args_crossref_with_output() {
        let mut args = default_args();
        args.source = Source::Crossref;
        args.output_crossref = Some("crossref.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_datacite_requires_output() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--output-datacite"));
    }

    #[test]
    fn test_validate_args_datacite_requires_records() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--datacite-records"));
    }

    #[test]
    fn test_validate_args_datacite_with_records_file() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_datacite_with_index() {
        let mut args = default_args();
        args.source = Source::Datacite;
        args.output_datacite = Some("datacite.jsonl".to_string());
        args.load_datacite_index = Some("index.parquet".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_arxiv_requires_output() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--output-arxiv"));
    }

    #[test]
    fn test_validate_args_arxiv_requires_records() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.output_arxiv = Some("arxiv.jsonl".to_string());
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--datacite-records"));
    }

    #[test]
    fn test_validate_args_arxiv_with_all_required() {
        let mut args = default_args();
        args.source = Source::Arxiv;
        args.output_arxiv = Some("arxiv.jsonl".to_string());
        args.datacite_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_citation() {
        assert!(should_include_citation("10.1234/a", "10.5678/b"));
        assert!(!should_include_citation("10.1234/a", "10.1234/a"));
        assert!(!should_include_citation("10.1234/A", "10.1234/a")); // Case insensitive
    }
}
