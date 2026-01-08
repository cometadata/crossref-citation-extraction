use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::{debug, info, warn};
use polars::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Instant;
use tar::Archive;

use crate::cli::ConvertArgs;
use crate::common::{create_spinner, format_elapsed, setup_logging, ConvertStats};

pub fn run_convert(args: ConvertArgs) -> Result<ConvertStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Crossref → Parquet Conversion");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    let mut citing_dois: Vec<String> = Vec::new();
    let mut ref_indices: Vec<u32> = Vec::new();
    let mut ref_dois: Vec<Option<String>> = Vec::new();
    let mut ref_unstructureds: Vec<Option<String>> = Vec::new();
    let mut ref_article_titles: Vec<Option<String>> = Vec::new();
    let mut ref_journal_titles: Vec<Option<String>> = Vec::new();
    let mut ref_urls: Vec<Option<String>> = Vec::new();
    let mut ref_jsons: Vec<Option<String>> = Vec::new();
    let mut has_arxiv_hints: Vec<bool> = Vec::new();

    let mut json_files_processed: usize = 0;
    let mut total_records: usize = 0;
    let mut total_references: usize = 0;
    let mut references_with_hint: usize = 0;

    info!("Opening tar.gz archive...");
    let tar_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;

    let gz_decoder = GzDecoder::new(tar_file);
    let mut archive = Archive::new(gz_decoder);

    let progress = create_spinner("Processing archive entries...");

    for entry_result in archive.entries()? {
        let mut entry = entry_result.context("Failed to read tar entry")?;
        let path = entry.path()?.to_path_buf();

        if !path.extension().map_or(false, |ext| ext == "json") {
            continue;
        }

        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        progress.set_message(format!(
            "Processing: {} | {} refs",
            file_name, total_references
        ));

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

        let items = match json_data.get("items") {
            Some(Value::Array(items)) => items,
            _ => {
                warn!("No 'items' array found in {}", path.display());
                continue;
            }
        };

        json_files_processed += 1;

        for record in items {
            total_records += 1;

            let doi = match record.get("DOI").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => continue,
            };

            let references = match record.get("reference") {
                Some(Value::Array(refs)) => refs,
                _ => continue,
            };

            for (idx, reference) in references.iter().enumerate() {
                total_references += 1;

                let ref_doi = reference.get("DOI").and_then(|v| v.as_str()).map(String::from);
                let unstructured = reference
                    .get("unstructured")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let article_title = reference
                    .get("article-title")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let journal_title = reference
                    .get("journal-title")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let url = reference.get("URL").and_then(|v| v.as_str()).map(String::from);
                let ref_json = serde_json::to_string(reference).ok();

                let has_hint = check_arxiv_hint(
                    ref_doi.as_deref(),
                    unstructured.as_deref(),
                    article_title.as_deref(),
                    journal_title.as_deref(),
                    url.as_deref(),
                );

                if has_hint {
                    references_with_hint += 1;
                }

                citing_dois.push(doi.to_string());
                ref_indices.push(idx as u32);
                ref_dois.push(ref_doi);
                ref_unstructureds.push(unstructured);
                ref_article_titles.push(article_title);
                ref_journal_titles.push(journal_title);
                ref_urls.push(url);
                ref_jsons.push(ref_json);
                has_arxiv_hints.push(has_hint);
            }
        }

        debug!(
            "Completed processing: {} ({} items)",
            file_name,
            items.len()
        );
    }

    progress.finish_with_message("Archive processing complete");

    info!(
        "Building DataFrame with {} references...",
        citing_dois.len()
    );

    let df = DataFrame::new(vec![
        Column::new("citing_doi".into(), citing_dois),
        Column::new("ref_index".into(), ref_indices),
        Column::new("ref_doi".into(), ref_dois),
        Column::new("ref_unstructured".into(), ref_unstructureds),
        Column::new("ref_article_title".into(), ref_article_titles),
        Column::new("ref_journal_title".into(), ref_journal_titles),
        Column::new("ref_url".into(), ref_urls),
        Column::new("ref_json".into(), ref_jsons),
        Column::new("has_arxiv_hint".into(), has_arxiv_hints),
    ])?;

    info!("Writing Parquet file...");

    let file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(args.row_group_size))
        .finish(&mut df.clone())?;

    let total_time = start_time.elapsed();

    let stats = ConvertStats {
        json_files_processed,
        total_records,
        total_references,
        references_with_hint,
    };

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("JSON files processed: {}", stats.json_files_processed);
    info!("Total records scanned: {}", stats.total_records);
    info!("Total references flattened: {}", stats.total_references);
    info!(
        "References with arXiv hint: {} ({:.2}%)",
        stats.references_with_hint,
        100.0 * stats.references_with_hint as f64 / stats.total_references.max(1) as f64
    );
    info!("Output file: {}", args.output);
    info!("========================================================");

    Ok(stats)
}

fn check_arxiv_hint(
    doi: Option<&str>,
    unstructured: Option<&str>,
    article_title: Option<&str>,
    journal_title: Option<&str>,
    url: Option<&str>,
) -> bool {
    let check = |s: Option<&str>| s.map(|t| t.to_lowercase().contains("arxiv")).unwrap_or(false);

    let doi_check = doi
        .map(|d| d.to_lowercase().contains("10.48550"))
        .unwrap_or(false);

    check(unstructured) || check(article_title) || check(journal_title) || check(url) || doi_check
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_arxiv_hint_with_arxiv() {
        assert!(check_arxiv_hint(
            None,
            Some("arXiv preprint arXiv:2403.03542"),
            None,
            None,
            None
        ));
    }

    #[test]
    fn test_check_arxiv_hint_with_doi() {
        assert!(check_arxiv_hint(
            Some("10.48550/arXiv.2403.03542"),
            None,
            None,
            None,
            None
        ));
    }

    #[test]
    fn test_check_arxiv_hint_with_url() {
        assert!(check_arxiv_hint(
            None,
            None,
            None,
            None,
            Some("https://arxiv.org/abs/2403.03542")
        ));
    }

    #[test]
    fn test_check_arxiv_hint_without_arxiv() {
        assert!(!check_arxiv_hint(
            Some("10.1234/test"),
            Some("A normal reference"),
            Some("Some paper"),
            Some("Nature"),
            Some("https://nature.com/paper")
        ));
    }
}
