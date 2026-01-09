use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use log::info;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::cli::Source;
use crate::common::{format_elapsed, CitationRecord, MultiValidateStats};
use crate::index::DoiIndex;

use super::{check_doi_resolves, create_doi_client, lookup_doi, LookupResult};

/// Multiplier for buffer_unordered capacity relative to concurrency
const BUFFER_CAPACITY_MULTIPLIER: usize = 2;

/// Results from validation
pub struct ValidationResults {
    pub valid: Vec<(CitationRecord, Source)>,
    pub failed: Vec<CitationRecord>,
    pub stats: MultiValidateStats,
}

/// Validate citations from a JSONL file against indexes
pub async fn validate_citations(
    input_path: &str,
    crossref_index: Option<&DoiIndex>,
    datacite_index: Option<&DoiIndex>,
    source: Source,
    http_fallback: bool,
    concurrency: usize,
    timeout_secs: u64,
) -> Result<ValidationResults> {
    let start = Instant::now();
    info!("Validating citations from: {}", input_path);

    let file = File::open(input_path).with_context(|| format!("Failed to open: {}", input_path))?;
    let reader = BufReader::new(file);

    let mut matched: Vec<(CitationRecord, Source)> = Vec::new();
    let mut unmatched: Vec<CitationRecord> = Vec::new();
    let mut stats = MultiValidateStats::default();

    // Phase 1: Index lookup
    for line_result in reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }

        let record: CitationRecord = serde_json::from_str(&line).with_context(|| {
            format!("Failed to parse record at line {}", stats.total_records + 1)
        })?;
        stats.total_records += 1;

        match lookup_doi(&record.doi, source, crossref_index, datacite_index) {
            LookupResult::Found(found_source) => {
                match found_source {
                    Source::Crossref => stats.crossref_matched += 1,
                    Source::Datacite => stats.datacite_matched += 1,
                    _ => {}
                }
                matched.push((record, found_source));
            }
            LookupResult::NotFound => {
                unmatched.push(record);
            }
        }
    }

    info!(
        "Index lookup: {} matched, {} unmatched",
        matched.len(),
        unmatched.len()
    );

    // Phase 2: HTTP fallback for unmatched (if enabled)
    let mut http_resolved: Vec<(CitationRecord, Source)> = Vec::new();
    let mut failed: Vec<CitationRecord> = Vec::new();

    if http_fallback && !unmatched.is_empty() {
        info!(
            "Running HTTP fallback for {} unmatched DOIs...",
            unmatched.len()
        );

        let client = create_doi_client()?;
        let timeout = Duration::from_secs(timeout_secs);
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let results: Vec<(CitationRecord, bool)> = stream::iter(unmatched.into_iter())
            .map(|record| {
                let client = client.clone();
                let semaphore = semaphore.clone();

                async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .expect("semaphore should never be closed");
                    let resolves = check_doi_resolves(&client, &record.doi, timeout).await;
                    (record, resolves)
                }
            })
            .buffer_unordered(concurrency * BUFFER_CAPACITY_MULTIPLIER)
            .collect()
            .await;

        for (record, resolves) in results {
            if resolves {
                // Determine source based on prefix for stats
                match source {
                    Source::Crossref => stats.crossref_http_resolved += 1,
                    Source::Datacite | Source::Arxiv => stats.datacite_http_resolved += 1,
                    Source::All => stats.datacite_http_resolved += 1, // Default to datacite for all
                }
                http_resolved.push((record, source));
            } else {
                match source {
                    Source::Crossref => stats.crossref_failed += 1,
                    _ => stats.datacite_failed += 1,
                }
                failed.push(record);
            }
        }
    } else {
        // All unmatched go to failed
        for record in unmatched {
            match source {
                Source::Crossref => stats.crossref_failed += 1,
                _ => stats.datacite_failed += 1,
            }
            failed.push(record);
        }
    }

    // Combine matched and http_resolved
    matched.extend(http_resolved);

    info!("Validation complete in {}", format_elapsed(start.elapsed()));

    Ok(ValidationResults {
        valid: matched,
        failed,
        stats,
    })
}

/// Write validation results to output files
pub fn write_validation_results(
    results: &ValidationResults,
    output_valid: &str,
    output_failed: Option<&str>,
) -> Result<()> {
    info!(
        "Writing {} valid records to: {}",
        results.valid.len(),
        output_valid
    );

    let file = File::create(output_valid)?;
    let mut writer = BufWriter::new(file);

    for (record, _source) in &results.valid {
        writeln!(writer, "{}", serde_json::to_string(record)?)?;
    }
    writer.flush()?;

    if let Some(failed_path) = output_failed {
        info!(
            "Writing {} failed records to: {}",
            results.failed.len(),
            failed_path
        );
        let file = File::create(failed_path)?;
        let mut writer = BufWriter::new(file);

        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_record(doi: &str) -> CitationRecord {
        CitationRecord {
            doi: doi.to_string(),
            reference_count: 0,
            citation_count: 1,
            cited_by: vec![json!({"doi": "10.1234/citing"})],
        }
    }

    fn create_test_jsonl(records: &[CitationRecord]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        for record in records {
            writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[tokio::test]
    async fn test_validate_citations_with_crossref_index() {
        let mut crossref_index = DoiIndex::new();
        crossref_index.insert("10.1234/found");

        let records = vec![
            create_test_record("10.1234/found"),
            create_test_record("10.1234/notfound"),
        ];
        let input_file = create_test_jsonl(&records);

        let results = validate_citations(
            input_file.path().to_str().unwrap(),
            Some(&crossref_index),
            None,
            Source::Crossref,
            false,
            10,
            5,
        )
        .await
        .unwrap();

        assert_eq!(results.stats.total_records, 2);
        assert_eq!(results.stats.crossref_matched, 1);
        assert_eq!(results.valid.len(), 1);
        assert_eq!(results.failed.len(), 1);
        assert_eq!(results.valid[0].0.doi, "10.1234/found");
        assert_eq!(results.failed[0].doi, "10.1234/notfound");
    }

    #[tokio::test]
    async fn test_validate_citations_with_datacite_index() {
        let mut datacite_index = DoiIndex::new();
        datacite_index.insert("10.48550/arxiv.2301.00001");

        let records = vec![
            create_test_record("10.48550/arXiv.2301.00001"),
            create_test_record("10.48550/arxiv.9999.99999"),
        ];
        let input_file = create_test_jsonl(&records);

        let results = validate_citations(
            input_file.path().to_str().unwrap(),
            None,
            Some(&datacite_index),
            Source::Datacite,
            false,
            10,
            5,
        )
        .await
        .unwrap();

        assert_eq!(results.stats.total_records, 2);
        assert_eq!(results.stats.datacite_matched, 1);
        assert_eq!(results.valid.len(), 1);
        assert_eq!(results.failed.len(), 1);
    }

    #[tokio::test]
    async fn test_validate_citations_all_mode() {
        let mut crossref_index = DoiIndex::new();
        crossref_index.insert("10.1234/crossref");

        let mut datacite_index = DoiIndex::new();
        datacite_index.insert("10.48550/arxiv.2301.00001");

        let records = vec![
            create_test_record("10.1234/crossref"),
            create_test_record("10.48550/arXiv.2301.00001"),
            create_test_record("10.9999/unknown"),
        ];
        let input_file = create_test_jsonl(&records);

        let results = validate_citations(
            input_file.path().to_str().unwrap(),
            Some(&crossref_index),
            Some(&datacite_index),
            Source::All,
            false,
            10,
            5,
        )
        .await
        .unwrap();

        assert_eq!(results.stats.total_records, 3);
        assert_eq!(results.stats.crossref_matched, 1);
        assert_eq!(results.stats.datacite_matched, 1);
        assert_eq!(results.valid.len(), 2);
        assert_eq!(results.failed.len(), 1);
    }

    #[test]
    fn test_write_validation_results() {
        let results = ValidationResults {
            valid: vec![(create_test_record("10.1234/valid"), Source::Crossref)],
            failed: vec![create_test_record("10.1234/failed")],
            stats: MultiValidateStats::default(),
        };

        let valid_file = NamedTempFile::new().unwrap();
        let failed_file = NamedTempFile::new().unwrap();

        write_validation_results(
            &results,
            valid_file.path().to_str().unwrap(),
            Some(failed_file.path().to_str().unwrap()),
        )
        .unwrap();

        // Verify valid file
        let valid_content = std::fs::read_to_string(valid_file.path()).unwrap();
        assert!(valid_content.contains("10.1234/valid"));

        // Verify failed file
        let failed_content = std::fs::read_to_string(failed_file.path()).unwrap();
        assert!(failed_content.contains("10.1234/failed"));
    }

    #[tokio::test]
    async fn test_validate_citations_empty_file() {
        let input_file = NamedTempFile::new().unwrap();

        let results = validate_citations(
            input_file.path().to_str().unwrap(),
            None,
            None,
            Source::All,
            false,
            10,
            5,
        )
        .await
        .unwrap();

        assert_eq!(results.stats.total_records, 0);
        assert_eq!(results.valid.len(), 0);
        assert_eq!(results.failed.len(), 0);
    }
}
