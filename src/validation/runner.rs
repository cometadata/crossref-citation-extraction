use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use log::info;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::cli::Source;
use crate::common::{format_elapsed, CitationRecord, MultiValidateStats, SplitOutputPaths};
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

    let file = File::create(output_valid)
        .with_context(|| format!("Failed to create output file: {}", output_valid))?;
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
        let file = File::create(failed_path)
            .with_context(|| format!("Failed to create output file: {}", failed_path))?;
        let mut writer = BufWriter::new(file);

        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    Ok(())
}

/// Write validation results split by source
pub fn write_split_validation_results(
    results: &ValidationResults,
    output_crossref: Option<&str>,
    output_datacite: Option<&str>,
    output_crossref_failed: Option<&str>,
    output_datacite_failed: Option<&str>,
) -> Result<(usize, usize)> {
    // Split valid results by source
    let (crossref_valid, datacite_valid): (Vec<_>, Vec<_>) = results
        .valid
        .iter()
        .partition(|(_, source)| *source == Source::Crossref);

    let crossref_count = crossref_valid.len();
    let datacite_count = datacite_valid.len();

    // Write Crossref valid
    if let Some(path) = output_crossref {
        info!("Writing {} Crossref citations to: {}", crossref_count, path);
        let file = File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?;
        let mut writer = BufWriter::new(file);
        for (record, _) in &crossref_valid {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    // Write DataCite valid
    if let Some(path) = output_datacite {
        info!("Writing {} DataCite citations to: {}", datacite_count, path);
        let file = File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?;
        let mut writer = BufWriter::new(file);
        for (record, _) in &datacite_valid {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    // Write failed (to both files if provided - we can't know which source they belong to)
    if let Some(path) = output_crossref_failed {
        let file = File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?;
        let mut writer = BufWriter::new(file);
        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    if let Some(path) = output_datacite_failed {
        let file = File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?;
        let mut writer = BufWriter::new(file);
        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    Ok((crossref_count, datacite_count))
}

/// Write arXiv validation results (preserves arXiv format)
pub fn write_arxiv_validation_results(
    results: &ValidationResults,
    output_arxiv: &str,
    output_arxiv_failed: Option<&str>,
) -> Result<()> {
    info!(
        "Writing {} arXiv citations to: {}",
        results.valid.len(),
        output_arxiv
    );

    let file = File::create(output_arxiv)
        .with_context(|| format!("Failed to create output file: {}", output_arxiv))?;
    let mut writer = BufWriter::new(file);

    for (record, _) in &results.valid {
        // Use arxiv_id from record if present, otherwise extract from DOI
        let arxiv_id = record.arxiv_id.as_deref().unwrap_or_else(|| {
            record
                .doi
                .strip_prefix("10.48550/arXiv.")
                .or_else(|| record.doi.strip_prefix("10.48550/arxiv."))
                .unwrap_or(&record.doi)
        });

        let arxiv_record = serde_json::json!({
            "arxiv_doi": record.doi,
            "arxiv_id": arxiv_id,
            "reference_count": record.reference_count,
            "citation_count": record.citation_count,
            "cited_by": record.cited_by
        });

        writeln!(writer, "{}", arxiv_record)?;
    }
    writer.flush()?;

    if let Some(failed_path) = output_arxiv_failed {
        info!(
            "Writing {} failed arXiv citations to: {}",
            results.failed.len(),
            failed_path
        );
        let file = File::create(failed_path)
            .with_context(|| format!("Failed to create output file: {}", failed_path))?;
        let mut writer = BufWriter::new(file);

        for record in &results.failed {
            let arxiv_id = record.arxiv_id.as_deref().unwrap_or_else(|| {
                record
                    .doi
                    .strip_prefix("10.48550/arXiv.")
                    .or_else(|| record.doi.strip_prefix("10.48550/arxiv."))
                    .unwrap_or(&record.doi)
            });

            let arxiv_record = serde_json::json!({
                "arxiv_doi": record.doi,
                "arxiv_id": arxiv_id,
                "reference_count": record.reference_count,
                "citation_count": record.citation_count,
                "cited_by": record.cited_by
            });

            writeln!(writer, "{}", arxiv_record)?;
        }
        writer.flush()?;
    }

    Ok(())
}

/// Filter cited_by entries by provenance
fn filter_cited_by_by_provenance(
    cited_by: &[serde_json::Value],
    keep_asserted: bool,
) -> Vec<serde_json::Value> {
    cited_by
        .iter()
        .filter(|entry| {
            let provenance = entry
                .get("provenance")
                .and_then(|p| p.as_str())
                .unwrap_or("mined");
            let is_asserted = provenance == "publisher" || provenance == "crossref";
            if keep_asserted {
                is_asserted
            } else {
                !is_asserted
            }
        })
        .cloned()
        .collect()
}

/// Write validation results with automatic split by provenance
pub fn write_validation_results_with_split(
    valid: &[(CitationRecord, Source)],
    failed: &[CitationRecord],
    output_path: &str,
    output_failed: Option<&str>,
) -> Result<()> {
    let paths = SplitOutputPaths::from_base(output_path);

    // Open all three output files
    let file_all =
        File::create(&paths.all).with_context(|| format!("Failed to create: {:?}", paths.all))?;
    let file_asserted = File::create(&paths.asserted)
        .with_context(|| format!("Failed to create: {:?}", paths.asserted))?;
    let file_mined = File::create(&paths.mined)
        .with_context(|| format!("Failed to create: {:?}", paths.mined))?;

    let mut writer_all = BufWriter::new(file_all);
    let mut writer_asserted = BufWriter::new(file_asserted);
    let mut writer_mined = BufWriter::new(file_mined);

    for (record, _source) in valid {
        // Write to main file
        writeln!(writer_all, "{}", serde_json::to_string(record)?)?;

        // Filter and write to asserted file
        let asserted_cited_by = filter_cited_by_by_provenance(&record.cited_by, true);
        if !asserted_cited_by.is_empty() {
            let asserted_record = serde_json::json!({
                "doi": record.doi,
                "arxiv_id": record.arxiv_id,
                "reference_count": record.reference_count,
                "citation_count": asserted_cited_by.len(),
                "cited_by": asserted_cited_by,
            });
            writeln!(writer_asserted, "{}", asserted_record)?;
        }

        // Filter and write to mined file
        let mined_cited_by = filter_cited_by_by_provenance(&record.cited_by, false);
        if !mined_cited_by.is_empty() {
            let mined_record = serde_json::json!({
                "doi": record.doi,
                "arxiv_id": record.arxiv_id,
                "reference_count": record.reference_count,
                "citation_count": mined_cited_by.len(),
                "cited_by": mined_cited_by,
            });
            writeln!(writer_mined, "{}", mined_record)?;
        }
    }

    writer_all.flush()?;
    writer_asserted.flush()?;
    writer_mined.flush()?;

    // Handle failed outputs with split (similar logic)
    if let Some(failed_base) = output_failed {
        let failed_paths = SplitOutputPaths::from_base(failed_base);

        let file_all = File::create(&failed_paths.all)
            .with_context(|| format!("Failed to create: {:?}", failed_paths.all))?;
        let file_asserted = File::create(&failed_paths.asserted)
            .with_context(|| format!("Failed to create: {:?}", failed_paths.asserted))?;
        let file_mined = File::create(&failed_paths.mined)
            .with_context(|| format!("Failed to create: {:?}", failed_paths.mined))?;

        let mut writer_all = BufWriter::new(file_all);
        let mut writer_asserted = BufWriter::new(file_asserted);
        let mut writer_mined = BufWriter::new(file_mined);

        for record in failed {
            // Write to main file
            writeln!(writer_all, "{}", serde_json::to_string(record)?)?;

            // Filter and write to asserted file
            let asserted_cited_by = filter_cited_by_by_provenance(&record.cited_by, true);
            if !asserted_cited_by.is_empty() {
                let asserted_record = serde_json::json!({
                    "doi": record.doi,
                    "arxiv_id": record.arxiv_id,
                    "reference_count": record.reference_count,
                    "citation_count": asserted_cited_by.len(),
                    "cited_by": asserted_cited_by,
                });
                writeln!(writer_asserted, "{}", asserted_record)?;
            }

            // Filter and write to mined file
            let mined_cited_by = filter_cited_by_by_provenance(&record.cited_by, false);
            if !mined_cited_by.is_empty() {
                let mined_record = serde_json::json!({
                    "doi": record.doi,
                    "arxiv_id": record.arxiv_id,
                    "reference_count": record.reference_count,
                    "citation_count": mined_cited_by.len(),
                    "cited_by": mined_cited_by,
                });
                writeln!(writer_mined, "{}", mined_record)?;
            }
        }

        writer_all.flush()?;
        writer_asserted.flush()?;
        writer_mined.flush()?;

        info!("Wrote split failed output files:");
        info!("  All: {:?}", failed_paths.all);
        info!("  Asserted: {:?}", failed_paths.asserted);
        info!("  Mined: {:?}", failed_paths.mined);
    }

    info!("Wrote split output files:");
    info!("  All: {:?}", paths.all);
    info!("  Asserted: {:?}", paths.asserted);
    info!("  Mined: {:?}", paths.mined);

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
            arxiv_id: None,
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

    #[test]
    fn test_citation_record_parses_arxiv_format() {
        // arXiv format uses arxiv_doi instead of doi
        let arxiv_json = r#"{"arxiv_doi":"10.48550/arXiv.2301.00001","arxiv_id":"2301.00001","reference_count":1,"citation_count":5,"cited_by":[]}"#;
        let record: CitationRecord = serde_json::from_str(arxiv_json).unwrap();
        assert_eq!(record.doi, "10.48550/arXiv.2301.00001");
        assert_eq!(record.arxiv_id, Some("2301.00001".to_string()));
    }

    #[test]
    fn test_citation_record_parses_generic_format() {
        // Generic format uses doi
        let generic_json =
            r#"{"doi":"10.1234/test","reference_count":1,"citation_count":5,"cited_by":[]}"#;
        let record: CitationRecord = serde_json::from_str(generic_json).unwrap();
        assert_eq!(record.doi, "10.1234/test");
        assert_eq!(record.arxiv_id, None);
    }

    #[test]
    fn test_write_split_by_provenance() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let base_path = dir.path().join("output.jsonl");

        // Create records with different provenances in cited_by
        let record_mixed = CitationRecord {
            doi: "10.1234/mixed".to_string(),
            arxiv_id: None,
            reference_count: 2,
            citation_count: 2,
            cited_by: vec![
                serde_json::json!({"doi": "10.5555/a", "provenance": "publisher"}),
                serde_json::json!({"doi": "10.5555/b", "provenance": "mined"}),
            ],
        };

        let records = vec![(record_mixed, Source::Crossref)];

        write_validation_results_with_split(&records, &[], base_path.to_str().unwrap(), None)
            .unwrap();

        // Verify main file exists
        assert!(base_path.exists());

        // Verify asserted file has only publisher/crossref entries
        let asserted_path = dir.path().join("output_asserted.jsonl");
        assert!(asserted_path.exists());
        let asserted_content = std::fs::read_to_string(&asserted_path).unwrap();
        assert!(asserted_content.contains("publisher"));
        assert!(!asserted_content.contains("\"provenance\":\"mined\""));

        // Verify mined file has only mined entries
        let mined_path = dir.path().join("output_mined.jsonl");
        assert!(mined_path.exists());
        let mined_content = std::fs::read_to_string(&mined_path).unwrap();
        assert!(mined_content.contains("mined"));
    }
}
