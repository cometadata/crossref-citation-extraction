use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use log::{debug, info, warn};
use reqwest::Client;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::cli::ValidateArgs;
use crate::common::{
    format_elapsed, create_bytes_progress_bar, create_count_progress_bar, setup_logging,
    ArxivCitationsSimple, DataCiteRecord, ValidateStats,
};

fn load_datacite_dois(path: &str) -> Result<HashSet<String>> {
    info!("Loading DataCite DOIs from: {}", path);
    let start = Instant::now();

    let file = File::open(path)
        .with_context(|| format!("Failed to open records file: {}", path))?;

    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut dois: HashSet<String> = HashSet::new();
    let mut lines_processed = 0;
    let mut lines_failed = 0;

    for line_result in reader.lines() {
        let line = line_result.context("Failed to read line from records")?;

        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<DataCiteRecord>(&line) {
            Ok(record) => {
                dois.insert(record.id.to_lowercase());
            }
            Err(e) => {
                if lines_failed < 5 {
                    warn!("Failed to parse record: {}", e);
                }
                lines_failed += 1;
            }
        }

        lines_processed += 1;
        if lines_processed % 500000 == 0 {
            info!("  Loaded {} DOIs...", dois.len());
        }
    }

    info!("Loaded {} unique DOIs from {} records in {}",
          dois.len(), lines_processed, format_elapsed(start.elapsed()));

    if lines_failed > 0 {
        warn!("Failed to parse {} records", lines_failed);
    }

    Ok(dois)
}

async fn check_doi_resolves(client: &Client, doi: &str, timeout: Duration) -> bool {
    let url = format!("https://doi.org/{}", doi);

    match client.head(&url)
        .timeout(timeout)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            status.is_redirection() || status.is_success()
        }
        Err(e) => {
            debug!("DOI resolution failed for {}: {}", doi, e);
            false
        }
    }
}

pub async fn run_validate_async(args: ValidateArgs) -> Result<ValidateStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Crossref ArXiv Citations Validator");
    info!("Input: {}", args.input);
    info!("Records: {}", args.records);
    info!("Output valid: {}", args.output_valid);
    info!("Output failed: {}", args.output_failed);
    info!("Concurrency: {}", args.concurrency);
    info!("Timeout: {}s", args.timeout);

    let datacite_dois = load_datacite_dois(&args.records)?;

    let input_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;
    let file_size = input_file.metadata()?.len();
    let reader = BufReader::new(input_file);

    info!("Categorizing citations...");

    let progress = create_bytes_progress_bar(file_size);

    let mut matched: Vec<ArxivCitationsSimple> = Vec::new();
    let mut unmatched: Vec<ArxivCitationsSimple> = Vec::new();
    let mut lines_processed = 0;

    for line_result in reader.lines() {
        let line = line_result.context("Failed to read line")?;
        progress.inc(line.len() as u64 + 1);

        if line.trim().is_empty() {
            continue;
        }

        let record: ArxivCitationsSimple = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to parse line {}: {}", lines_processed + 1, e);
                continue;
            }
        };

        lines_processed += 1;

        if datacite_dois.contains(&record.arxiv_doi.to_lowercase()) {
            matched.push(record);
        } else {
            unmatched.push(record);
        }

        if lines_processed % 100000 == 0 {
            progress.set_message(format!(
                "{} records | {} matched | {} unmatched",
                lines_processed, matched.len(), unmatched.len()
            ));
        }
    }

    progress.finish_with_message("Categorization complete");

    info!("Categorization results:");
    info!("  Total records: {}", lines_processed);
    info!("  Matched in DataCite: {}", matched.len());
    info!("  Unmatched (need resolution check): {}", unmatched.len());

    info!("Writing {} matched records to valid output...", matched.len());
    let valid_file = File::create(&args.output_valid)
        .with_context(|| format!("Failed to create output file: {}", args.output_valid))?;
    let mut valid_writer = BufWriter::new(valid_file);

    for record in &matched {
        let json_line = serde_json::to_string(record)?;
        writeln!(valid_writer, "{}", json_line)?;
    }
    valid_writer.flush()?;

    let matched_count = matched.len();
    drop(matched);

    let mut final_resolved = 0;
    let mut final_failed = 0;

    if unmatched.is_empty() {
        info!("No unmatched records to check");
        File::create(&args.output_failed)
            .with_context(|| format!("Failed to create output file: {}", args.output_failed))?;
    } else {
        info!("Checking resolution for {} unmatched DOIs...", unmatched.len());

        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let timeout = Duration::from_secs(args.timeout);
        let semaphore = Arc::new(Semaphore::new(args.concurrency));

        let resolved_count = Arc::new(AtomicUsize::new(0));
        let failed_count = Arc::new(AtomicUsize::new(0));
        let checked_count = Arc::new(AtomicUsize::new(0));

        let check_progress = create_count_progress_bar(unmatched.len() as u64);

        let results: Vec<(ArxivCitationsSimple, bool)> = stream::iter(unmatched.into_iter())
            .map(|record| {
                let client = client.clone();
                let semaphore = semaphore.clone();
                let resolved = resolved_count.clone();
                let failed = failed_count.clone();
                let checked = checked_count.clone();
                let progress = check_progress.clone();

                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let doi = &record.arxiv_doi;
                    let resolves = check_doi_resolves(&client, doi, timeout).await;

                    if resolves {
                        resolved.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }

                    let count = checked.fetch_add(1, Ordering::Relaxed) + 1;
                    if count % 1000 == 0 {
                        progress.set_message(format!(
                            "resolved: {} | failed: {}",
                            resolved.load(Ordering::Relaxed),
                            failed.load(Ordering::Relaxed)
                        ));
                    }
                    progress.inc(1);

                    (record, resolves)
                }
            })
            .buffer_unordered(args.concurrency * 2)
            .collect()
            .await;

        check_progress.finish_with_message("Resolution check complete");

        info!("Resolution check results:");
        info!("  Resolved: {}", resolved_count.load(Ordering::Relaxed));
        info!("  Failed: {}", failed_count.load(Ordering::Relaxed));

        let valid_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&args.output_valid)?;
        let mut valid_writer = BufWriter::new(valid_file);

        let failed_file = File::create(&args.output_failed)
            .with_context(|| format!("Failed to create output file: {}", args.output_failed))?;
        let mut failed_writer = BufWriter::new(failed_file);

        for (record, resolves) in results {
            let json_line = serde_json::to_string(&record)?;
            if resolves {
                writeln!(valid_writer, "{}", json_line)?;
                final_resolved += 1;
            } else {
                writeln!(failed_writer, "{}", json_line)?;
                final_failed += 1;
            }
        }

        valid_writer.flush()?;
        failed_writer.flush()?;
    }

    let total_time = start_time.elapsed();
    let stats = ValidateStats {
        total_records: lines_processed,
        matched_in_datacite: matched_count,
        resolution_resolved: final_resolved,
        resolution_failed: final_failed,
        total_valid: matched_count + final_resolved,
        total_failed: final_failed,
    };

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("Input records: {}", stats.total_records);
    info!("Matched in DataCite: {}", stats.matched_in_datacite);
    info!("Resolution checks: {} resolved, {} failed", stats.resolution_resolved, stats.resolution_failed);
    info!("Total valid: {}", stats.total_valid);
    info!("Total failed: {}", stats.total_failed);
    info!("Output valid: {}", args.output_valid);
    info!("Output failed: {}", args.output_failed);
    info!("========================================================");

    Ok(stats)
}

pub fn run_validate(args: ValidateArgs) -> Result<ValidateStats> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_validate_async(args))
}
