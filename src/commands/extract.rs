use anyhow::{Context, Result};
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use log::{debug, error, info, warn};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tar::Archive;

use crate::cli::ExtractArgs;
use crate::common::{
    format_elapsed, create_spinner, setup_logging,
    ArxivMatch, ArxivRefMatch, ExtractStats,
};
use crate::extract::extract_arxiv_matches_from_text;

struct ProcessingStats {
    total_records: AtomicUsize,
    records_with_refs: AtomicUsize,
    arxiv_matches: AtomicUsize,
    total_arxiv_ids: AtomicUsize,
    json_files_processed: AtomicUsize,
}

impl ProcessingStats {
    fn new() -> Self {
        Self {
            total_records: AtomicUsize::new(0),
            records_with_refs: AtomicUsize::new(0),
            arxiv_matches: AtomicUsize::new(0),
            total_arxiv_ids: AtomicUsize::new(0),
            json_files_processed: AtomicUsize::new(0),
        }
    }

    fn log_current(&self) {
        info!(
            "Progress: {} JSON files | {} records | {} with refs | {} arXiv matches | {} arXiv IDs",
            self.json_files_processed.load(Ordering::Relaxed),
            self.total_records.load(Ordering::Relaxed),
            self.records_with_refs.load(Ordering::Relaxed),
            self.arxiv_matches.load(Ordering::Relaxed),
            self.total_arxiv_ids.load(Ordering::Relaxed),
        );
    }

    fn to_extract_stats(&self, records_written: usize) -> ExtractStats {
        ExtractStats {
            json_files_processed: self.json_files_processed.load(Ordering::Relaxed),
            total_records: self.total_records.load(Ordering::Relaxed),
            records_with_refs: self.records_with_refs.load(Ordering::Relaxed),
            arxiv_matches: self.arxiv_matches.load(Ordering::Relaxed),
            total_arxiv_ids: self.total_arxiv_ids.load(Ordering::Relaxed),
            records_written,
        }
    }
}

fn extract_arxiv_from_reference(reference: &Value) -> (Vec<ArxivMatch>, bool) {
    let mut all_matches: HashMap<String, ArxivMatch> = HashMap::new();
    let mut has_arxiv = false;

    if let Some(doi) = reference.get("DOI").and_then(|v| v.as_str()) {
        let matches = extract_arxiv_matches_from_text(doi);
        if !matches.is_empty() {
            has_arxiv = true;
            for m in matches {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    if let Some(unstructured) = reference.get("unstructured").and_then(|v| v.as_str()) {
        if unstructured.to_lowercase().contains("arxiv") {
            has_arxiv = true;
            for m in extract_arxiv_matches_from_text(unstructured) {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    if let Some(title) = reference.get("article-title").and_then(|v| v.as_str()) {
        if title.to_lowercase().contains("arxiv") {
            has_arxiv = true;
            for m in extract_arxiv_matches_from_text(title) {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    if let Some(journal) = reference.get("journal-title").and_then(|v| v.as_str()) {
        if journal.to_lowercase().contains("arxiv") {
            has_arxiv = true;
            for m in extract_arxiv_matches_from_text(journal) {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    if let Some(url) = reference.get("URL").and_then(|v| v.as_str()) {
        if url.to_lowercase().contains("arxiv") {
            has_arxiv = true;
            for m in extract_arxiv_matches_from_text(url) {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    (all_matches.into_values().collect(), has_arxiv)
}

fn process_record(record: &Value) -> Option<ArxivRefMatch> {
    let doi = record.get("DOI").and_then(|v| v.as_str())?;

    let references = match record.get("reference") {
        Some(Value::Array(refs)) => refs,
        _ => return None,
    };

    let mut all_matches: HashMap<String, ArxivMatch> = HashMap::new();
    let mut matching_references = Vec::new();

    for reference in references {
        let (matches, has_arxiv) = extract_arxiv_from_reference(reference);

        if has_arxiv {
            matching_references.push(reference.clone());
            for m in matches {
                all_matches.entry(m.id.clone()).or_insert(m);
            }
        }
    }

    if matching_references.is_empty() || all_matches.is_empty() {
        return None;
    }

    let mut arxiv_matches: Vec<ArxivMatch> = all_matches.into_values().collect();
    arxiv_matches.sort_by(|a, b| a.id.cmp(&b.id));

    Some(ArxivRefMatch {
        doi: doi.to_string(),
        arxiv_matches,
        references: matching_references,
    })
}

fn process_json_items(items: &[Value], stats: &ProcessingStats) -> Vec<ArxivRefMatch> {
    items
        .par_iter()
        .filter_map(|record| {
            stats.total_records.fetch_add(1, Ordering::Relaxed);

            if record.get("reference").is_some() {
                stats.records_with_refs.fetch_add(1, Ordering::Relaxed);
            }

            let result = process_record(record);

            if let Some(ref match_result) = result {
                stats.arxiv_matches.fetch_add(1, Ordering::Relaxed);
                stats.total_arxiv_ids.fetch_add(match_result.arxiv_matches.len(), Ordering::Relaxed);
            }

            result
        })
        .collect()
}

/// Run the extract command with the given arguments
pub fn run_extract(args: ExtractArgs) -> Result<ExtractStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Crossref ArXiv Reference Extractor");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);

    let num_threads = if args.threads == 0 {
        let cores = num_cpus::get();
        info!("Auto-detected {} CPU cores. Using {} threads.", cores, cores);
        cores
    } else {
        info!("Using specified {} threads.", args.threads);
        args.threads
    };

    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
    {
        error!("Failed to build thread pool: {}. Using default.", e);
    }

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    let (sender, receiver) = bounded::<ArxivRefMatch>(num_threads * 100);

    let output_path = args.output.clone();
    let writer_thread = thread::spawn(move || -> Result<usize> {
        let file = File::create(&output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;
        let mut writer = BufWriter::new(file);
        let mut count = 0;

        for match_result in receiver {
            let json_line = serde_json::to_string(&match_result)
                .context("Failed to serialize match result")?;
            writeln!(writer, "{}", json_line)
                .context("Failed to write to output file")?;
            count += 1;

            if count % 10000 == 0 {
                writer.flush()?;
            }
        }

        writer.flush()?;
        Ok(count)
    });

    let stats = Arc::new(ProcessingStats::new());
    let stats_thread_running = Arc::new(std::sync::Mutex::new(true));
    let stats_clone = Arc::clone(&stats);
    let stats_running_clone = Arc::clone(&stats_thread_running);
    let stats_interval = Duration::from_secs(args.stats_interval);

    let stats_thread = thread::spawn(move || {
        let mut last_log = Instant::now();
        loop {
            if let Ok(running) = stats_running_clone.lock() {
                if !*running {
                    break;
                }
            }

            thread::sleep(Duration::from_millis(500));

            if last_log.elapsed() >= stats_interval {
                stats_clone.log_current();
                last_log = Instant::now();
            }
        }
    });

    info!("Opening tar.gz archive...");
    let tar_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;

    let gz_decoder = GzDecoder::new(tar_file);
    let mut archive = Archive::new(gz_decoder);

    let progress = create_spinner("Processing archive entries...");

    let batch_size = args.batch_size;

    for entry_result in archive.entries()? {
        let mut entry = entry_result.context("Failed to read tar entry")?;
        let path = entry.path()?.to_path_buf();

        if !path.extension().map_or(false, |ext| ext == "json") {
            continue;
        }

        let file_name = path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        progress.set_message(format!("Processing: {}", file_name));

        let mut content = String::new();
        entry.read_to_string(&mut content)
            .with_context(|| format!("Failed to read content from: {}", path.display()))?;

        let json_data: Value = match serde_json::from_str(&content) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to parse JSON in {}: {}", path.display(), e);
                continue;
            }
        };

        let items = match json_data.get("items") {
            Some(Value::Array(items)) => items.clone(),
            Some(_) => {
                warn!("'items' is not an array in {}", path.display());
                continue;
            }
            None => {
                warn!("No 'items' field found in {}", path.display());
                continue;
            }
        };

        stats.json_files_processed.fetch_add(1, Ordering::Relaxed);

        for chunk in items.chunks(batch_size) {
            let matches = process_json_items(chunk, &stats);

            for m in matches {
                if let Err(e) = sender.send(m) {
                    error!("Failed to send match to writer: {}", e);
                }
            }
        }

        debug!("Completed processing: {} ({} items)", file_name, items.len());
    }

    progress.finish_with_message("Archive processing complete");

    drop(sender);

    info!("Waiting for writer thread to finish...");
    let records_written = match writer_thread.join() {
        Ok(Ok(count)) => count,
        Ok(Err(e)) => {
            error!("Writer thread error: {}", e);
            0
        }
        Err(e) => {
            error!("Writer thread panicked: {:?}", e);
            0
        }
    };

    if let Ok(mut running) = stats_thread_running.lock() {
        *running = false;
    }
    let _ = stats_thread.join();

    let final_stats = stats.to_extract_stats(records_written);
    let total_time = start_time.elapsed();

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("JSON files processed: {}", final_stats.json_files_processed);
    info!("Total records scanned: {}", final_stats.total_records);
    info!("Records with references: {}", final_stats.records_with_refs);
    info!("DOIs with arXiv references: {}", final_stats.arxiv_matches);
    info!("Total arXiv IDs extracted: {}", final_stats.total_arxiv_ids);
    info!("Records written to output: {}", final_stats.records_written);
    info!("Output file: {}", args.output);
    info!("========================================================");

    Ok(final_stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_process_reference_with_arxiv() {
        let reference = json!({
            "unstructured": "Smith J. (2024). A great paper. arXiv preprint arXiv:2403.03542.",
            "key": "ref1"
        });

        let (matches, has_arxiv) = extract_arxiv_from_reference(&reference);
        assert!(has_arxiv);
        assert!(matches.iter().any(|m| m.id == "2403.03542"));
    }

    #[test]
    fn test_process_reference_without_arxiv() {
        let reference = json!({
            "unstructured": "Smith J. (2024). A great paper. Nature 123:456.",
            "key": "ref1"
        });

        let (matches, has_arxiv) = extract_arxiv_from_reference(&reference);
        assert!(!has_arxiv);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_process_record_with_arxiv_refs() {
        let record = json!({
            "DOI": "10.1234/test.paper",
            "reference": [
                {
                    "unstructured": "arXiv preprint arXiv:2403.03542",
                    "key": "ref1"
                },
                {
                    "journal-title": "Nature",
                    "volume": "123",
                    "key": "ref2"
                }
            ]
        });

        let result = process_record(&record);
        assert!(result.is_some());

        let match_result = result.unwrap();
        assert_eq!(match_result.doi, "10.1234/test.paper");
        assert!(match_result.arxiv_matches.iter().any(|m| m.id == "2403.03542"));
        assert_eq!(match_result.references.len(), 1);
    }

    #[test]
    fn test_process_record_filters_empty_matches() {
        // Test that records with arxiv mention but no extractable ID are filtered out
        let record = json!({
            "DOI": "10.1234/test.paper",
            "reference": [
                {
                    "journal-title": "arXiv Prepr",
                    "key": "ref1"
                    // No volume/issue, so no ID can be extracted
                }
            ]
        });

        let result = process_record(&record);
        assert!(result.is_none()); // Should be filtered out
    }
}
