use anyhow::{Context, Result};
use log::{info, warn};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::time::Instant;

use crate::cli::InvertArgs;
use crate::common::{
    format_elapsed, create_bytes_progress_bar, create_count_progress_bar, setup_logging,
    ArxivCitations, CitingWork, InvertStats, ReferenceMatch,
};

/// Input format from extract step (for deserialization only)
#[derive(Debug, Deserialize)]
struct InputRecord {
    doi: String,
    arxiv_matches: Vec<InputArxivMatch>,
    references: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct InputArxivMatch {
    id: String,
    raw: String,
    arxiv_doi: String,
}

/// Run the invert command with the given arguments
pub fn run_invert(args: InvertArgs) -> Result<InvertStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Crossref ArXiv Refs Invert");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);

    let mut index: HashMap<String, (String, HashMap<String, Vec<ReferenceMatch>>)> = HashMap::new();

    let input_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;
    let file_size = input_file.metadata()?.len();
    let reader = BufReader::new(input_file);
    let progress = create_bytes_progress_bar(file_size);

    let mut lines_processed: usize = 0;
    let mut lines_failed: usize = 0;
    let mut total_citations: usize = 0;

    info!("Reading input and building inverted index...");

    for line_result in reader.lines() {
        let line = line_result.context("Failed to read line")?;
        progress.inc(line.len() as u64 + 1); // +1 for newline

        if line.trim().is_empty() {
            continue;
        }

        let record: InputRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to parse line {}: {}", lines_processed + 1, e);
                lines_failed += 1;
                continue;
            }
        };

        lines_processed += 1;

        for arxiv_match in &record.arxiv_matches {
            // Find the reference containing this arXiv match via string matching
            let matching_ref = record.references.iter()
                .find(|r| {
                    let ref_str = serde_json::to_string(r).unwrap_or_default();
                    ref_str.to_lowercase().contains(&arxiv_match.raw.to_lowercase())
                        || ref_str.to_lowercase().contains(&arxiv_match.id.to_lowercase())
                })
                .cloned()
                .unwrap_or(Value::Null);

            let ref_match = ReferenceMatch {
                raw_match: arxiv_match.raw.clone(),
                reference: matching_ref,
            };

            total_citations += 1;

            index
                .entry(arxiv_match.arxiv_doi.clone())
                .or_insert_with(|| (arxiv_match.id.clone(), HashMap::new()))
                .1
                .entry(record.doi.clone())
                .or_insert_with(Vec::new)
                .push(ref_match);
        }

        if lines_processed % 100000 == 0 {
            progress.set_message(format!(
                "{} records | {} arXiv works | {} citations",
                lines_processed,
                index.len(),
                total_citations
            ));
        }
    }

    progress.finish_with_message("Index building complete");

    let mut entries: Vec<ArxivCitations> = index
        .into_iter()
        .map(|(arxiv_doi, (arxiv_id, citing_works))| {
            let cited_by: Vec<CitingWork> = citing_works
                .into_iter()
                .map(|(doi, matches)| CitingWork { doi, matches })
                .collect();
            let reference_count: usize = cited_by.iter().map(|c| c.matches.len()).sum();
            let citation_count = cited_by.len();
            ArxivCitations {
                arxiv_doi,
                arxiv_id,
                reference_count,
                citation_count,
                cited_by,
            }
        })
        .collect();

    let total_unique_citations: usize = entries.iter().map(|e| e.citation_count).sum();

    info!("Input processing complete:");
    info!("  Records processed: {}", lines_processed);
    info!("  Records failed: {}", lines_failed);
    info!("  Unique arXiv works: {}", entries.len());
    info!("  Total references: {}", total_citations);
    info!("  Unique citing works: {}", total_unique_citations);

    info!("Writing output file...");
    let output_file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;
    let mut writer = BufWriter::new(output_file);
    let write_progress = create_count_progress_bar(entries.len() as u64);

    entries.sort_by(|a, b| b.citation_count.cmp(&a.citation_count));
    let num_entries = entries.len();

    for citations in &entries {
        let json_line = serde_json::to_string(citations)
            .context("Failed to serialize output")?;
        writeln!(writer, "{}", json_line)
            .context("Failed to write output")?;
        write_progress.inc(1);
    }

    writer.flush()?;
    write_progress.finish_with_message("Write complete");

    let total_time = start_time.elapsed();
    let stats = InvertStats {
        records_processed: lines_processed,
        records_failed: lines_failed,
        unique_arxiv_works: num_entries,
        total_references: total_citations,
        unique_citing_works: total_unique_citations,
    };

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("Input records: {}", stats.records_processed);
    info!("Unique arXiv works: {}", stats.unique_arxiv_works);
    info!("Total references: {}", stats.total_references);
    info!("Unique citing works: {}", stats.unique_citing_works);
    info!("Output file: {}", args.output);
    info!("========================================================");

    Ok(stats)
}
