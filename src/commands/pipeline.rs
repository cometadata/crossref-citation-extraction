use anyhow::Result;
use log::{info, warn};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::cli::{should_include_provenance_filter, PipelineArgs};
use crate::common::{extract_input_stem, setup_logging, MatchType, OutputPaths};
use crate::extract::{extract_arxiv_matches_from_text, Provenance};
use crate::index::{build_fst_index_from_source, FstIndex};
use crate::input::{
    detect_crossref_input, detect_datacite_input, open_datacite_source, visit_crossref_entries,
    CrossrefEntry,
};
use crate::streaming::{
    aggregate_and_validate, ExtractionCheckpoint, PartitionRow, SegmentedPartitionWriter,
};

const FLUSH_THRESHOLD_DIVISOR: usize = 100;

/// Files processed between durable checkpoint commits during extraction.
const CHECKPOINT_FILES_INTERVAL: usize = 25;

/// Raw entries buffered between reader and parse workers.
const ENTRY_CHANNEL_CAP: usize = 4;

/// Per-file result batches buffered between workers and the writer.
const RESULT_CHANNEL_CAP: usize = 32;

/// Extraction reads only the work DOI, the reference array, and five fields per
/// reference. Parsing whole files into `serde_json::Value` allocated a tree node
/// for every other field too; these types read just what is used and leave each
/// reference as an unparsed slice of the source buffer.
#[derive(serde::Deserialize)]
struct CrossrefFile<'a> {
    #[serde(default, borrow)]
    items: Vec<CrossrefItem<'a>>,
}

#[derive(serde::Deserialize)]
struct CrossrefItem<'a> {
    #[serde(rename = "DOI", default, borrow)]
    doi: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "reference", default, borrow)]
    references: Vec<&'a serde_json::value::RawValue>,
}

#[derive(serde::Deserialize, Default)]
struct RefFields<'a> {
    #[serde(rename = "DOI", default, borrow)]
    doi: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "URL", default, borrow)]
    url: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, borrow)]
    unstructured: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "article-title", default, borrow)]
    article_title: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "journal-title", default, borrow)]
    journal_title: Option<std::borrow::Cow<'a, str>>,
    #[serde(rename = "doi-asserted-by", default, borrow)]
    doi_asserted_by: Option<std::borrow::Cow<'a, str>>,
}

/// Make all buffered rows durable, then mark pending files complete.
/// Order matters: a file checkpointed before its rows are flushed would be
/// skipped on resume with those rows lost.
fn commit_progress(
    writer: &mut SegmentedPartitionWriter,
    checkpoint: &mut Option<ExtractionCheckpoint>,
    pending: &mut Vec<String>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    writer.flush_all()?;
    if let Some(cp) = checkpoint {
        for filename in pending.iter() {
            cp.mark_completed(filename)?;
        }
    }
    pending.clear();
    Ok(())
}

/// Quick check if reference has any fields worth searching.
#[inline]
fn has_searchable_content(reference: &RefFields<'_>) -> bool {
    reference.doi.is_some()
        || reference.unstructured.is_some()
        || reference.url.is_some()
        || reference.article_title.is_some()
        || reference.journal_title.is_some()
}

#[inline]
fn quick_arxiv_likely(text: &str) -> bool {
    text.as_bytes()
        .windows(5)
        .any(|w| w.eq_ignore_ascii_case(b"arxiv"))
}

fn build_search_text(reference: &RefFields<'_>, buffer: &mut String) -> bool {
    buffer.clear();

    for v in [
        &reference.doi,
        &reference.url,
        &reference.article_title,
        &reference.journal_title,
    ]
    .into_iter()
    .flatten()
    {
        buffer.push_str(v);
        buffer.push(' ');
    }

    if let Some(v) = &reference.unstructured {
        buffer.push_str(v);
    }

    !buffer.is_empty()
}

#[inline]
fn should_include_citation(citing_doi: &str, cited_id: &str) -> bool {
    !citing_doi.eq_ignore_ascii_case(cited_id)
}

#[inline]
fn determine_provenance(reference: &RefFields<'_>, extracted_doi: &str) -> Provenance {
    if let Some(doi_field) = &reference.doi {
        if doi_field.eq_ignore_ascii_case(extracted_doi) {
            return match reference.doi_asserted_by.as_deref() {
                Some("publisher") => Provenance::Publisher,
                Some("crossref") => Provenance::Crossref,
                _ => Provenance::Mined,
            };
        }
    }

    Provenance::Mined
}

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation extraction pipeline");
    info!("Input: {}", args.input);
    if !args.provenance.is_empty() {
        info!("Provenance filter: {}", args.provenance.join(", "));
    }

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    run_scalable_pipeline(&args)
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    if args.arxiv_records.is_none() && args.arxiv_fst.is_none() {
        return Err(anyhow::anyhow!(
            "arXiv extraction requires --arxiv-records or --arxiv-fst"
        ));
    }
    Ok(())
}

/// Run the scalable pipeline with FST indexes and streaming aggregation
fn run_scalable_pipeline(args: &PipelineArgs) -> Result<()> {
    info!("Running scalable pipeline mode");

    let (partition_dir, skip_extraction) = if let Some(ref existing_dir) = args.partitions_dir {
        let path = PathBuf::from(existing_dir);
        if !path.exists() {
            return Err(anyhow::anyhow!(
                "Partitions directory does not exist: {}",
                existing_dir
            ));
        }
        info!("Using existing partitions from: {}", existing_dir);
        (path, true)
    } else {
        let path = args
            .temp_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("arxiv-scalable-{}", Uuid::new_v4()))
            });
        fs::create_dir_all(&path)?;
        (path, false)
    };

    let arxiv_fst = load_or_build_arxiv_fst(args, &partition_dir)?;

    if skip_extraction {
        info!("=== Skipping Extraction (using existing partitions) ===");
    } else {
        info!("=== Extraction Phase ===");
        run_scalable_extraction(args, &partition_dir)?;
    }

    let output_types = args.parse_outputs().map_err(|e| anyhow::anyhow!(e))?;

    let output_dir = args
        .output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    let input_stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&output_types, &output_dir, &input_stem);

    if !outputs.has_any() {
        return Err(anyhow::anyhow!(
            "No outputs specified. Use --outputs to specify at least one output type."
        ));
    }

    info!("=== Aggregation Phase ===");

    let stats = aggregate_and_validate(
        &partition_dir,
        arxiv_fst.as_ref(),
        &outputs,
        args.resume,
        args.checkpoint_interval,
    )?;

    info!("Pipeline complete:");
    info!("  Valid: {}", stats.valid_count);
    info!("  Failed: {}", stats.failed_count);
    info!("  Total citations: {}", stats.total_citations);

    let has_provenance_outputs =
        outputs.publisher.is_some() || outputs.crossref.is_some() || outputs.mined.is_some();
    if has_provenance_outputs {
        info!("  By provenance:");
        info!("    Publisher: {}", stats.publisher_citations);
        info!("    Crossref: {}", stats.crossref_citations);
        info!("    Mined: {}", stats.mined_citations);
    }

    if let Some(ref p) = outputs.valid {
        info!("Output valid: {}", p.display());
    }
    if let Some(ref p) = outputs.failed {
        info!("Output failed: {}", p.display());
    }
    if let Some(ref p) = outputs.publisher {
        info!("Output publisher: {}", p.display());
    }
    if let Some(ref p) = outputs.crossref {
        info!("Output crossref: {}", p.display());
    }
    if let Some(ref p) = outputs.mined {
        info!("Output mined: {}", p.display());
    }

    if skip_extraction || args.keep_intermediates {
        info!(
            "Intermediate partitions kept at: {}",
            partition_dir.display()
        );
    } else {
        info!(
            "Removing intermediate partition directory: {}",
            partition_dir.display()
        );
        if let Err(e) = fs::remove_dir_all(&partition_dir) {
            warn!(
                "Failed to remove intermediate directory {}: {}",
                partition_dir.display(),
                e
            );
        }
    }

    Ok(())
}

fn load_or_build_arxiv_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.arxiv_fst {
        info!("Loading arXiv FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    if let Some(ref records_path) = args.arxiv_records {
        info!("Building arXiv FST from: {}", records_path);
        let fst_path = temp_dir.join("arxiv.fst");
        let input = detect_datacite_input(records_path)?;
        let source = open_datacite_source(input)?;
        build_fst_index_from_source(source, &fst_path, temp_dir)?;
        return Ok(Some(FstIndex::load(&fst_path)?));
    }

    Ok(None)
}

/// Per-file counters produced by a parse worker, folded into the run totals by
/// the writer thread.
#[derive(Default)]
struct FileStats {
    items: u64,
    extracted: u64,
    filtered_provenance: u64,
    filtered_self_cite: u64,
    no_match: u64,
    refs_unparseable: u64,
}

/// Everything one input file produced: rows to write (in file order), counters,
/// and whether the file failed to parse (a failed file is never checkpointed, so
/// `--resume` retries it).
struct FileResult {
    filename: String,
    rows: Vec<PartitionRow>,
    stats: FileStats,
    parse_failed: bool,
}

/// Parse one Crossref file and extract every arXiv citation it contains.
/// Pure apart from the reusable `search_text` scratch buffer, so parse workers
/// can run it concurrently.
fn extract_file(
    entry: &CrossrefEntry,
    provenance_filter: &[String],
    search_text: &mut String,
) -> FileResult {
    let mut result = FileResult {
        filename: entry.filename.clone(),
        rows: Vec::new(),
        stats: FileStats::default(),
        parse_failed: false,
    };

    let parsed: CrossrefFile = match serde_json::from_slice(&entry.bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse JSON in {}: {}", entry.filename, e);
            result.parse_failed = true;
            return result;
        }
    };

    for item in &parsed.items {
        result.stats.items += 1;

        let work_doi = match &item.doi {
            Some(doi) => doi.to_lowercase(),
            None => continue,
        };

        for (ref_idx, raw_ref) in item.references.iter().enumerate() {
            let ref_index = ref_idx as u32;

            let reference: RefFields = match serde_json::from_str(raw_ref.get()) {
                Ok(r) => r,
                Err(_) => {
                    result.stats.refs_unparseable += 1;
                    continue;
                }
            };

            if !has_searchable_content(&reference) {
                continue;
            }

            if !build_search_text(&reference, search_text) {
                continue;
            }

            if !quick_arxiv_likely(search_text) {
                continue;
            }

            let matches = extract_arxiv_as_dois(search_text);

            if matches.is_empty() {
                result.stats.no_match += 1;
                continue;
            }

            let mut ref_json_cache: Option<String> = None;

            for (doi, raw) in matches {
                if !should_include_citation(&work_doi, &doi) {
                    result.stats.filtered_self_cite += 1;
                    continue;
                }

                let provenance = determine_provenance(&reference, &doi);

                if !should_include_provenance_filter(provenance_filter, provenance.as_str()) {
                    result.stats.filtered_provenance += 1;
                    continue;
                }

                let ref_json = ref_json_cache.get_or_insert_with(|| raw_ref.get().to_string());

                result.rows.push(PartitionRow {
                    citing_doi: work_doi.clone(),
                    ref_index,
                    cited_id: doi, // Move instead of clone - we own this
                    provenance: provenance.as_str().to_string(),
                    match_type: MatchType::Arxiv,
                    raw_match: raw,
                    ref_json: ref_json.clone(),
                });
                result.stats.extracted += 1;
            }
        }
    }

    result
}

fn run_scalable_extraction(args: &PipelineArgs, partition_dir: &Path) -> Result<()> {
    let flush_threshold = (args.batch_size / FLUSH_THRESHOLD_DIVISOR).max(10000);
    let mut partition_writer = SegmentedPartitionWriter::new(partition_dir, flush_threshold)?;

    let input = detect_crossref_input(&args.input)?;

    let checkpoint_path = partition_dir.join("extraction.checkpoint");
    let mut checkpoint = if args.resume {
        let cp = ExtractionCheckpoint::new(&checkpoint_path)?;
        if cp.completed_count() > 0 {
            info!(
                "Resuming extraction: {} files already processed",
                cp.completed_count()
            );
        }
        Some(cp)
    } else {
        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path)?;
        }
        None
    };

    // The reader thread needs its own copy of the completed set: the checkpoint
    // itself stays with the writer, which is the only thread that mutates it.
    let completed: std::collections::HashSet<String> = checkpoint
        .as_ref()
        .map(|cp| cp.completed_snapshot())
        .unwrap_or_default();

    let mut items_processed = 0u64;
    let mut files_processed = 0u64;
    let mut files_skipped = 0u64;
    let mut refs_extracted = 0u64;
    let mut refs_filtered_provenance = 0u64;
    let mut refs_filtered_self_cite = 0u64;
    let mut refs_no_match = 0u64;
    let mut refs_unparseable = 0u64;
    let mut pending_files: Vec<String> = Vec::new();

    // One reader (gzip + tar), n parse/extract workers, and this thread as the
    // writer: only the writer touches the partition writer and the checkpoint.
    let n_workers = num_cpus::get().saturating_sub(2).max(1);
    let (entry_tx, entry_rx) = crossbeam_channel::bounded::<CrossrefEntry>(ENTRY_CHANNEL_CAP);
    let (result_tx, result_rx) = crossbeam_channel::bounded::<FileResult>(RESULT_CHANNEL_CAP);

    let provenance_filter = args.provenance.clone();

    info!(
        "Extracting arXiv IDs from references ({} parse workers)...",
        n_workers
    );

    std::thread::scope(|s| -> Result<()> {
        // Reader: owns the archive; sends raw entries, skipping checkpointed files.
        let reader = s.spawn(move || -> Result<u64> {
            let mut skipped = 0u64;
            visit_crossref_entries(input, |entry| {
                if completed.contains(&entry.filename) {
                    skipped += 1;
                    return Ok(());
                }
                entry_tx
                    .send(entry)
                    .map_err(|_| anyhow::anyhow!("extraction workers hung up"))
            })?;
            Ok(skipped)
        });

        // Workers: parse + extract, one FileResult per file.
        let mut worker_handles = Vec::new();
        for _ in 0..n_workers {
            let entry_rx = entry_rx.clone();
            let result_tx = result_tx.clone();
            let provenance_filter = provenance_filter.clone();
            worker_handles.push(s.spawn(move || {
                let mut search_text = String::with_capacity(512);
                for entry in entry_rx.iter() {
                    let result = extract_file(&entry, &provenance_filter, &mut search_text);
                    if result_tx.send(result).is_err() {
                        break; // writer gone; scope will surface its error
                    }
                }
            }));
        }
        drop(entry_rx);
        drop(result_tx); // writer's rx closes when all workers finish

        // Writer: this thread. Single owner of partition_writer + checkpoint.
        // Consumed by value so an early `?` drops the receiver and lets the
        // workers unblock instead of deadlocking the scope's join.
        for file_result in result_rx {
            for row in file_result.rows {
                partition_writer.write(row)?;
            }
            items_processed += file_result.stats.items;
            refs_extracted += file_result.stats.extracted;
            refs_filtered_provenance += file_result.stats.filtered_provenance;
            refs_filtered_self_cite += file_result.stats.filtered_self_cite;
            refs_no_match += file_result.stats.no_match;
            refs_unparseable += file_result.stats.refs_unparseable;

            if !file_result.parse_failed {
                pending_files.push(file_result.filename);
                files_processed += 1;
                // Logged only on the iteration that moved the counter: outside
                // this branch it would fire once at zero and then again for
                // every parse failure while the counter sat on a multiple.
                if files_processed.is_multiple_of(100) {
                    info!(
                        "Progress: {} files, {} items, {} refs extracted",
                        files_processed, items_processed, refs_extracted
                    );
                }
            }

            // Periodic durable commit only matters when checkpointing; without
            // --resume, segment flushing stays purely threshold-driven.
            if checkpoint.is_some() && pending_files.len() >= CHECKPOINT_FILES_INTERVAL {
                commit_progress(&mut partition_writer, &mut checkpoint, &mut pending_files)?;
            }
        }

        files_skipped = reader
            .join()
            .map_err(|_| anyhow::anyhow!("reader thread panicked"))??;
        for h in worker_handles {
            h.join()
                .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        }
        Ok(())
    })?;

    commit_progress(&mut partition_writer, &mut checkpoint, &mut pending_files)?;
    partition_writer.flush_all()?; // no-op if commit_progress just ran; kept for the checkpoint-disabled path

    info!("Extraction complete:");
    info!("  Files processed: {}", files_processed);
    if files_skipped > 0 {
        info!("  Files skipped (resumed): {}", files_skipped);
    }
    info!("  Items processed: {}", items_processed);
    info!("  Refs extracted: {}", refs_extracted);
    info!(
        "  Refs filtered by provenance: {}",
        refs_filtered_provenance
    );
    info!(
        "  Refs filtered (self-citations): {}",
        refs_filtered_self_cite
    );
    info!("  Refs with no arXiv match: {}", refs_no_match);
    info!("  Refs unparseable: {}", refs_unparseable);

    if let Some(cp) = checkpoint {
        cp.cleanup()?;
    }

    Ok(())
}

fn extract_arxiv_as_dois(text: &str) -> Vec<(String, String)> {
    extract_arxiv_matches_from_text(text)
        .into_iter()
        .map(|m| (m.arxiv_doi, m.raw))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PipelineArgs;

    fn default_args() -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            arxiv_records: Some("arxiv.jsonl.gz".to_string()),
            arxiv_fst: None,
            provenance: vec![],
            outputs: vec![],
            output_dir: None,
            partitions_dir: None,
            log_level: "INFO".to_string(),
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
            resume: false,
            checkpoint_interval: 1,
        }
    }

    #[test]
    fn test_validate_args_requires_arxiv_index() {
        let mut args = default_args();
        args.arxiv_records = None;
        args.arxiv_fst = None;
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--arxiv-records"));
    }

    #[test]
    fn test_validate_args_with_records() {
        let mut args = default_args();
        args.arxiv_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_with_fst() {
        let mut args = default_args();
        args.arxiv_records = None;
        args.arxiv_fst = Some("arxiv.fst".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_citation() {
        assert!(should_include_citation("10.1234/a", "10.5678/b"));
        assert!(!should_include_citation("10.1234/a", "10.1234/a"));
        assert!(!should_include_citation("10.1234/A", "10.1234/a")); // Case insensitive
    }

    fn ref_fields(json: &str) -> RefFields<'_> {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_determine_provenance() {
        use crate::extract::Provenance;

        // Publisher asserted
        let ref_publisher =
            ref_fields(r#"{"DOI": "10.1234/test", "doi-asserted-by": "publisher"}"#);
        assert_eq!(
            determine_provenance(&ref_publisher, "10.1234/test"),
            Provenance::Publisher
        );

        // Crossref asserted
        let ref_crossref = ref_fields(r#"{"DOI": "10.1234/test", "doi-asserted-by": "crossref"}"#);
        assert_eq!(
            determine_provenance(&ref_crossref, "10.1234/test"),
            Provenance::Crossref
        );

        // DOI present but no doi-asserted-by
        let ref_no_assertion = ref_fields(r#"{"DOI": "10.1234/test"}"#);
        assert_eq!(
            determine_provenance(&ref_no_assertion, "10.1234/test"),
            Provenance::Mined
        );

        // Mined from unstructured (DOI not in DOI field)
        let ref_unstructured = ref_fields(r#"{"unstructured": "See doi:10.1234/test"}"#);
        assert_eq!(
            determine_provenance(&ref_unstructured, "10.1234/test"),
            Provenance::Mined
        );
    }

    #[test]
    fn test_has_searchable_content() {
        // Has DOI field
        assert!(has_searchable_content(&ref_fields(
            r#"{"DOI": "10.1234/test"}"#
        )));

        // Has unstructured field
        assert!(has_searchable_content(&ref_fields(
            r#"{"unstructured": "Some text"}"#
        )));

        // Has URL field
        assert!(has_searchable_content(&ref_fields(
            r#"{"URL": "https://example.com"}"#
        )));

        // Has article-title field
        assert!(has_searchable_content(&ref_fields(
            r#"{"article-title": "A Paper"}"#
        )));

        // Has journal-title field
        assert!(has_searchable_content(&ref_fields(
            r#"{"journal-title": "Nature"}"#
        )));

        // Empty reference
        assert!(!has_searchable_content(&ref_fields(r#"{}"#)));

        // Only has irrelevant fields
        assert!(!has_searchable_content(&ref_fields(
            r#"{"key": "r1", "author": "Smith"}"#
        )));
    }

    #[test]
    fn test_quick_arxiv_likely() {
        // Contains arXiv (various cases)
        assert!(quick_arxiv_likely("See arXiv:2403.12345"));
        assert!(quick_arxiv_likely("ARXIV paper reference"));
        assert!(quick_arxiv_likely("Available at arxiv.org/abs/2403.12345"));
        assert!(quick_arxiv_likely("See ArXiV preprint")); // mixed case, no colon

        // Does not contain arXiv
        assert!(!quick_arxiv_likely("Just a regular paper"));
        assert!(!quick_arxiv_likely("10.1234/example"));
        assert!(!quick_arxiv_likely("")); // Empty string
    }

    #[test]
    fn test_build_search_text() {
        let mut buffer = String::new();

        // Single field
        let has_text = build_search_text(&ref_fields(r#"{"DOI": "10.1234/test"}"#), &mut buffer);
        assert!(has_text);
        assert!(buffer.contains("10.1234/test"));

        // Multiple fields
        let has_text = build_search_text(
            &ref_fields(
                r#"{
                "DOI": "10.1234/test",
                "unstructured": "A paper about something"
            }"#,
            ),
            &mut buffer,
        );
        assert!(has_text);
        assert!(buffer.contains("10.1234/test"));
        assert!(buffer.contains("A paper about something"));

        // Empty reference
        let has_text = build_search_text(&ref_fields(r#"{}"#), &mut buffer);
        assert!(!has_text);

        // Only irrelevant fields
        let has_text = build_search_text(
            &ref_fields(r#"{"key": "ref1", "author": "Smith"}"#),
            &mut buffer,
        );
        assert!(!has_text);
    }

    #[test]
    fn test_provenance_filter() {
        // No filter (empty) - includes all
        let mut args = default_args();
        assert!(args.should_include_provenance("publisher"));
        assert!(args.should_include_provenance("crossref"));
        assert!(args.should_include_provenance("mined"));

        // Filter to mined only
        args.provenance = vec!["mined".to_string()];
        assert!(!args.should_include_provenance("publisher"));
        assert!(!args.should_include_provenance("crossref"));
        assert!(args.should_include_provenance("mined"));

        // Filter to publisher only
        args.provenance = vec!["publisher".to_string()];
        assert!(args.should_include_provenance("publisher"));
        assert!(!args.should_include_provenance("crossref"));
        assert!(!args.should_include_provenance("mined"));

        // Filter to multiple
        args.provenance = vec!["publisher".to_string(), "crossref".to_string()];
        assert!(args.should_include_provenance("publisher"));
        assert!(args.should_include_provenance("crossref"));
        assert!(!args.should_include_provenance("mined"));
    }

    #[test]
    fn test_extract_arxiv_as_dois() {
        // Test basic arXiv extraction and DOI conversion
        let text = "See arXiv:2403.12345 for details";
        let results = extract_arxiv_as_dois(text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "10.48550/arXiv.2403.12345");

        // Test with URL format
        let text = "Available at https://arxiv.org/abs/2312.00001";
        let results = extract_arxiv_as_dois(text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "10.48550/arXiv.2312.00001");

        // Test with no arXiv
        let text = "Just a regular DOI 10.1234/test";
        let results = extract_arxiv_as_dois(text);
        assert!(results.is_empty());
    }

    fn entry(json: &str) -> CrossrefEntry {
        CrossrefEntry {
            filename: "0.json".to_string(),
            bytes: json.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_extract_file_rows_and_stats() {
        let e = entry(
            r#"{"items": [
                {"DOI": "10.1111/Citing", "reference": [
                    {"unstructured": "See arXiv:2403.12345 for details"},
                    "not an object",
                    {"unstructured": "arXiv preprint without an identifier"},
                    {"DOI": "10.48550/arXiv.2403.67890", "doi-asserted-by": "publisher"}
                ]},
                {"reference": [{"unstructured": "arXiv:2403.11111"}]}
            ]}"#,
        );
        let mut buf = String::new();
        let result = extract_file(&e, &[], &mut buf);

        assert!(!result.parse_failed);
        assert_eq!(result.filename, "0.json");
        assert_eq!(result.stats.items, 2);
        assert_eq!(result.stats.extracted, 2);
        assert_eq!(
            result.stats.refs_unparseable, 1,
            "the bare string reference"
        );
        assert_eq!(result.stats.no_match, 1);
        assert_eq!(result.stats.filtered_provenance, 0);
        assert_eq!(result.stats.filtered_self_cite, 0);

        // Rows stay in file order, and the DOI-less item contributes none.
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].citing_doi, "10.1111/citing");
        assert_eq!(result.rows[0].ref_index, 0);
        assert_eq!(result.rows[0].cited_id, "10.48550/arXiv.2403.12345");
        assert_eq!(result.rows[0].provenance, "mined");
        assert_eq!(result.rows[1].ref_index, 3);
        assert_eq!(result.rows[1].cited_id, "10.48550/arXiv.2403.67890");
        assert_eq!(result.rows[1].provenance, "publisher");
    }

    #[test]
    fn test_extract_file_filters_self_cites_and_provenance() {
        let e = entry(
            r#"{"items": [{"DOI": "10.48550/arXiv.2403.99999", "reference": [
                {"unstructured": "See arXiv:2403.99999"},
                {"DOI": "10.48550/arXiv.2403.67890", "doi-asserted-by": "crossref"}
            ]}]}"#,
        );
        let mut buf = String::new();
        let filter = vec!["publisher".to_string()];
        let result = extract_file(&e, &filter, &mut buf);

        assert_eq!(result.stats.filtered_self_cite, 1);
        assert_eq!(result.stats.filtered_provenance, 1);
        assert_eq!(result.stats.extracted, 0);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_extract_file_marks_parse_failure() {
        let mut buf = String::new();
        let result = extract_file(&entry("{not json"), &[], &mut buf);

        assert!(result.parse_failed);
        assert!(result.rows.is_empty());
        assert_eq!(result.stats.items, 0);
    }

    #[test]
    fn test_commit_progress_flushes_before_marking() {
        use crate::streaming::{ExtractionCheckpoint, PartitionRow, SegmentedPartitionWriter};
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        // Threshold high enough that nothing auto-flushes.
        let mut writer = SegmentedPartitionWriter::new(&partition_dir, 1_000_000).unwrap();
        writer
            .write(PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: 0,
                cited_id: "10.48550/arxiv.2403.00001".to_string(),
                provenance: "mined".to_string(),
                match_type: MatchType::Arxiv,
                raw_match: "arXiv:2403.99999".to_string(),
                ref_json: "{}".to_string(),
            })
            .unwrap();
        assert_eq!(writer.total_rows_written(), 0, "row is only buffered");

        let cp_path = partition_dir.join("extraction.checkpoint");
        let mut checkpoint = Some(ExtractionCheckpoint::new(&cp_path).unwrap());
        let mut pending = vec!["file1.json".to_string()];

        commit_progress(&mut writer, &mut checkpoint, &mut pending).unwrap();

        assert_eq!(writer.total_rows_written(), 1, "buffered row hit disk");
        assert!(pending.is_empty());
        // Reload checkpoint from disk: the mark must be durable.
        drop(checkpoint);
        let reloaded = ExtractionCheckpoint::new(&cp_path).unwrap();
        assert!(reloaded.is_completed("file1.json"));
    }

    #[test]
    fn test_extract_file_captures_raw_match() {
        let body = serde_json::json!({
            "items": [{
                "DOI": "10.1234/citing",
                "reference": [
                    {"key": "r1", "unstructured": "See arXiv:2403.03542 for details"}
                ]
            }]
        });
        let entry = CrossrefEntry {
            filename: "t.json".to_string(),
            bytes: serde_json::to_vec(&body).unwrap(),
        };
        let mut scratch = String::new();
        let result = extract_file(&entry, &[], &mut scratch);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].raw_match, "arXiv:2403.03542");
    }
}
