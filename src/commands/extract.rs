use anyhow::{Context, Result};
use log::info;
use polars::prelude::*;
use std::path::Path;
use std::time::Instant;

use crate::cli::ExtractArgs;
use crate::common::{format_elapsed, setup_logging, ExtractStats};

const ARXIV_PATTERN: &str = r"(?i)(?:arxiv[.:\s]+(?:\d{4}\.\d{4,6}(?:v\d+)?|[a-z][a-z0-9.-]*/\s*\d{7}(?:v\d+)?)|10\.48550/arxiv\.\d{4}\.\d{4,6}(?:v\d+)?|arxiv\.org/(?:abs|pdf)/(?:\d{4}\.\d{4,6}(?:v\d+)?|[a-z][a-z0-9.-]*/\d{7}(?:v\d+)?))";

const ID_STRIP_PATTERN: &str = r"(?i)^(?:arxiv\.org/(?:abs|pdf)/|10\.48550/arxiv\.|arxiv[.:\s]+)";

pub fn run_extract(args: ExtractArgs) -> Result<ExtractStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Polars Vectorized arXiv Extraction");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    info!("Scanning Parquet file...");

    let lf = LazyFrame::scan_parquet(&args.input, Default::default())
        .context("Failed to scan Parquet file")?;

    let initial_count = lf
        .clone()
        .select([col("citing_doi").count().alias("count")])
        .collect()?
        .column("count")?
        .u32()?
        .get(0)
        .unwrap_or(0) as usize;

    info!("Total references in input: {}", initial_count);

    info!("Applying vectorized extraction pipeline...");

    let extracted = lf
        .filter(col("has_arxiv_hint").eq(lit(true)))
        .with_columns([
            col("ref_unstructured")
                .str()
                .extract_all(lit(ARXIV_PATTERN))
                .alias("unstructured_matches"),
            col("ref_doi")
                .str()
                .extract_all(lit(ARXIV_PATTERN))
                .alias("doi_matches"),
            col("ref_url")
                .str()
                .extract_all(lit(ARXIV_PATTERN))
                .alias("url_matches"),
            col("ref_article_title")
                .str()
                .extract_all(lit(ARXIV_PATTERN))
                .alias("title_matches"),
            col("ref_journal_title")
                .str()
                .extract_all(lit(ARXIV_PATTERN))
                .alias("journal_matches"),
        ])
        // fill_null with empty list before concat_list, otherwise concat_list returns NULL if ANY input is NULL
        .with_columns([concat_list([
            col("unstructured_matches").fill_null(lit(Series::from_any_values(PlSmallStr::EMPTY, &[AnyValue::List(Series::new_empty(PlSmallStr::EMPTY, &DataType::String))], false).unwrap())),
            col("doi_matches").fill_null(lit(Series::from_any_values(PlSmallStr::EMPTY, &[AnyValue::List(Series::new_empty(PlSmallStr::EMPTY, &DataType::String))], false).unwrap())),
            col("url_matches").fill_null(lit(Series::from_any_values(PlSmallStr::EMPTY, &[AnyValue::List(Series::new_empty(PlSmallStr::EMPTY, &DataType::String))], false).unwrap())),
            col("title_matches").fill_null(lit(Series::from_any_values(PlSmallStr::EMPTY, &[AnyValue::List(Series::new_empty(PlSmallStr::EMPTY, &DataType::String))], false).unwrap())),
            col("journal_matches").fill_null(lit(Series::from_any_values(PlSmallStr::EMPTY, &[AnyValue::List(Series::new_empty(PlSmallStr::EMPTY, &DataType::String))], false).unwrap())),
        ])?
        .alias("all_matches")])
        .with_columns([col("all_matches")
            .list()
            .eval(col("").drop_nulls(), false)
            .alias("raw_matches")])
        .filter(col("raw_matches").list().len().gt(lit(0)))
        .with_columns([col("raw_matches")
            .list()
            .eval(
                col("")
                    .str()
                    .replace(lit(ID_STRIP_PATTERN), lit(""), false)
                    .str()
                    .to_lowercase()
                    .str()
                    .replace_all(lit(r"\s+"), lit(""), false)
                    .str()
                    .replace(lit(r"v\d+$"), lit(""), false),
                false,
            )
            .alias("normalized_arxiv_ids")])
        .select([
            col("citing_doi"),
            col("ref_index"),
            col("ref_json"),
            col("raw_matches"),
            col("normalized_arxiv_ids"),
        ]);

    info!("Collecting results...");

    let result_df = extracted.collect().context("Failed to collect results")?;

    let references_with_matches = result_df.height();

    let total_arxiv_ids: usize = result_df
        .column("normalized_arxiv_ids")?
        .list()?
        .into_iter()
        .filter_map(|opt_series| opt_series.map(|s| s.len()))
        .sum();

    info!("Writing output Parquet...");

    let output_file = std::fs::File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;

    ParquetWriter::new(output_file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(250_000))
        .finish(&mut result_df.clone())?;

    let total_time = start_time.elapsed();

    let stats = ExtractStats {
        total_references: initial_count,
        references_with_matches,
        total_arxiv_ids,
    };

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("Total references scanned: {}", stats.total_references);
    info!(
        "References with arXiv matches: {} ({:.2}%)",
        stats.references_with_matches,
        100.0 * stats.references_with_matches as f64 / stats.total_references.max(1) as f64
    );
    info!("Total arXiv IDs extracted: {}", stats.total_arxiv_ids);
    info!("Output file: {}", args.output);
    info!("========================================================");

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_pattern_compiles() {
        let _ = regex::Regex::new(ARXIV_PATTERN).unwrap();
        let _ = regex::Regex::new(ID_STRIP_PATTERN).unwrap();
    }
}
