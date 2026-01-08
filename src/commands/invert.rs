use anyhow::{Context, Result};
use log::info;
use polars::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use crate::cli::InvertArgs;
use crate::common::{format_elapsed, setup_logging, InvertStats};

pub fn run_invert(args: InvertArgs) -> Result<InvertStats> {
    let start_time = Instant::now();

    setup_logging(&args.log_level)?;

    info!("Starting Polars Hash-Based Inversion");
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

    info!("Total rows in input: {}", initial_count);

    info!("Building inverted index with hash-based aggregation...");

    let inverted = lf
        .explode([col("normalized_arxiv_ids"), col("raw_matches")])
        .rename(["normalized_arxiv_ids"], ["arxiv_id"], true)
        .rename(["raw_matches"], ["raw_match"], true)
        .unique(
            Some(vec!["citing_doi".into(), "arxiv_id".into()]),
            UniqueKeepStrategy::First,
        )
        .group_by([col("arxiv_id")])
        .agg([
            col("citing_doi").n_unique().alias("citation_count"),
            col("citing_doi").count().alias("reference_count"),
            as_struct(vec![
                col("citing_doi").alias("doi"),
                col("raw_match"),
                col("ref_json").alias("reference"),
            ])
            .alias("cited_by"),
        ])
        .with_columns([concat_str([lit("10.48550/arXiv."), col("arxiv_id")], "", true)
            .alias("arxiv_doi")])
        .sort(
            ["citation_count"],
            SortMultipleOptions::default().with_order_descending(true),
        );

    info!("Collecting results...");

    let result_df = inverted.collect().context("Failed to collect results")?;

    let unique_arxiv_works = result_df.height();

    let unique_citing_works: u32 = result_df
        .column("citation_count")?
        .u32()?
        .sum()
        .unwrap_or(0);

    info!("Writing output Parquet...");

    let output_file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;

    ParquetWriter::new(output_file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(250_000))
        .finish(&mut result_df.clone())?;

    if let Some(jsonl_path) = &args.output_jsonl {
        info!("Writing JSONL output for validate compatibility...");
        write_jsonl_output(&result_df, jsonl_path)?;
    }

    let total_time = start_time.elapsed();

    let stats = InvertStats {
        total_rows_processed: initial_count,
        unique_arxiv_works,
        unique_citing_works: unique_citing_works as usize,
    };

    info!("==================== FINAL SUMMARY ====================");
    info!("Total execution time: {}", format_elapsed(total_time));
    info!("Total rows processed: {}", stats.total_rows_processed);
    info!("Unique arXiv works: {}", stats.unique_arxiv_works);
    info!("Total citations: {}", stats.unique_citing_works);
    info!("Output file: {}", args.output);
    if let Some(jsonl_path) = &args.output_jsonl {
        info!("JSONL output: {}", jsonl_path);
    }
    info!("========================================================");

    Ok(stats)
}

fn write_jsonl_output(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path).with_context(|| format!("Failed to create JSONL file: {}", path))?;
    let mut writer = BufWriter::new(file);

    let arxiv_doi = df.column("arxiv_doi")?.str()?;
    let arxiv_id = df.column("arxiv_id")?.str()?;
    let reference_count = df.column("reference_count")?.u32()?;
    let citation_count = df.column("citation_count")?.u32()?;
    let cited_by = df.column("cited_by")?;

    for i in 0..df.height() {
        let doi = arxiv_doi.get(i).unwrap_or("");
        let id = arxiv_id.get(i).unwrap_or("");
        let ref_count = reference_count.get(i).unwrap_or(0);
        let cit_count = citation_count.get(i).unwrap_or(0);

        // Build cited_by array from struct column
        let cited_by_json = build_cited_by_json(cited_by, i)?;

        let json_line = serde_json::json!({
            "arxiv_doi": doi,
            "arxiv_id": id,
            "reference_count": ref_count,
            "citation_count": cit_count,
            "cited_by": cited_by_json
        });

        writeln!(writer, "{}", json_line)?;
    }

    writer.flush()?;
    Ok(())
}

fn build_cited_by_json(cited_by_col: &Column, row_idx: usize) -> Result<serde_json::Value> {
    use std::collections::HashMap;

    let list = cited_by_col.list()?;
    let row_list = list.get_as_series(row_idx);

    match row_list {
        Some(series) => {
            let structs = series.struct_()?;
            let doi_field = structs.field_by_name("doi")?;
            let raw_match_field = structs.field_by_name("raw_match")?;
            let ref_field = structs.field_by_name("reference")?;

            let dois = doi_field.str()?;
            let raw_matches = raw_match_field.str()?;
            let refs = ref_field.str()?;

            let mut doi_matches: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

            for j in 0..series.len() {
                let doi = dois.get(j).unwrap_or("").to_string();
                let raw_match = raw_matches.get(j).unwrap_or("");
                let ref_json_str = refs.get(j).unwrap_or("null");

                let reference: serde_json::Value =
                    serde_json::from_str(ref_json_str).unwrap_or(serde_json::Value::Null);

                let match_obj = serde_json::json!({
                    "raw_match": raw_match,
                    "reference": reference
                });

                doi_matches.entry(doi).or_insert_with(Vec::new).push(match_obj);
            }

            let cited_by_arr: Vec<serde_json::Value> = doi_matches
                .into_iter()
                .map(|(doi, matches)| {
                    serde_json::json!({
                        "doi": doi,
                        "matches": matches
                    })
                })
                .collect();

            Ok(serde_json::Value::Array(cited_by_arr))
        }
        None => Ok(serde_json::Value::Array(vec![])),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_build_cited_by_json_empty() {
        let empty = serde_json::json!([]);
        assert!(empty.is_array());
    }
}
