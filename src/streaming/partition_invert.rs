use anyhow::{Context, Result};
use log::{debug, info};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::Checkpoint;

/// Statistics from inverting partitions
#[derive(Debug, Clone, Default)]
pub struct InvertStats {
    pub partitions_processed: usize,
    pub unique_arxiv_works: usize,
    pub total_citations: usize,
}

/// Invert a single partition file
///
/// Each partition file contains rows with (citing_doi, ref_index, ref_json, raw_match, arxiv_id).
/// This function groups by arxiv_id and aggregates to produce the inverted index.
fn invert_single_partition(partition_path: &Path) -> Result<DataFrame> {
    debug!("Inverting partition: {:?}", partition_path);

    let lf = LazyFrame::scan_parquet(partition_path, Default::default())
        .with_context(|| format!("Failed to scan partition: {:?}", partition_path))?;

    // Group by arxiv_id, aggregating citations
    // Note: rows are already exploded (one row per arxiv_id per reference)
    let inverted = lf
        // Deduplicate (same citing_doi + arxiv_id should only count once)
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
        .with_columns([
            concat_str([lit("10.48550/arXiv."), col("arxiv_id")], "", true)
                .alias("arxiv_doi"),
        ]);

    inverted.collect()
        .with_context(|| format!("Failed to collect inverted partition: {:?}", partition_path))
}

/// Invert all partition files in parallel
pub fn invert_partitions(
    partition_dir: &Path,
    output_parquet: &Path,
    output_jsonl: Option<&Path>,
    checkpoint: &mut Checkpoint,
) -> Result<InvertStats> {
    // Find all partition files
    let partition_files: Vec<_> = fs::read_dir(partition_dir)
        .with_context(|| format!("Failed to read partition directory: {:?}", partition_dir))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().map_or(false, |ext| ext == "parquet"))
        .filter(|path| {
            // Skip already-inverted partitions (from checkpoint)
            let name = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            !checkpoint.is_partition_inverted(name)
        })
        .collect();

    info!("Inverting {} partitions in parallel", partition_files.len());

    // Process partitions in parallel
    let results: Vec<Result<(String, DataFrame)>> = partition_files
        .par_iter()
        .map(|path| {
            let df = invert_single_partition(path)?;
            let name = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            Ok((name, df))
        })
        .collect();

    // Collect successful results and track which partitions completed
    let mut dfs = Vec::new();
    for result in results {
        match result {
            Ok((name, df)) => {
                checkpoint.mark_partition_inverted(&name);
                dfs.push(df);
            }
            Err(e) => {
                return Err(e.context("Failed to invert partition"));
            }
        }
    }

    if dfs.is_empty() {
        info!("No partitions to invert (all already processed or none found)");
        return Ok(InvertStats::default());
    }

    info!("Concatenating {} inverted partitions", dfs.len());

    // Concatenate all dataframes
    let lazy_dfs: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
    let mut combined = concat(&lazy_dfs, UnionArgs::default())
        .context("Failed to concatenate inverted partitions")?
        .sort(
            ["citation_count"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()
        .context("Failed to collect combined dataframe")?;

    let unique_arxiv_works = combined.height();
    let total_citations: u32 = combined
        .column("citation_count")?
        .u32()?
        .sum()
        .unwrap_or(0);

    info!("Writing inverted output: {} unique arXiv works", unique_arxiv_works);

    // Write Parquet output
    let file = File::create(output_parquet)
        .with_context(|| format!("Failed to create output file: {:?}", output_parquet))?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(250_000))
        .finish(&mut combined)
        .context("Failed to write output parquet")?;

    // Write JSONL output if requested
    if let Some(jsonl_path) = output_jsonl {
        write_jsonl_output(&combined, jsonl_path)?;
    }

    let stats = InvertStats {
        partitions_processed: partition_files.len(),
        unique_arxiv_works,
        total_citations: total_citations as usize,
    };

    Ok(stats)
}

/// Write DataFrame to JSONL format for validate step compatibility
fn write_jsonl_output(df: &DataFrame, path: &Path) -> Result<()> {
    info!("Writing JSONL output: {:?}", path);

    let file = File::create(path)
        .with_context(|| format!("Failed to create JSONL file: {:?}", path))?;
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

/// Build cited_by JSON array from struct column
fn build_cited_by_json(cited_by_col: &Column, row_idx: usize) -> Result<serde_json::Value> {
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

                doi_matches.entry(doi).or_default().push(match_obj);
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
    use super::*;
    use tempfile::tempdir;

    fn create_test_partition(dir: &Path, name: &str, rows: Vec<(&str, u32, &str, &str, &str)>) -> Result<()> {
        let citing_dois: Vec<String> = rows.iter().map(|r| r.0.to_string()).collect();
        let ref_indices: Vec<u32> = rows.iter().map(|r| r.1).collect();
        let ref_jsons: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
        let raw_matches: Vec<String> = rows.iter().map(|r| r.3.to_string()).collect();
        let arxiv_ids: Vec<String> = rows.iter().map(|r| r.4.to_string()).collect();

        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), &citing_dois),
            Column::new("ref_index".into(), &ref_indices),
            Column::new("ref_json".into(), &ref_jsons),
            Column::new("raw_match".into(), &raw_matches),
            Column::new("arxiv_id".into(), &arxiv_ids),
        ])?;

        let file = File::create(dir.join(format!("{}.parquet", name)))?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }

    #[test]
    fn test_invert_single_partition() {
        let dir = tempdir().unwrap();

        create_test_partition(dir.path(), "2403", vec![
            ("10.1234/a", 0, "{}", "arXiv:2403.12345", "2403.12345"),
            ("10.1234/b", 1, "{}", "arXiv:2403.12345", "2403.12345"),
            ("10.1234/a", 2, "{}", "arXiv:2403.67890", "2403.67890"),
        ]).unwrap();

        let df = invert_single_partition(&dir.path().join("2403.parquet")).unwrap();

        assert_eq!(df.height(), 2); // Two unique arxiv_ids

        let arxiv_ids: Vec<_> = df.column("arxiv_id").unwrap()
            .str().unwrap()
            .into_iter()
            .filter_map(|s| s.map(|s| s.to_string()))
            .collect();

        assert!(arxiv_ids.contains(&"2403.12345".to_string()));
        assert!(arxiv_ids.contains(&"2403.67890".to_string()));
    }
}
