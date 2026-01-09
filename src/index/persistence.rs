use anyhow::{Context, Result};
use log::info;
use polars::prelude::*;
use std::fs::File;
use std::time::Instant;

use super::DoiIndex;
use crate::common::format_elapsed;

/// Save a DOI index to a Parquet file
pub fn save_index_to_parquet(index: &DoiIndex, path: &str) -> Result<()> {
    info!("Saving DOI index to: {}", path);
    let start = Instant::now();

    let dois: Vec<&str> = index.dois.iter().map(|s| s.as_str()).collect();
    let prefixes: Vec<&str> = index.prefixes.iter().map(|s| s.as_str()).collect();

    // Create two dataframes and save to same file using row groups
    let mut dois_df = DataFrame::new(vec![Column::new("doi".into(), &dois)])?;

    let mut prefixes_df = DataFrame::new(vec![Column::new("prefix".into(), &prefixes)])?;

    // Save DOIs
    let file = File::create(path).with_context(|| format!("Failed to create file: {}", path))?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut dois_df)
        .context("Failed to write DOIs to parquet")?;

    // Save prefixes to separate file
    let prefix_path = format!("{}.prefixes", path);
    let prefix_file = File::create(&prefix_path)
        .with_context(|| format!("Failed to create prefix file: {}", prefix_path))?;

    ParquetWriter::new(prefix_file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut prefixes_df)
        .context("Failed to write prefixes to parquet")?;

    info!(
        "Saved {} DOIs and {} prefixes in {}",
        index.len(),
        index.prefix_count(),
        format_elapsed(start.elapsed())
    );

    Ok(())
}

/// Load a DOI index from a Parquet file
pub fn load_index_from_parquet(path: &str) -> Result<DoiIndex> {
    info!("Loading DOI index from: {}", path);
    let start = Instant::now();

    let mut index = DoiIndex::new();

    // Load DOIs
    let dois_df = LazyFrame::scan_parquet(path, Default::default())
        .with_context(|| format!("Failed to scan parquet: {}", path))?
        .collect()
        .context("Failed to collect DOIs dataframe")?;

    let dois_col = dois_df.column("doi")?.str()?;
    for doi in dois_col.into_iter().flatten() {
        index.dois.insert(doi.to_string());
    }

    // Load prefixes
    let prefix_path = format!("{}.prefixes", path);
    if std::path::Path::new(&prefix_path).exists() {
        let prefixes_df = LazyFrame::scan_parquet(&prefix_path, Default::default())
            .with_context(|| format!("Failed to scan prefix parquet: {}", prefix_path))?
            .collect()
            .context("Failed to collect prefixes dataframe")?;

        let prefixes_col = prefixes_df.column("prefix")?.str()?;
        for prefix in prefixes_col.into_iter().flatten() {
            index.prefixes.insert(prefix.to_string());
        }
    } else {
        // Rebuild prefixes from DOIs if prefix file missing
        for doi in &index.dois {
            if let Some(prefix) = crate::extract::doi_prefix(doi) {
                index.prefixes.insert(prefix);
            }
        }
    }

    info!(
        "Loaded {} DOIs and {} prefixes in {}",
        index.len(),
        index.prefix_count(),
        format_elapsed(start.elapsed())
    );

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_load_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_index.parquet");
        let path_str = path.to_str().unwrap();

        let mut index = DoiIndex::new();
        index.insert("10.1234/example1");
        index.insert("10.1234/example2");
        index.insert("10.5678/other");

        save_index_to_parquet(&index, path_str).unwrap();

        let loaded = load_index_from_parquet(path_str).unwrap();

        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains("10.1234/example1"));
        assert!(loaded.contains("10.5678/other"));
        assert_eq!(loaded.prefix_count(), 2);
        assert!(loaded.has_prefix("10.1234"));
    }
}
