use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use super::DoiIndex;
use crate::common::format_elapsed;

/// Build a DOI index from a gzipped JSONL file containing records with "id" field
pub fn build_index_from_jsonl_gz(path: &str, id_field: &str) -> Result<DoiIndex> {
    info!("Building DOI index from: {}", path);
    let start = Instant::now();

    let file = File::open(path).with_context(|| format!("Failed to open file: {}", path))?;

    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut index = DoiIndex::with_capacity(10_000_000, 100_000);
    let mut lines_processed = 0;
    let mut lines_failed = 0;

    for line_result in reader.lines() {
        let line = line_result.context("Failed to read line")?;

        if line.trim().is_empty() {
            continue;
        }

        // Parse just enough to get the ID field
        match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(record) => {
                if let Some(id) = record.get(id_field).and_then(|v| v.as_str()) {
                    index.insert(id);
                }
            }
            Err(_) => {
                lines_failed += 1;
            }
        }

        lines_processed += 1;
        if lines_processed % 500_000 == 0 {
            info!(
                "  Processed {} records, {} DOIs indexed...",
                lines_processed,
                index.len()
            );
        }
    }

    info!(
        "Built index with {} DOIs ({} prefixes) from {} records in {}",
        index.len(),
        index.prefix_count(),
        lines_processed,
        format_elapsed(start.elapsed())
    );

    if lines_failed > 0 {
        info!("  ({} records failed to parse)", lines_failed);
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_jsonl_gz(records: &[&str]) -> NamedTempFile {
        let file = NamedTempFile::new().unwrap();
        let encoder = GzEncoder::new(file.reopen().unwrap(), Compression::default());
        let mut writer = std::io::BufWriter::new(encoder);

        for record in records {
            writeln!(writer, "{}", record).unwrap();
        }
        writer.into_inner().unwrap().finish().unwrap();

        file
    }

    #[test]
    fn test_build_index_from_jsonl_gz() {
        let file = create_test_jsonl_gz(&[
            r#"{"id": "10.1234/example1"}"#,
            r#"{"id": "10.1234/example2"}"#,
            r#"{"id": "10.5678/other"}"#,
        ]);

        let index = build_index_from_jsonl_gz(file.path().to_str().unwrap(), "id").unwrap();

        assert_eq!(index.len(), 3);
        assert!(index.contains("10.1234/example1"));
        assert!(index.contains("10.5678/other"));
        assert_eq!(index.prefix_count(), 2);
    }
}
