# DOI Extraction Expansion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expand the CLI from arXiv-only to multi-source DOI extraction and validation (Crossref, DataCite, arXiv).

**Architecture:** Single-pass streaming through input tar.gz extracts DOIs while building Crossref index. DOIs are partitioned by prefix, inverted in parallel, then validated against source-specific indexes with optional HTTP fallback. Indexes can be persisted to Parquet for reuse.

**Tech Stack:** Rust, clap (CLI), polars (DataFrame/Parquet), tokio/reqwest (async HTTP), regex (DOI patterns), rayon (parallelism)

---

## Task 1: Rename Package and Update Cargo.toml

**Files:**
- Modify: `Cargo.toml:1-6`

**Step 1: Update package metadata**

Change the package name and description in `Cargo.toml`:

```toml
[package]
name = "crossref-citation-extraction"
version = "2.0.0"
edition = "2021"
description = "CLI for extracting, inverting, and validating DOI references from Crossref data"
```

**Step 2: Build to verify compilation**

Run: `cargo build`
Expected: Build succeeds with new binary name

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: rename package to crossref-citation-extraction"
```

---

## Task 2: Add Source Enum and Update CLI Types

**Files:**
- Modify: `src/cli.rs`

**Step 1: Add Source enum at top of file**

After the imports, add:

```rust
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Source {
    #[default]
    All,
    Crossref,
    Datacite,
    Arxiv,
}

impl FromStr for Source {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" => Ok(Source::All),
            "crossref" => Ok(Source::Crossref),
            "datacite" => Ok(Source::Datacite),
            "arxiv" => Ok(Source::Arxiv),
            _ => Err(format!("Invalid source: {}. Valid options: all, crossref, datacite, arxiv", s)),
        }
    }
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::All => write!(f, "all"),
            Source::Crossref => write!(f, "crossref"),
            Source::Datacite => write!(f, "datacite"),
            Source::Arxiv => write!(f, "arxiv"),
        }
    }
}
```

**Step 2: Build to verify compilation**

Run: `cargo build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): add Source enum for extraction modes"
```

---

## Task 3: Update PipelineArgs with New Flags

**Files:**
- Modify: `src/cli.rs:28-69` (PipelineArgs struct)

**Step 1: Replace PipelineArgs with new structure**

Replace the entire `PipelineArgs` struct:

```rust
#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file for validation
    #[arg(long)]
    pub datacite_records: Option<String>,

    /// Source to extract: all, crossref, datacite, arxiv
    #[arg(long, default_value = "all")]
    pub source: Source,

    /// Output file for Crossref citations (JSONL)
    #[arg(long)]
    pub output_crossref: Option<String>,

    /// Output file for DataCite citations (JSONL)
    #[arg(long)]
    pub output_datacite: Option<String>,

    /// Output file for arXiv citations (JSONL, arxiv mode only)
    #[arg(long)]
    pub output_arxiv: Option<String>,

    /// Output file for failed Crossref validations
    #[arg(long)]
    pub output_crossref_failed: Option<String>,

    /// Output file for failed DataCite validations
    #[arg(long)]
    pub output_datacite_failed: Option<String>,

    /// Output file for failed arXiv validations
    #[arg(long)]
    pub output_arxiv_failed: Option<String>,

    /// Enable HTTP fallback for specified sources (comma-separated: crossref,datacite)
    #[arg(long, value_delimiter = ',')]
    pub http_fallback: Vec<String>,

    /// Load Crossref DOI index from Parquet file
    #[arg(long)]
    pub load_crossref_index: Option<String>,

    /// Save Crossref DOI index to Parquet file
    #[arg(long)]
    pub save_crossref_index: Option<String>,

    /// Load DataCite DOI index from Parquet file
    #[arg(long)]
    pub load_datacite_index: Option<String>,

    /// Save DataCite DOI index to Parquet file
    #[arg(long)]
    pub save_datacite_index: Option<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,

    /// Keep intermediate files (partitions, temp parquet)
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate partition files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,

    /// Batch size for memory management during streaming
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,
}
```

**Step 2: Build to verify compilation**

Run: `cargo build`
Expected: Build fails (pipeline.rs uses old args) - this is expected, we'll fix in later tasks

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): update PipelineArgs with multi-source flags"
```

---

## Task 4: Update CLI Metadata and ValidateArgs

**Files:**
- Modify: `src/cli.rs:1-27` (top section and Commands enum)
- Modify: `src/cli.rs` (ValidateArgs)

**Step 1: Update CLI metadata and Commands**

Update the top of cli.rs:

```rust
use clap::{Parser, Subcommand};
use std::str::FromStr;

#[derive(Parser)]
#[command(name = "crossref-citation-extraction")]
#[command(about = "Extract, invert, and validate DOI references from Crossref data")]
#[command(version = "2.0.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the full pipeline: extract DOIs, invert by cited work, validate
    ///
    /// Streams through the Crossref tar.gz archive, extracts DOI references,
    /// partitions by DOI prefix, inverts in parallel, and validates against
    /// source-specific records.
    Pipeline(PipelineArgs),

    /// Validate citations against records without re-running extraction
    Validate(ValidateArgs),
}
```

**Step 2: Update ValidateArgs**

Replace ValidateArgs:

```rust
#[derive(Parser, Clone)]
pub struct ValidateArgs {
    /// Input citations JSONL file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file (for datacite/arxiv validation)
    #[arg(long)]
    pub datacite_records: Option<String>,

    /// Crossref DOI index Parquet file (for crossref validation)
    #[arg(long)]
    pub crossref_index: Option<String>,

    /// Source type of the input file: crossref, datacite, arxiv
    #[arg(long, required = true)]
    pub source: Source,

    /// Output file for valid citations
    #[arg(long, required = true)]
    pub output_valid: String,

    /// Output file for failed citations
    #[arg(long, required = true)]
    pub output_failed: String,

    /// Enable HTTP fallback validation
    #[arg(long, default_value = "false")]
    pub http_fallback: bool,

    /// Concurrent HTTP requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per request
    #[arg(short, long, default_value = "5")]
    pub timeout: u64,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}
```

**Step 3: Build to verify syntax**

Run: `cargo check`
Expected: Errors about pipeline.rs/validate.rs using old args - expected for now

**Step 4: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): complete CLI restructure for multi-source support"
```

---

## Task 5: Create DOI Extraction Module - Types and Patterns

**Files:**
- Create: `src/extract/doi.rs`
- Modify: `src/extract/mod.rs`

**Step 1: Write test for DOI extraction**

Create `src/extract/doi.rs`:

```rust
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;

lazy_static! {
    /// DOI pattern - captures DOI from various formats
    /// Matches: bare DOI, doi:prefix, URL forms
    pub static ref DOI_PATTERN: Regex = Regex::new(
        r"(?i)(?:doi[:\s]*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^\s\]\)>,;\"']+)"
    ).unwrap();
}

/// Represents a matched DOI with raw match text and normalized form
#[derive(Debug, Clone, PartialEq)]
pub struct DoiMatch {
    pub doi: String,      // Normalized DOI (lowercase, cleaned)
    pub raw: String,      // Original matched substring
}

impl DoiMatch {
    pub fn new(doi: String, raw: String) -> Self {
        Self { doi, raw }
    }
}

/// Clean up a captured DOI string
/// - Strip trailing punctuation
/// - Decode URL-encoded characters
/// - Normalize to lowercase
pub fn normalize_doi(doi: &str) -> String {
    let mut result = doi.to_string();

    // Decode common URL-encoded characters
    result = result
        .replace("%2F", "/")
        .replace("%2f", "/")
        .replace("%3A", ":")
        .replace("%3a", ":")
        .replace("%28", "(")
        .replace("%29", ")")
        .replace("%3C", "<")
        .replace("%3c", "<")
        .replace("%3E", ">")
        .replace("%3e", ">");

    // Strip trailing punctuation that's likely not part of the DOI
    let trailing_chars: &[char] = &['.', ',', ';', ':', ')', ']', '>', '"', '\'', ' '];
    while result.ends_with(trailing_chars) {
        result.pop();
    }

    // Strip trailing HTML entities
    for entity in &["&gt", "&lt", "&amp", "&quot"] {
        if result.ends_with(entity) {
            result = result[..result.len() - entity.len()].to_string();
        }
    }

    result.to_lowercase()
}

/// Extract DOI matches from text
pub fn extract_doi_matches_from_text(text: &str) -> Vec<DoiMatch> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut matches = Vec::new();

    for cap in DOI_PATTERN.captures_iter(text) {
        if let Some(doi_match) = cap.get(1) {
            let raw = doi_match.as_str().to_string();
            let normalized = normalize_doi(&raw);

            // Skip if we've already seen this normalized DOI
            if seen.insert(normalized.clone()) {
                matches.push(DoiMatch::new(normalized, raw));
            }
        }
    }

    matches
}

/// Extract DOI prefix (registrant code) from a DOI
pub fn doi_prefix(doi: &str) -> Option<String> {
    let parts: Vec<&str> = doi.splitn(2, '/').collect();
    if parts.len() == 2 && parts[0].starts_with("10.") {
        Some(parts[0].to_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bare_doi() {
        let text = "See 10.1234/example.paper for details";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].doi, "10.1234/example.paper");
    }

    #[test]
    fn test_extract_doi_with_prefix() {
        let text = "doi:10.1234/example";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].doi, "10.1234/example");
    }

    #[test]
    fn test_extract_doi_url() {
        let text = "https://doi.org/10.1234/example";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].doi, "10.1234/example");
    }

    #[test]
    fn test_extract_dx_doi_url() {
        let text = "http://dx.doi.org/10.1234/example";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].doi, "10.1234/example");
    }

    #[test]
    fn test_normalize_trailing_punctuation() {
        assert_eq!(normalize_doi("10.1234/test."), "10.1234/test");
        assert_eq!(normalize_doi("10.1234/test,"), "10.1234/test");
        assert_eq!(normalize_doi("10.1234/test)"), "10.1234/test");
        assert_eq!(normalize_doi("10.1234/test],"), "10.1234/test");
    }

    #[test]
    fn test_normalize_url_encoded() {
        assert_eq!(normalize_doi("10.1234%2Ftest"), "10.1234/test");
    }

    #[test]
    fn test_normalize_lowercase() {
        assert_eq!(normalize_doi("10.1234/TEST"), "10.1234/test");
    }

    #[test]
    fn test_doi_prefix() {
        assert_eq!(doi_prefix("10.1234/example"), Some("10.1234".to_string()));
        assert_eq!(doi_prefix("10.48550/arXiv.2403.12345"), Some("10.48550".to_string()));
        assert_eq!(doi_prefix("invalid"), None);
    }

    #[test]
    fn test_deduplicate_matches() {
        let text = "10.1234/test and also 10.1234/TEST";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_multiple_dois() {
        let text = "See 10.1234/first and 10.5678/second";
        let matches = extract_doi_matches_from_text(text);
        assert_eq!(matches.len(), 2);
    }
}
```

**Step 2: Update extract/mod.rs**

Replace `src/extract/mod.rs`:

```rust
pub mod doi;
pub mod patterns;

pub use doi::*;
pub use patterns::*;
```

**Step 3: Run tests to verify**

Run: `cargo test extract::doi`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/extract/doi.rs src/extract/mod.rs
git commit -m "feat(extract): add DOI extraction module with patterns and normalization"
```

---

## Task 6: Rename patterns.rs to arxiv.rs

**Files:**
- Rename: `src/extract/patterns.rs` -> `src/extract/arxiv.rs`
- Modify: `src/extract/mod.rs`

**Step 1: Rename file**

```bash
mv src/extract/patterns.rs src/extract/arxiv.rs
```

**Step 2: Update mod.rs**

```rust
pub mod arxiv;
pub mod doi;

pub use arxiv::*;
pub use doi::*;
```

**Step 3: Run tests to verify**

Run: `cargo test extract::`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/extract/
git commit -m "refactor(extract): rename patterns.rs to arxiv.rs for clarity"
```

---

## Task 7: Create Index Module - Types

**Files:**
- Create: `src/index/mod.rs`
- Modify: `src/main.rs`

**Step 1: Create index module with types**

Create `src/index/mod.rs`:

```rust
pub mod builder;
pub mod persistence;

use std::collections::HashSet;

/// DOI index containing DOIs and their prefixes for fast lookup
#[derive(Debug, Clone, Default)]
pub struct DoiIndex {
    /// Set of all DOIs (lowercase)
    pub dois: HashSet<String>,
    /// Set of all DOI prefixes (e.g., "10.1234")
    pub prefixes: HashSet<String>,
}

impl DoiIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(doi_capacity: usize, prefix_capacity: usize) -> Self {
        Self {
            dois: HashSet::with_capacity(doi_capacity),
            prefixes: HashSet::with_capacity(prefix_capacity),
        }
    }

    /// Add a DOI to the index, also tracking its prefix
    pub fn insert(&mut self, doi: &str) {
        let doi_lower = doi.to_lowercase();
        if let Some(prefix) = crate::extract::doi_prefix(&doi_lower) {
            self.prefixes.insert(prefix);
        }
        self.dois.insert(doi_lower);
    }

    /// Check if a DOI exists in the index
    pub fn contains(&self, doi: &str) -> bool {
        self.dois.contains(&doi.to_lowercase())
    }

    /// Check if a prefix exists in the index
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.prefixes.contains(&prefix.to_lowercase())
    }

    /// Get count of DOIs
    pub fn len(&self) -> usize {
        self.dois.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.dois.is_empty()
    }

    /// Get count of unique prefixes
    pub fn prefix_count(&self) -> usize {
        self.prefixes.len()
    }

    /// Merge another index into this one
    pub fn merge(&mut self, other: DoiIndex) {
        self.dois.extend(other.dois);
        self.prefixes.extend(other.prefixes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doi_index_insert_and_contains() {
        let mut index = DoiIndex::new();
        index.insert("10.1234/example");

        assert!(index.contains("10.1234/example"));
        assert!(index.contains("10.1234/EXAMPLE")); // Case insensitive
        assert!(!index.contains("10.5678/other"));
    }

    #[test]
    fn test_doi_index_prefix_tracking() {
        let mut index = DoiIndex::new();
        index.insert("10.1234/example1");
        index.insert("10.1234/example2");
        index.insert("10.5678/other");

        assert!(index.has_prefix("10.1234"));
        assert!(index.has_prefix("10.5678"));
        assert!(!index.has_prefix("10.9999"));
        assert_eq!(index.prefix_count(), 2);
    }

    #[test]
    fn test_doi_index_merge() {
        let mut index1 = DoiIndex::new();
        index1.insert("10.1234/a");

        let mut index2 = DoiIndex::new();
        index2.insert("10.5678/b");

        index1.merge(index2);

        assert!(index1.contains("10.1234/a"));
        assert!(index1.contains("10.5678/b"));
        assert_eq!(index1.len(), 2);
    }
}
```

**Step 2: Update main.rs to include index module**

Add to `src/main.rs` after other mod declarations:

```rust
mod index;
```

**Step 3: Create placeholder files**

Create `src/index/builder.rs`:

```rust
// Index building from streaming data - to be implemented
```

Create `src/index/persistence.rs`:

```rust
// Parquet save/load for indexes - to be implemented
```

**Step 4: Build to verify**

Run: `cargo build`
Expected: Build succeeds (with warnings about unused)

**Step 5: Run tests**

Run: `cargo test index::`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/index/ src/main.rs
git commit -m "feat(index): add DoiIndex type with prefix tracking"
```

---

## Task 8: Implement Index Builder

**Files:**
- Modify: `src/index/builder.rs`

**Step 1: Implement builder module**

Replace `src/index/builder.rs`:

```rust
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

    let file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path))?;

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
            info!("  Processed {} records, {} DOIs indexed...", lines_processed, index.len());
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
    use std::io::Write;
    use tempfile::NamedTempFile;
    use flate2::write::GzEncoder;
    use flate2::Compression;

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
```

**Step 2: Run tests**

Run: `cargo test index::builder`
Expected: All tests pass

**Step 3: Commit**

```bash
git add src/index/builder.rs
git commit -m "feat(index): implement index builder from JSONL.gz files"
```

---

## Task 9: Implement Index Persistence

**Files:**
- Modify: `src/index/persistence.rs`

**Step 1: Implement persistence module**

Replace `src/index/persistence.rs`:

```rust
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
    let mut dois_df = DataFrame::new(vec![
        Column::new("doi".into(), &dois),
    ])?;

    let mut prefixes_df = DataFrame::new(vec![
        Column::new("prefix".into(), &prefixes),
    ])?;

    // Save DOIs
    let file = File::create(path)
        .with_context(|| format!("Failed to create file: {}", path))?;

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
```

**Step 2: Update mod.rs exports**

Update `src/index/mod.rs` to add pub use statements:

```rust
pub mod builder;
pub mod persistence;

pub use builder::*;
pub use persistence::*;

// ... rest of file unchanged
```

**Step 3: Run tests**

Run: `cargo test index::persistence`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/index/
git commit -m "feat(index): implement Parquet save/load for DOI indexes"
```

---

## Task 10: Create Validation Module Structure

**Files:**
- Create: `src/validation/mod.rs`
- Create: `src/validation/prefix_filter.rs`
- Create: `src/validation/lookup.rs`
- Create: `src/validation/http.rs`
- Modify: `src/main.rs`

**Step 1: Create validation mod.rs**

Create `src/validation/mod.rs`:

```rust
pub mod http;
pub mod lookup;
pub mod prefix_filter;

pub use http::*;
pub use lookup::*;
pub use prefix_filter::*;

use crate::cli::Source;
use crate::index::DoiIndex;

/// Combined validation context for multi-source validation
pub struct ValidationContext {
    pub crossref_index: Option<DoiIndex>,
    pub datacite_index: Option<DoiIndex>,
    pub http_fallback_crossref: bool,
    pub http_fallback_datacite: bool,
    pub concurrency: usize,
    pub timeout_secs: u64,
}

impl ValidationContext {
    pub fn new() -> Self {
        Self {
            crossref_index: None,
            datacite_index: None,
            http_fallback_crossref: false,
            http_fallback_datacite: false,
            concurrency: 50,
            timeout_secs: 5,
        }
    }

    /// Check if we have the necessary indexes for a given source
    pub fn can_validate(&self, source: Source) -> bool {
        match source {
            Source::All => self.crossref_index.is_some() || self.datacite_index.is_some(),
            Source::Crossref => self.crossref_index.is_some() || self.http_fallback_crossref,
            Source::Datacite | Source::Arxiv => self.datacite_index.is_some() || self.http_fallback_datacite,
        }
    }
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self::new()
    }
}
```

**Step 2: Create prefix_filter.rs**

Create `src/validation/prefix_filter.rs`:

```rust
use crate::index::DoiIndex;
use crate::extract::doi_prefix;

/// Fast prefix-based filter for DOIs
/// Returns true if the DOI's prefix exists in any of the provided indexes
pub fn has_known_prefix(doi: &str, crossref: Option<&DoiIndex>, datacite: Option<&DoiIndex>) -> bool {
    let prefix = match doi_prefix(doi) {
        Some(p) => p,
        None => return false,
    };

    if let Some(idx) = crossref {
        if idx.has_prefix(&prefix) {
            return true;
        }
    }

    if let Some(idx) = datacite {
        if idx.has_prefix(&prefix) {
            return true;
        }
    }

    false
}

/// Determine which source(s) might contain a DOI based on prefix
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixMatch {
    None,
    Crossref,
    Datacite,
    Both,
}

pub fn prefix_source(doi: &str, crossref: Option<&DoiIndex>, datacite: Option<&DoiIndex>) -> PrefixMatch {
    let prefix = match doi_prefix(doi) {
        Some(p) => p,
        None => return PrefixMatch::None,
    };

    let in_crossref = crossref.map_or(false, |idx| idx.has_prefix(&prefix));
    let in_datacite = datacite.map_or(false, |idx| idx.has_prefix(&prefix));

    match (in_crossref, in_datacite) {
        (true, true) => PrefixMatch::Both,
        (true, false) => PrefixMatch::Crossref,
        (false, true) => PrefixMatch::Datacite,
        (false, false) => PrefixMatch::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_known_prefix() {
        let mut crossref = DoiIndex::new();
        crossref.insert("10.1234/example");

        let mut datacite = DoiIndex::new();
        datacite.insert("10.48550/arXiv.2403.12345");

        assert!(has_known_prefix("10.1234/other", Some(&crossref), Some(&datacite)));
        assert!(has_known_prefix("10.48550/arXiv.9999.99999", Some(&crossref), Some(&datacite)));
        assert!(!has_known_prefix("10.9999/unknown", Some(&crossref), Some(&datacite)));
    }

    #[test]
    fn test_prefix_source() {
        let mut crossref = DoiIndex::new();
        crossref.insert("10.1234/example");

        let mut datacite = DoiIndex::new();
        datacite.insert("10.48550/arXiv.2403.12345");

        assert_eq!(prefix_source("10.1234/x", Some(&crossref), Some(&datacite)), PrefixMatch::Crossref);
        assert_eq!(prefix_source("10.48550/x", Some(&crossref), Some(&datacite)), PrefixMatch::Datacite);
        assert_eq!(prefix_source("10.9999/x", Some(&crossref), Some(&datacite)), PrefixMatch::None);
    }
}
```

**Step 3: Create lookup.rs**

Create `src/validation/lookup.rs`:

```rust
use crate::cli::Source;
use crate::index::DoiIndex;

/// Result of a DOI lookup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResult {
    /// DOI found in specified source
    Found(Source),
    /// DOI not found in any checked source
    NotFound,
}

/// Look up a DOI in the appropriate index based on source mode
pub fn lookup_doi(
    doi: &str,
    source: Source,
    crossref: Option<&DoiIndex>,
    datacite: Option<&DoiIndex>,
) -> LookupResult {
    let doi_lower = doi.to_lowercase();

    match source {
        Source::All => {
            // Check Crossref first, then DataCite
            if let Some(idx) = crossref {
                if idx.contains(&doi_lower) {
                    return LookupResult::Found(Source::Crossref);
                }
            }
            if let Some(idx) = datacite {
                if idx.contains(&doi_lower) {
                    return LookupResult::Found(Source::Datacite);
                }
            }
            LookupResult::NotFound
        }
        Source::Crossref => {
            if let Some(idx) = crossref {
                if idx.contains(&doi_lower) {
                    return LookupResult::Found(Source::Crossref);
                }
            }
            LookupResult::NotFound
        }
        Source::Datacite | Source::Arxiv => {
            if let Some(idx) = datacite {
                if idx.contains(&doi_lower) {
                    return LookupResult::Found(Source::Datacite);
                }
            }
            LookupResult::NotFound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_doi_crossref() {
        let mut crossref = DoiIndex::new();
        crossref.insert("10.1234/found");

        let result = lookup_doi("10.1234/found", Source::Crossref, Some(&crossref), None);
        assert_eq!(result, LookupResult::Found(Source::Crossref));

        let result = lookup_doi("10.1234/notfound", Source::Crossref, Some(&crossref), None);
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn test_lookup_doi_all_mode() {
        let mut crossref = DoiIndex::new();
        crossref.insert("10.1234/crossref");

        let mut datacite = DoiIndex::new();
        datacite.insert("10.48550/datacite");

        let result = lookup_doi("10.1234/crossref", Source::All, Some(&crossref), Some(&datacite));
        assert_eq!(result, LookupResult::Found(Source::Crossref));

        let result = lookup_doi("10.48550/datacite", Source::All, Some(&crossref), Some(&datacite));
        assert_eq!(result, LookupResult::Found(Source::Datacite));

        let result = lookup_doi("10.9999/unknown", Source::All, Some(&crossref), Some(&datacite));
        assert_eq!(result, LookupResult::NotFound);
    }
}
```

**Step 4: Create http.rs (copy from existing validate.rs)**

Create `src/validation/http.rs`:

```rust
use log::debug;
use reqwest::Client;
use std::time::Duration;

/// Check if a DOI resolves via HTTP HEAD request
pub async fn check_doi_resolves(client: &Client, doi: &str, timeout: Duration) -> bool {
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

/// Create an HTTP client configured for DOI resolution
pub fn create_doi_client() -> reqwest::Result<Client> {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
}
```

**Step 5: Update main.rs**

Add to `src/main.rs`:

```rust
mod validation;
```

**Step 6: Build and test**

Run: `cargo test validation::`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/validation/ src/main.rs
git commit -m "feat(validation): add multi-source validation module with prefix filter and lookup"
```

---

## Task 11: Update Common Types for Multi-Source

**Files:**
- Modify: `src/common/types.rs`

**Step 1: Add generic citation types**

Add to `src/common/types.rs`:

```rust
/// Generic citation record for Crossref/DataCite output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationRecord {
    pub doi: String,
    pub reference_count: usize,
    pub citation_count: usize,
    pub cited_by: Vec<serde_json::Value>,
}

/// Statistics from multi-source validation
#[derive(Debug, Clone, Default)]
pub struct MultiValidateStats {
    pub total_records: usize,
    pub crossref_matched: usize,
    pub crossref_http_resolved: usize,
    pub crossref_failed: usize,
    pub datacite_matched: usize,
    pub datacite_http_resolved: usize,
    pub datacite_failed: usize,
}
```

**Step 2: Update mod.rs exports**

Ensure `src/common/mod.rs` exports the new types:

```rust
pub use types::{
    ArxivCitationsSimple, ArxivMatch, CitationRecord, DataCiteRecord,
    MultiValidateStats, ValidateStats,
};
```

**Step 3: Build to verify**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/common/
git commit -m "feat(common): add generic citation types for multi-source output"
```

---

## Task 12: Update Streaming Module for DOI Partitioning

**Files:**
- Modify: `src/streaming/mod.rs`
- Modify: `src/streaming/partition_writer.rs`

**Step 1: Update partition_key for generic DOIs**

In `src/streaming/mod.rs`, update the `partition_key` function:

```rust
/// Extract partition key from a DOI or arXiv ID.
/// For DOIs: uses prefix (e.g., "10.1234" -> "10.1234")
/// For arXiv IDs: uses first 4 chars (existing behavior)
pub fn partition_key(id: &str) -> String {
    // Check if it looks like a DOI (starts with 10.)
    if id.starts_with("10.") {
        // Use the DOI prefix as partition key
        if let Some(slash_pos) = id.find('/') {
            return id[..slash_pos].to_lowercase();
        }
    }

    // Fall back to first 4 chars for arXiv IDs
    id.to_lowercase()
        .chars()
        .take(4)
        .map(|c| if c == '/' { '_' } else { c })
        .collect()
}
```

**Step 2: Update partition_writer for generic DOI field name**

In `src/streaming/partition_writer.rs`, rename `arxiv_id` to `cited_id` in the buffer and related code:

```rust
/// A single extracted and exploded row ready for partitioning
#[derive(Debug, Clone)]
pub struct ExplodedRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub ref_json: String,
    pub raw_match: String,
    pub cited_id: String,  // Renamed from arxiv_id
}

struct PartitionBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    ref_jsons: Vec<String>,
    raw_matches: Vec<String>,
    cited_ids: Vec<String>,  // Renamed from arxiv_ids
    file_path: PathBuf,
    rows_written: usize,
}
```

Update all usages of `arxiv_id` to `cited_id` throughout the file and update the DataFrame column name.

**Step 3: Run tests**

Run: `cargo test streaming::`
Expected: Tests may need updates - fix as needed

**Step 4: Commit**

```bash
git add src/streaming/
git commit -m "refactor(streaming): generalize partitioning for DOIs, rename arxiv_id to cited_id"
```

---

## Task 13: Update Partition Invert for Multi-Source Output

**Files:**
- Modify: `src/streaming/partition_invert.rs`

**Step 1: Update invert logic for generic DOI output**

The inversion logic needs to produce generic output. Update `write_jsonl_output` to support both arXiv and generic formats based on a parameter.

Add a new function for generic output:

```rust
/// Write DataFrame to JSONL format for generic DOI citations
fn write_generic_jsonl_output(df: &DataFrame, path: &Path) -> Result<()> {
    info!("Writing generic JSONL output: {:?}", path);

    let file = File::create(path)
        .with_context(|| format!("Failed to create JSONL file: {:?}", path))?;
    let mut writer = BufWriter::new(file);

    let cited_id = df.column("cited_id")?.str()?;
    let reference_count = df.column("reference_count")?.u32()?;
    let citation_count = df.column("citation_count")?.u32()?;
    let cited_by = df.column("cited_by")?;

    for i in 0..df.height() {
        let doi = cited_id.get(i).unwrap_or("");
        let ref_count = reference_count.get(i).unwrap_or(0);
        let cit_count = citation_count.get(i).unwrap_or(0);

        let cited_by_json = build_cited_by_json(cited_by, i)?;

        let json_line = serde_json::json!({
            "doi": doi,
            "reference_count": ref_count,
            "citation_count": cit_count,
            "cited_by": cited_by_json
        });

        writeln!(writer, "{}", json_line)?;
    }

    writer.flush()?;
    Ok(())
}
```

**Step 2: Update invert_single_partition to use cited_id**

Replace `arxiv_id` references with `cited_id`:

```rust
let inverted = lf
    .unique(
        Some(vec!["citing_doi".into(), "cited_id".into()]),
        UniqueKeepStrategy::First,
    )
    .group_by([col("cited_id")])
    .agg([
        col("citing_doi").n_unique().alias("citation_count"),
        col("citing_doi").count().alias("reference_count"),
        as_struct(vec![
            col("citing_doi").alias("doi"),
            col("raw_match"),
            col("ref_json").alias("reference"),
        ])
        .alias("cited_by"),
    ]);
```

**Step 3: Update tests**

Update tests to use `cited_id` instead of `arxiv_id`.

**Step 4: Run tests**

Run: `cargo test streaming::partition_invert`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/streaming/partition_invert.rs
git commit -m "refactor(streaming): update invert to use generic cited_id, add generic JSONL output"
```

---

## Task 14: Stub Out Updated Pipeline Command

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Create minimal compiling pipeline**

This is a large refactor. First, stub out the pipeline to make it compile:

```rust
use anyhow::{Context, Result};
use log::info;
use std::path::Path;

use crate::cli::{PipelineArgs, Source};
use crate::common::setup_logging;

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    // Validate required arguments based on source mode
    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // TODO: Implement full pipeline
    info!("Pipeline stub - implementation in progress");

    Ok(())
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    match args.source {
        Source::All => {
            if args.output_crossref.is_none() || args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'all' requires both --output-crossref and --output-datacite"
                ));
            }
        }
        Source::Crossref => {
            if args.output_crossref.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'crossref' requires --output-crossref"
                ));
            }
        }
        Source::Datacite => {
            if args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --output-datacite"
                ));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
        Source::Arxiv => {
            if args.output_arxiv.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --output-arxiv"
                ));
            }
            if args.datacite_records.is_none() && args.load_datacite_index.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --datacite-records or --load-datacite-index"
                ));
            }
        }
    }
    Ok(())
}
```

**Step 2: Update commands/mod.rs**

Update the return type:

```rust
pub use pipeline::run_pipeline;
```

**Step 3: Update main.rs**

The main.rs should work with the stubbed pipeline.

**Step 4: Build to verify**

Run: `cargo build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add src/commands/pipeline.rs src/commands/mod.rs
git commit -m "refactor(pipeline): stub out new multi-source pipeline structure"
```

---

## Task 15: Implement Pipeline - Index Loading Phase

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Add index loading**

Add function to load or build indexes:

```rust
use crate::index::{build_index_from_jsonl_gz, load_index_from_parquet, save_index_to_parquet, DoiIndex};

struct PipelineIndexes {
    crossref: Option<DoiIndex>,
    datacite: Option<DoiIndex>,
}

fn load_indexes(args: &PipelineArgs) -> Result<PipelineIndexes> {
    let mut indexes = PipelineIndexes {
        crossref: None,
        datacite: None,
    };

    // Load or defer Crossref index (built during streaming)
    if let Some(ref path) = args.load_crossref_index {
        info!("Loading Crossref index from: {}", path);
        indexes.crossref = Some(load_index_from_parquet(path)?);
    }

    // Load or build DataCite index
    if let Some(ref path) = args.load_datacite_index {
        info!("Loading DataCite index from: {}", path);
        indexes.datacite = Some(load_index_from_parquet(path)?);
    } else if let Some(ref path) = args.datacite_records {
        info!("Building DataCite index from: {}", path);
        indexes.datacite = Some(build_index_from_jsonl_gz(path, "id")?);
    }

    Ok(indexes)
}
```

**Step 2: Integrate into run_pipeline**

Update run_pipeline to call load_indexes:

```rust
pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // Phase 1: Load indexes
    info!("");
    info!("=== Loading Indexes ===");
    let mut indexes = load_indexes(&args)?;

    // TODO: Phase 2: Extract and build Crossref index
    // TODO: Phase 3: Invert partitions
    // TODO: Phase 4: Validate

    // Save indexes if requested
    if let Some(ref path) = args.save_datacite_index {
        if let Some(ref index) = indexes.datacite {
            save_index_to_parquet(index, path)?;
        }
    }

    Ok(())
}
```

**Step 3: Build and test**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "feat(pipeline): implement index loading phase"
```

---

## Task 16: Implement Pipeline - Extraction Phase

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Add extraction function for DOI mode**

Add extraction logic that builds Crossref index while streaming:

```rust
use crate::extract::{extract_doi_matches_from_text, extract_arxiv_matches_from_text};
use crate::streaming::PartitionWriter;

fn run_extraction(
    args: &PipelineArgs,
    indexes: &mut PipelineIndexes,
) -> Result<ExtractionStats> {
    // ... implementation that:
    // 1. Streams through tar.gz
    // 2. For each record, extracts the DOI and adds to Crossref index
    // 3. For each reference, extracts DOIs (or arXiv IDs in arxiv mode)
    // 4. Writes to partition files
}
```

This is a substantial implementation. Continue building out the extraction logic following the existing pattern in the current pipeline.rs.

**Step 2: Build and test**

Run: `cargo build`

**Step 3: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "feat(pipeline): implement extraction phase with Crossref index building"
```

---

## Task 17: Implement Pipeline - Inversion Phase

**Files:**
- Modify: `src/commands/pipeline.rs`
- Modify: `src/streaming/partition_invert.rs`

**Step 1: Add OutputMode enum to partition_invert.rs**

Add at top of `src/streaming/partition_invert.rs`:

```rust
/// Output format mode for inversion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// arXiv-specific output with arxiv_doi and arxiv_id fields
    Arxiv,
    /// Generic DOI output with just doi field
    Generic,
}
```

**Step 2: Update invert_partitions signature**

Modify the function signature to accept output mode:

```rust
pub fn invert_partitions(
    partition_dir: &Path,
    output_parquet: &Path,
    output_jsonl: Option<&Path>,
    checkpoint: &mut Checkpoint,
    output_mode: OutputMode,
) -> Result<InvertStats> {
```

**Step 3: Update JSONL writing to use output mode**

Replace the JSONL writing section:

```rust
// Write JSONL output if requested
if let Some(jsonl_path) = output_jsonl {
    match output_mode {
        OutputMode::Arxiv => write_arxiv_jsonl_output(&combined, jsonl_path)?,
        OutputMode::Generic => write_generic_jsonl_output(&combined, jsonl_path)?,
    }
}
```

**Step 4: Implement write_arxiv_jsonl_output**

Rename existing `write_jsonl_output` to `write_arxiv_jsonl_output`:

```rust
/// Write DataFrame to JSONL format for arXiv citations (original format)
fn write_arxiv_jsonl_output(df: &DataFrame, path: &Path) -> Result<()> {
    info!("Writing arXiv JSONL output: {:?}", path);

    let file = File::create(path)
        .with_context(|| format!("Failed to create JSONL file: {:?}", path))?;
    let mut writer = BufWriter::new(file);

    let cited_id = df.column("cited_id")?.str()?;
    let reference_count = df.column("reference_count")?.u32()?;
    let citation_count = df.column("citation_count")?.u32()?;
    let cited_by = df.column("cited_by")?;

    for i in 0..df.height() {
        let id = cited_id.get(i).unwrap_or("");
        let arxiv_doi = format!("10.48550/arXiv.{}", id);
        let ref_count = reference_count.get(i).unwrap_or(0);
        let cit_count = citation_count.get(i).unwrap_or(0);

        let cited_by_json = build_cited_by_json(cited_by, i)?;

        let json_line = serde_json::json!({
            "arxiv_doi": arxiv_doi,
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
```

**Step 5: Add inversion call to pipeline**

In `src/commands/pipeline.rs`, add inversion phase:

```rust
use crate::streaming::{invert_partitions, OutputMode};

// In run_pipeline, after extraction phase:
info!("");
info!("=== Aggregating Citations ===");

let output_mode = match args.source {
    Source::Arxiv => OutputMode::Arxiv,
    _ => OutputMode::Generic,
};

let invert_stats = invert_partitions(
    &ctx.partition_dir,
    &ctx.output_parquet,
    Some(&ctx.output_jsonl),
    &mut checkpoint,
    output_mode,
)?;

info!("Aggregation complete:");
info!("  Partitions processed: {}", invert_stats.partitions_processed);
info!("  Unique cited works: {}", invert_stats.unique_arxiv_works);
info!("  Total citations: {}", invert_stats.total_citations);
```

**Step 6: Run tests**

Run: `cargo test streaming::partition_invert`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/streaming/partition_invert.rs src/commands/pipeline.rs
git commit -m "feat(pipeline): implement inversion phase with multi-source output modes"
```

---

## Task 18: Implement Pipeline - Validation Phase

**Files:**
- Modify: `src/commands/pipeline.rs`
- Create: `src/validation/runner.rs`
- Modify: `src/validation/mod.rs`

**Step 1: Create validation runner module**

Create `src/validation/runner.rs`:

```rust
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use log::{debug, info};
use reqwest::Client;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::cli::Source;
use crate::common::{format_elapsed, CitationRecord, MultiValidateStats};
use crate::index::DoiIndex;
use super::{check_doi_resolves, create_doi_client, lookup_doi, LookupResult};

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

    let file = File::open(input_path)
        .with_context(|| format!("Failed to open: {}", input_path))?;
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

        let record: CitationRecord = serde_json::from_str(&line)?;
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

    info!("Index lookup: {} matched, {} unmatched", matched.len(), unmatched.len());

    // Phase 2: HTTP fallback for unmatched (if enabled)
    let mut http_resolved: Vec<(CitationRecord, Source)> = Vec::new();
    let mut failed: Vec<CitationRecord> = Vec::new();

    if http_fallback && !unmatched.is_empty() {
        info!("Running HTTP fallback for {} unmatched DOIs...", unmatched.len());

        let client = create_doi_client()?;
        let timeout = Duration::from_secs(timeout_secs);
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let results: Vec<(CitationRecord, bool)> = stream::iter(unmatched.into_iter())
            .map(|record| {
                let client = client.clone();
                let semaphore = semaphore.clone();

                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let resolves = check_doi_resolves(&client, &record.doi, timeout).await;
                    (record, resolves)
                }
            })
            .buffer_unordered(concurrency * 2)
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
                    Source::Datacite | Source::Arxiv => stats.datacite_failed += 1,
                    Source::All => stats.datacite_failed += 1,
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

/// Results from validation
pub struct ValidationResults {
    pub valid: Vec<(CitationRecord, Source)>,
    pub failed: Vec<CitationRecord>,
    pub stats: MultiValidateStats,
}

/// Write validation results to output files
pub fn write_validation_results(
    results: &ValidationResults,
    output_valid: &str,
    output_failed: Option<&str>,
) -> Result<()> {
    info!("Writing {} valid records to: {}", results.valid.len(), output_valid);

    let file = File::create(output_valid)?;
    let mut writer = BufWriter::new(file);

    for (record, _source) in &results.valid {
        writeln!(writer, "{}", serde_json::to_string(record)?)?;
    }
    writer.flush()?;

    if let Some(failed_path) = output_failed {
        info!("Writing {} failed records to: {}", results.failed.len(), failed_path);
        let file = File::create(failed_path)?;
        let mut writer = BufWriter::new(file);

        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    Ok(())
}
```

**Step 2: Update validation/mod.rs**

Add to `src/validation/mod.rs`:

```rust
pub mod runner;
pub use runner::*;
```

**Step 3: Add validation phase to pipeline**

In `src/commands/pipeline.rs`:

```rust
use crate::validation::{validate_citations, write_validation_results};

// In run_pipeline, after inversion phase:
info!("");
info!("=== Validating Citations ===");

let http_fallback_enabled = args.http_fallback.iter()
    .any(|s| s == "crossref" || s == "datacite" || s == "all");

let rt = tokio::runtime::Runtime::new()?;
let validation_results = rt.block_on(validate_citations(
    &ctx.output_jsonl.to_string_lossy(),
    indexes.crossref.as_ref(),
    indexes.datacite.as_ref(),
    args.source,
    http_fallback_enabled,
    args.concurrency,
    args.timeout,
))?;

info!("Validation results:");
info!("  Total valid: {}", validation_results.valid.len());
info!("  Total failed: {}", validation_results.failed.len());
```

**Step 4: Run tests**

Run: `cargo test validation::`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/validation/runner.rs src/validation/mod.rs src/commands/pipeline.rs
git commit -m "feat(pipeline): implement validation phase with HTTP fallback"
```

---

## Task 19: Implement Pipeline - Output Writing Phase

**Files:**
- Modify: `src/commands/pipeline.rs`
- Modify: `src/validation/runner.rs`

**Step 1: Add source-specific output writing**

Add to `src/validation/runner.rs`:

```rust
/// Write validation results split by source
pub fn write_split_validation_results(
    results: &ValidationResults,
    output_crossref: Option<&str>,
    output_datacite: Option<&str>,
    output_crossref_failed: Option<&str>,
    output_datacite_failed: Option<&str>,
) -> Result<(usize, usize)> {
    let mut crossref_count = 0;
    let mut datacite_count = 0;

    // Split valid results by source
    let (crossref_valid, datacite_valid): (Vec<_>, Vec<_>) = results.valid
        .iter()
        .partition(|(_, source)| *source == Source::Crossref);

    // Write Crossref valid
    if let Some(path) = output_crossref {
        info!("Writing {} Crossref citations to: {}", crossref_valid.len(), path);
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        for (record, _) in &crossref_valid {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
            crossref_count += 1;
        }
        writer.flush()?;
    }

    // Write DataCite valid
    if let Some(path) = output_datacite {
        info!("Writing {} DataCite citations to: {}", datacite_valid.len(), path);
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        for (record, _) in &datacite_valid {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
            datacite_count += 1;
        }
        writer.flush()?;
    }

    // Write failed (split by intended source based on prefix)
    if let Some(path) = output_crossref_failed {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        for record in &results.failed {
            // Simple heuristic: if prefix starts with common Crossref prefixes
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    if let Some(path) = output_datacite_failed {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        for record in &results.failed {
            writeln!(writer, "{}", serde_json::to_string(record)?)?;
        }
        writer.flush()?;
    }

    Ok((crossref_count, datacite_count))
}
```

**Step 2: Add arXiv-specific output writing**

Add to `src/validation/runner.rs`:

```rust
use crate::common::ArxivCitationsSimple;

/// Write arXiv validation results (converts generic to arXiv format)
pub fn write_arxiv_validation_results(
    results: &ValidationResults,
    output_arxiv: &str,
    output_arxiv_failed: Option<&str>,
) -> Result<()> {
    info!("Writing {} arXiv citations to: {}", results.valid.len(), output_arxiv);

    let file = File::create(output_arxiv)?;
    let mut writer = BufWriter::new(file);

    for (record, _) in &results.valid {
        // Convert generic CitationRecord to ArxivCitationsSimple
        let arxiv_id = record.doi
            .strip_prefix("10.48550/arXiv.")
            .or_else(|| record.doi.strip_prefix("10.48550/arxiv."))
            .unwrap_or(&record.doi);

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
        info!("Writing {} failed arXiv citations to: {}", results.failed.len(), failed_path);
        let file = File::create(failed_path)?;
        let mut writer = BufWriter::new(file);

        for record in &results.failed {
            let arxiv_id = record.doi
                .strip_prefix("10.48550/arXiv.")
                .or_else(|| record.doi.strip_prefix("10.48550/arxiv."))
                .unwrap_or(&record.doi);

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
```

**Step 3: Integrate output writing in pipeline**

In `src/commands/pipeline.rs`, after validation:

```rust
// Write outputs based on source mode
match args.source {
    Source::All => {
        write_split_validation_results(
            &validation_results,
            args.output_crossref.as_deref(),
            args.output_datacite.as_deref(),
            args.output_crossref_failed.as_deref(),
            args.output_datacite_failed.as_deref(),
        )?;
    }
    Source::Crossref => {
        write_validation_results(
            &validation_results,
            args.output_crossref.as_ref().unwrap(),
            args.output_crossref_failed.as_deref(),
        )?;
    }
    Source::Datacite => {
        write_validation_results(
            &validation_results,
            args.output_datacite.as_ref().unwrap(),
            args.output_datacite_failed.as_deref(),
        )?;
    }
    Source::Arxiv => {
        write_arxiv_validation_results(
            &validation_results,
            args.output_arxiv.as_ref().unwrap(),
            args.output_arxiv_failed.as_deref(),
        )?;
    }
}
```

**Step 4: Build and test**

Run: `cargo build`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add src/validation/runner.rs src/commands/pipeline.rs
git commit -m "feat(pipeline): implement source-specific output writing"
```

---

## Task 20: Update Validate Command for Standalone Use

**Files:**
- Modify: `src/commands/validate.rs`

**Step 1: Rewrite validate command**

Replace `src/commands/validate.rs`:

```rust
use anyhow::{Context, Result};
use log::info;
use std::path::Path;

use crate::cli::{Source, ValidateArgs};
use crate::common::setup_logging;
use crate::index::{build_index_from_jsonl_gz, load_index_from_parquet, DoiIndex};
use crate::validation::{validate_citations, write_arxiv_validation_results, write_validation_results};

pub fn run_validate(args: ValidateArgs) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_validate_async(args))
}

pub async fn run_validate_async(args: ValidateArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting standalone validation");
    info!("Input: {}", args.input);
    info!("Source: {}", args.source);

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // Load indexes based on source
    let crossref_index: Option<DoiIndex> = if let Some(ref path) = args.crossref_index {
        info!("Loading Crossref index from: {}", path);
        Some(load_index_from_parquet(path)?)
    } else {
        None
    };

    let datacite_index: Option<DoiIndex> = if let Some(ref path) = args.datacite_records {
        info!("Building DataCite index from: {}", path);
        Some(build_index_from_jsonl_gz(path, "id")?)
    } else {
        None
    };

    // Validate required indexes
    match args.source {
        Source::Crossref => {
            if crossref_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "Crossref validation requires --crossref-index or --http-fallback"
                ));
            }
        }
        Source::Datacite | Source::Arxiv => {
            if datacite_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "DataCite/arXiv validation requires --datacite-records or --http-fallback"
                ));
            }
        }
        Source::All => {
            if crossref_index.is_none() && datacite_index.is_none() && !args.http_fallback {
                return Err(anyhow::anyhow!(
                    "Validation requires at least one index or --http-fallback"
                ));
            }
        }
    }

    // Run validation
    let results = validate_citations(
        &args.input,
        crossref_index.as_ref(),
        datacite_index.as_ref(),
        args.source,
        args.http_fallback,
        args.concurrency,
        args.timeout,
    ).await?;

    // Write results
    match args.source {
        Source::Arxiv => {
            write_arxiv_validation_results(
                &results,
                &args.output_valid,
                Some(&args.output_failed),
            )?;
        }
        _ => {
            write_validation_results(
                &results,
                &args.output_valid,
                Some(&args.output_failed),
            )?;
        }
    }

    info!("==================== VALIDATION COMPLETE ====================");
    info!("Total records: {}", results.stats.total_records);
    info!("Valid: {}", results.valid.len());
    info!("Failed: {}", results.failed.len());
    info!("Output valid: {}", args.output_valid);
    info!("Output failed: {}", args.output_failed);
    info!("=============================================================");

    Ok(())
}
```

**Step 2: Update commands/mod.rs**

Ensure exports are correct:

```rust
pub mod pipeline;
pub mod validate;

pub use pipeline::run_pipeline;
pub use validate::run_validate;
```

**Step 3: Build and test**

Run: `cargo build`
Expected: Build succeeds

**Step 4: Test CLI help**

Run: `cargo run -- validate --help`
Expected: Shows new validate options

**Step 5: Commit**

```bash
git add src/commands/validate.rs src/commands/mod.rs
git commit -m "feat(validate): update standalone validate command for multi-source"
```

---

## Task 21: Add Integration Tests

**Files:**
- Create: `tests/integration_test.rs`

**Step 1: Create integration test file**

Create `tests/integration_test.rs`:

```rust
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use tempfile::tempdir;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::Builder;

/// Create a minimal Crossref tar.gz for testing
fn create_test_crossref_tar_gz(dir: &std::path::Path) -> std::path::PathBuf {
    let tar_path = dir.join("test_crossref.tar.gz");
    let file = File::create(&tar_path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    // Create a JSON file with test data
    let json_content = r#"{
        "items": [
            {
                "DOI": "10.1234/citing-paper",
                "reference": [
                    {"unstructured": "See arXiv:2403.12345 for details"},
                    {"DOI": "10.5678/another-paper"},
                    {"unstructured": "Also 10.9999/datacite-doi"}
                ]
            },
            {
                "DOI": "10.1234/self-cite",
                "reference": [
                    {"DOI": "10.1234/self-cite"}
                ]
            }
        ]
    }"#;

    let json_bytes = json_content.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_path("test/file1.json").unwrap();
    header.set_size(json_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();

    builder.append(&header, json_bytes).unwrap();
    builder.into_inner().unwrap().finish().unwrap();

    tar_path
}

/// Create a minimal DataCite records file
fn create_test_datacite_records(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("datacite.jsonl.gz");
    let file = File::create(&path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);

    writeln!(writer, r#"{{"id": "10.48550/arXiv.2403.12345"}}"#).unwrap();
    writeln!(writer, r#"{{"id": "10.9999/datacite-doi"}}"#).unwrap();

    writer.into_inner().unwrap().finish().unwrap();
    path
}

#[test]
fn test_arxiv_mode_extraction() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let datacite_path = create_test_datacite_records(dir.path());
    let output_path = dir.path().join("output.jsonl");

    let status = Command::new("cargo")
        .args([
            "run", "--",
            "pipeline",
            "--input", tar_path.to_str().unwrap(),
            "--datacite-records", datacite_path.to_str().unwrap(),
            "--source", "arxiv",
            "--output-arxiv", output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should succeed");
    assert!(output_path.exists(), "Output file should exist");

    // Verify output format
    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);
    let mut found_arxiv = false;

    for line in reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert!(record.get("arxiv_doi").is_some(), "Should have arxiv_doi field");
        assert!(record.get("arxiv_id").is_some(), "Should have arxiv_id field");

        if record["arxiv_id"].as_str() == Some("2403.12345") {
            found_arxiv = true;
        }
    }

    assert!(found_arxiv, "Should find the arXiv reference");
}

#[test]
fn test_self_citation_removed() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let output_path = dir.path().join("output.jsonl");

    let status = Command::new("cargo")
        .args([
            "run", "--",
            "pipeline",
            "--input", tar_path.to_str().unwrap(),
            "--source", "crossref",
            "--output-crossref", output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success());

    // Check that self-citation is not in output
    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.unwrap();
        let record: serde_json::Value = serde_json::from_str(&line).unwrap();

        // If we find the self-cite DOI, check it doesn't cite itself
        if record["doi"].as_str() == Some("10.1234/self-cite") {
            let cited_by = record["cited_by"].as_array().unwrap();
            for citation in cited_by {
                assert_ne!(
                    citation["doi"].as_str(),
                    Some("10.1234/self-cite"),
                    "Self-citation should be removed"
                );
            }
        }
    }
}

#[test]
fn test_index_save_and_load() {
    let dir = tempdir().unwrap();
    let datacite_path = create_test_datacite_records(dir.path());
    let index_path = dir.path().join("datacite_index.parquet");

    // First run: build and save index
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let output1 = dir.path().join("output1.jsonl");

    let status = Command::new("cargo")
        .args([
            "run", "--",
            "pipeline",
            "--input", tar_path.to_str().unwrap(),
            "--datacite-records", datacite_path.to_str().unwrap(),
            "--source", "datacite",
            "--output-datacite", output1.to_str().unwrap(),
            "--save-datacite-index", index_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success());
    assert!(index_path.exists(), "Index file should be saved");

    // Second run: load index (no datacite-records)
    let output2 = dir.path().join("output2.jsonl");

    let status = Command::new("cargo")
        .args([
            "run", "--",
            "pipeline",
            "--input", tar_path.to_str().unwrap(),
            "--load-datacite-index", index_path.to_str().unwrap(),
            "--source", "datacite",
            "--output-datacite", output2.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline with loaded index");

    assert!(status.success());
}
```

**Step 2: Run integration tests**

Run: `cargo test --test integration_test`
Expected: All tests pass (may need adjustments based on actual implementation)

**Step 3: Commit**

```bash
git add tests/integration_test.rs
git commit -m "test: add integration tests for multi-source pipeline"
```

---

## Task 22: Update README

**Files:**
- Modify: `README.md`

**Step 1: Replace README content**

Replace `README.md`:

```markdown
# crossref-citation-extraction

CLI for extracting and validating DOI references from Crossref data files.

## Overview

This tool processes Crossref snapshot data to identify works that cite other DOIs, validates those citations against Crossref and DataCite records, and outputs citation data organized by cited work.

Supported extraction modes:
- **all**: Extract all DOIs, validate against both Crossref and DataCite
- **crossref**: Extract DOIs, validate only against Crossref records
- **datacite**: Extract DOIs, validate only against DataCite records
- **arxiv**: Extract arXiv references using pattern matching, validate against DataCite

## Building

```bash
cargo build --release
```

The binary will be at `target/release/crossref-citation-extraction`.

## Usage

### Full Pipeline (All Sources)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite-records.jsonl.gz \
  --source all \
  --output-crossref crossref_citations.jsonl \
  --output-datacite datacite_citations.jsonl
```

### Crossref Only

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --source crossref \
  --output-crossref crossref_citations.jsonl
```

### arXiv Only (Original Behavior)

```bash
crossref-citation-extraction pipeline \
  --input crossref-snapshot.tar.gz \
  --datacite-records datacite-records.jsonl.gz \
  --source arxiv \
  --output-arxiv arxiv_citations.jsonl
```

### Options

**Source selection:**
- `--source all|crossref|datacite|arxiv` - Which source to extract and validate

**Input files:**
- `--input` - Crossref snapshot tar.gz (required)
- `--datacite-records` - DataCite records JSONL.gz (required for datacite/arxiv modes)

**Output files:**
- `--output-crossref` - Crossref citations output
- `--output-datacite` - DataCite citations output
- `--output-arxiv` - arXiv citations output (arxiv mode)
- `--output-*-failed` - Failed validation output for each source

**Index persistence:**
- `--save-crossref-index path.parquet` - Save Crossref DOI index
- `--load-crossref-index path.parquet` - Load Crossref DOI index
- `--save-datacite-index path.parquet` - Save DataCite DOI index
- `--load-datacite-index path.parquet` - Load DataCite DOI index

**Validation:**
- `--http-fallback crossref,datacite` - Enable HTTP validation for specified sources
- `--concurrency N` - Concurrent HTTP requests (default: 50)
- `--timeout N` - Seconds per request (default: 5)

**Other:**
- `--keep-intermediates` - Keep partition files after completion
- `--temp-dir` - Directory for intermediate files
- `--batch-size` - Batch size for memory management

### Standalone Validation

Validate a previously generated JSONL file:

```bash
crossref-citation-extraction validate \
  --input citations.jsonl \
  --datacite-records datacite.jsonl.gz \
  --source datacite \
  --output-valid valid.jsonl \
  --output-failed failed.jsonl
```

## Output Format

### Crossref/DataCite Output

```json
{
  "doi": "10.1234/example",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [
    {
      "doi": "10.5678/citing-paper",
      "matches": [
        {
          "raw_match": "10.1234/example",
          "reference": {"unstructured": "..."}
        }
      ]
    }
  ]
}
```

### arXiv Output

```json
{
  "arxiv_doi": "10.48550/arXiv.2403.03542",
  "arxiv_id": "2403.03542",
  "reference_count": 5,
  "citation_count": 3,
  "cited_by": [...]
}
```

## DOI Patterns

The extractor recognizes these DOI formats:
- Bare DOI: `10.1234/example`
- Prefixed: `doi:10.1234/example`
- URL: `https://doi.org/10.1234/example`, `http://dx.doi.org/10.1234/example`

## arXiv ID Patterns (arxiv mode)

- Modern: `arXiv:2403.03542`, `arXiv.2403.03542v2`
- Old format: `arXiv:hep-ph/9901234`, `arXiv:cs.DM/9910013`
- DOI format: `10.48550/arXiv.2403.03542`
- URL format: `arxiv.org/abs/2403.03542`

## Validation Logic

1. Check DOI against local index (fast, O(1) lookup)
2. For unmatched DOIs with `--http-fallback`, attempt HTTP HEAD to doi.org
3. DOI is valid if found in index OR doi.org returns 2xx/3xx
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README for multi-source CLI"
```

---

## Task 23: Add Performance Benchmarks

**Files:**
- Create: `benches/extraction_bench.rs`
- Modify: `Cargo.toml`

**Step 1: Add benchmark dependencies to Cargo.toml**

Add to `Cargo.toml`:

```toml
[dev-dependencies]
tempfile = "3"
criterion = "0.5"

[[bench]]
name = "extraction_bench"
harness = false
```

**Step 2: Create benchmark file**

Create `benches/extraction_bench.rs`:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// Import from the library
use crossref_citation_extraction::extract::{
    extract_doi_matches_from_text,
    extract_arxiv_matches_from_text,
    normalize_doi,
};
use crossref_citation_extraction::index::DoiIndex;

fn bench_doi_extraction(c: &mut Criterion) {
    let sample_texts = vec![
        "See 10.1234/example.paper for details",
        "doi:10.5678/another-paper and https://doi.org/10.9999/third",
        "Multiple DOIs: 10.1111/a, 10.2222/b, 10.3333/c, 10.4444/d",
        "No DOIs in this text at all",
        "Mixed content with arXiv:2403.12345 and 10.48550/arXiv.2403.12345",
    ];

    let mut group = c.benchmark_group("doi_extraction");
    group.throughput(Throughput::Elements(sample_texts.len() as u64));

    group.bench_function("extract_doi_matches", |b| {
        b.iter(|| {
            for text in &sample_texts {
                black_box(extract_doi_matches_from_text(text));
            }
        })
    });

    group.finish();
}

fn bench_arxiv_extraction(c: &mut Criterion) {
    let sample_texts = vec![
        "arXiv:2403.12345",
        "arXiv:hep-ph/9901234 and arXiv:cs.DM/9910013",
        "https://arxiv.org/abs/2403.12345",
        "10.48550/arXiv.2403.12345",
        "No arXiv references here",
    ];

    let mut group = c.benchmark_group("arxiv_extraction");
    group.throughput(Throughput::Elements(sample_texts.len() as u64));

    group.bench_function("extract_arxiv_matches", |b| {
        b.iter(|| {
            for text in &sample_texts {
                black_box(extract_arxiv_matches_from_text(text));
            }
        })
    });

    group.finish();
}

fn bench_doi_index_lookup(c: &mut Criterion) {
    // Create an index with 1M DOIs
    let mut index = DoiIndex::with_capacity(1_000_000, 10_000);
    for i in 0..1_000_000 {
        index.insert(&format!("10.{}/{}", 1000 + (i % 10000), i));
    }

    let test_dois = vec![
        "10.1234/500000",  // exists
        "10.5678/999999",  // exists
        "10.9999/notfound", // doesn't exist
    ];

    let mut group = c.benchmark_group("index_lookup");

    group.bench_function("contains", |b| {
        b.iter(|| {
            for doi in &test_dois {
                black_box(index.contains(doi));
            }
        })
    });

    group.bench_function("has_prefix", |b| {
        b.iter(|| {
            black_box(index.has_prefix("10.1234"));
            black_box(index.has_prefix("10.9999"));
        })
    });

    group.finish();
}

fn bench_normalize_doi(c: &mut Criterion) {
    let test_dois = vec![
        "10.1234/TEST",
        "10.1234/test.",
        "10.1234%2Fencoded",
        "10.1234/clean",
    ];

    c.bench_function("normalize_doi", |b| {
        b.iter(|| {
            for doi in &test_dois {
                black_box(normalize_doi(doi));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_doi_extraction,
    bench_arxiv_extraction,
    bench_doi_index_lookup,
    bench_normalize_doi,
);
criterion_main!(benches);
```

**Step 3: Make library exports public**

Update `src/main.rs` to expose modules for benchmarks (or create a `src/lib.rs`):

```rust
// src/lib.rs
pub mod cli;
pub mod common;
pub mod extract;
pub mod index;
pub mod streaming;
pub mod validation;
```

**Step 4: Run benchmarks**

Run: `cargo bench`
Expected: Benchmarks run and output performance metrics

**Step 5: Commit**

```bash
git add benches/extraction_bench.rs Cargo.toml src/lib.rs
git commit -m "perf: add benchmarks for extraction and index lookup"
```

---

## Task 24: Implement Self-Citation Removal and Deduplication

**Files:**
- Modify: `src/commands/pipeline.rs`
- Modify: `src/streaming/partition_invert.rs`

**Step 1: Add self-citation filter during extraction**

In `src/commands/pipeline.rs`, in the extraction loop:

```rust
// In extract_from_reference or the main extraction loop:
fn should_include_citation(citing_doi: &str, cited_doi: &str) -> bool {
    // Remove self-citations
    if citing_doi.to_lowercase() == cited_doi.to_lowercase() {
        return false;
    }
    true
}

// Usage in extraction:
for (raw_match, cited_id) in raw_matches.iter().zip(normalized_ids.iter()) {
    // Skip self-citations
    if !should_include_citation(citing_doi, cited_id) {
        continue;
    }

    writer.write(ExplodedRow {
        citing_doi: citing_doi.to_string(),
        ref_index,
        ref_json: ref_json.to_string(),
        raw_match: raw_match.clone(),
        cited_id: cited_id.clone(),
    })?;
}
```

**Step 2: Add deduplication in inversion**

The inversion already uses `.unique()` but ensure it's correct:

```rust
// In invert_single_partition:
let inverted = lf
    // Deduplicate: same citing_doi + cited_id should only count once
    .unique(
        Some(vec!["citing_doi".into(), "cited_id".into()]),
        UniqueKeepStrategy::First,
    )
    // Also filter out any self-citations that slipped through
    .filter(col("citing_doi").neq(col("cited_id")))
    .group_by([col("cited_id")])
    // ... rest of aggregation
```

**Step 3: Add test for self-citation removal**

Add to `src/commands/pipeline.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_include_citation() {
        assert!(should_include_citation("10.1234/a", "10.5678/b"));
        assert!(!should_include_citation("10.1234/a", "10.1234/a"));
        assert!(!should_include_citation("10.1234/A", "10.1234/a")); // Case insensitive
    }
}
```

**Step 4: Run tests**

Run: `cargo test`
Expected: All tests pass including new self-citation test

**Step 5: Commit**

```bash
git add src/commands/pipeline.rs src/streaming/partition_invert.rs
git commit -m "fix: ensure self-citations are removed and duplicates deduplicated"
```

---

## Task 25: Final Cleanup and Polish

**Files:**
- Multiple files for cleanup

**Step 1: Remove dead code warnings**

Run: `cargo clippy`
Fix any warnings about unused code, imports, or variables.

**Step 2: Update CLAUDE.md with new architecture**

Update `CLAUDE.md`:

```markdown
## Architecture

This is a Rust CLI that extracts DOI references from Crossref snapshot data files, aggregates citations by cited work, and validates them against Crossref and DataCite records.

### Data Flow

```
Crossref tar.gz → Extract DOIs → Partition by DOI prefix → Invert (aggregate by cited work) → Validate against indexes → Output
```

### Module Structure

- **`cli.rs`** - Clap-based command definitions with Source enum
- **`commands/`** - Command implementations
  - `pipeline.rs` - Full pipeline: streams tar.gz, extracts refs, partitions, inverts, validates
  - `validate.rs` - Standalone validation against indexes
- **`extract/`** - DOI and arXiv ID extraction
  - `doi.rs` - Generic DOI extraction patterns and normalization
  - `arxiv.rs` - arXiv-specific patterns (unchanged from v1)
- **`index/`** - DOI index management
  - `mod.rs` - DoiIndex type with prefix tracking
  - `builder.rs` - Build indexes from JSONL.gz files
  - `persistence.rs` - Parquet save/load for indexes
- **`streaming/`** - Partition-based processing
  - `partition_writer.rs` - Writes to per-partition Parquet files
  - `partition_invert.rs` - Parallel inversion with multi-format output
  - `checkpoint.rs` - Resume support
- **`validation/`** - Multi-source validation
  - `prefix_filter.rs` - Fast prefix-based filtering
  - `lookup.rs` - Index-based DOI lookup
  - `http.rs` - HTTP fallback validation
  - `runner.rs` - Validation orchestration
- **`common/`** - Shared types and utilities
```

**Step 3: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 4: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

**Step 5: Format code**

Run: `cargo fmt`

**Step 6: Build release**

Run: `cargo build --release`
Expected: Build succeeds

**Step 7: Final commit**

```bash
git add -A
git commit -m "chore: final cleanup and polish for v2.0"
```

---

## Testing Commands

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test extract::doi
cargo test index::
cargo test validation::
cargo test streaming::

# Build release
cargo build --release

# Test CLI help
./target/release/crossref-citation-extraction --help
./target/release/crossref-citation-extraction pipeline --help
```

## Verification Checklist

- [ ] All existing tests pass
- [ ] New tests pass for DOI extraction
- [ ] Index save/load roundtrips correctly
- [ ] arXiv mode produces identical output to v1
- [ ] Memory usage stays bounded during streaming
- [ ] Partition files use DOI prefix correctly
- [ ] HTTP fallback works for specified sources
- [ ] Self-citations are dropped
- [ ] Deduplication works correctly
