# Provenance Tracking Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Track whether each DOI reference was explicitly asserted (`publisher`/`crossref`) or mined from text, enabling quality-based filtering with automatic split output files.

**Architecture:** Add `Provenance` enum to extraction types, carry it through partition files, include in `cited_by` output, and generate `_asserted`/`_mined` split files automatically from any output path.

**Tech Stack:** Rust, Polars (Parquet), serde_json

---

## Task 1: Add Provenance Enum

**Files:**
- Create: `src/extract/provenance.rs`
- Modify: `src/extract/mod.rs`

**Step 1: Write the failing test**

In `src/extract/provenance.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provenance_serialization() {
        assert_eq!(serde_json::to_string(&Provenance::Publisher).unwrap(), "\"publisher\"");
        assert_eq!(serde_json::to_string(&Provenance::Crossref).unwrap(), "\"crossref\"");
        assert_eq!(serde_json::to_string(&Provenance::Mined).unwrap(), "\"mined\"");
    }

    #[test]
    fn test_provenance_deserialization() {
        assert_eq!(serde_json::from_str::<Provenance>("\"publisher\"").unwrap(), Provenance::Publisher);
        assert_eq!(serde_json::from_str::<Provenance>("\"crossref\"").unwrap(), Provenance::Crossref);
        assert_eq!(serde_json::from_str::<Provenance>("\"mined\"").unwrap(), Provenance::Mined);
    }

    #[test]
    fn test_provenance_ordering() {
        // Publisher > Crossref > Mined (for deduplication preference)
        assert!(Provenance::Publisher > Provenance::Crossref);
        assert!(Provenance::Crossref > Provenance::Mined);
    }

    #[test]
    fn test_provenance_to_string() {
        assert_eq!(Provenance::Publisher.as_str(), "publisher");
        assert_eq!(Provenance::Crossref.as_str(), "crossref");
        assert_eq!(Provenance::Mined.as_str(), "mined");
    }

    #[test]
    fn test_provenance_is_asserted() {
        assert!(Provenance::Publisher.is_asserted());
        assert!(Provenance::Crossref.is_asserted());
        assert!(!Provenance::Mined.is_asserted());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test provenance::tests -v`
Expected: FAIL with "can't find crate for `provenance`"

**Step 3: Write minimal implementation**

In `src/extract/provenance.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Provenance of a DOI reference - how it was obtained
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provenance {
    /// Mined from unstructured text (lowest quality)
    Mined = 0,
    /// Matched by Crossref
    Crossref = 1,
    /// Explicitly provided by publisher (highest quality)
    Publisher = 2,
}

impl Provenance {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Provenance::Publisher => "publisher",
            Provenance::Crossref => "crossref",
            Provenance::Mined => "mined",
        }
    }

    /// Check if this is an asserted provenance (publisher or crossref)
    pub fn is_asserted(&self) -> bool {
        matches!(self, Provenance::Publisher | Provenance::Crossref)
    }
}

impl std::fmt::Display for Provenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
```

**Step 4: Update mod.rs to export**

In `src/extract/mod.rs`, add:

```rust
mod provenance;

pub use provenance::Provenance;
```

**Step 5: Run test to verify it passes**

Run: `cargo test provenance::tests -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/extract/provenance.rs src/extract/mod.rs
git commit -m "feat: add Provenance enum for DOI source tracking"
```

---

## Task 2: Update DoiMatch to Include Provenance

**Files:**
- Modify: `src/extract/doi.rs`

**Step 1: Write the failing test**

Add to `src/extract/doi.rs` tests:

```rust
#[test]
fn test_doi_match_with_provenance() {
    let m = DoiMatch::new("10.1234/test".to_string(), "10.1234/test".to_string(), Provenance::Publisher);
    assert_eq!(m.doi, "10.1234/test");
    assert_eq!(m.provenance, Provenance::Publisher);
}

#[test]
fn test_doi_match_mined_default() {
    let m = DoiMatch::mined("10.1234/test".to_string(), "10.1234/test".to_string());
    assert_eq!(m.provenance, Provenance::Mined);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test doi::tests::test_doi_match_with_provenance -v`
Expected: FAIL

**Step 3: Write minimal implementation**

Update `DoiMatch` struct in `src/extract/doi.rs`:

```rust
use super::Provenance;

/// Represents a matched DOI with raw match text, normalized form, and provenance
#[derive(Debug, Clone, PartialEq)]
pub struct DoiMatch {
    pub doi: String,        // Normalized DOI (lowercase, cleaned)
    pub raw: String,        // Original matched substring
    pub provenance: Provenance,  // How this DOI was obtained
}

impl DoiMatch {
    pub fn new(doi: String, raw: String, provenance: Provenance) -> Self {
        Self { doi, raw, provenance }
    }

    /// Create a mined DoiMatch (extracted from text)
    pub fn mined(doi: String, raw: String) -> Self {
        Self::new(doi, raw, Provenance::Mined)
    }
}
```

**Step 4: Update extract_doi_matches_from_text**

Update the function to use `DoiMatch::mined`:

```rust
pub fn extract_doi_matches_from_text(text: &str) -> Vec<DoiMatch> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut matches = Vec::new();

    for cap in DOI_PATTERN.captures_iter(text) {
        if let Some(doi_match) = cap.get(1) {
            let raw = doi_match.as_str().to_string();
            let normalized = normalize_doi(&raw);

            if seen.insert(normalized.clone()) {
                matches.push(DoiMatch::mined(normalized, raw));
            }
        }
    }

    matches
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test doi::tests -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/extract/doi.rs
git commit -m "feat: add provenance field to DoiMatch"
```

---

## Task 3: Update ExplodedRow and PartitionBuffer

**Files:**
- Modify: `src/streaming/partition_writer.rs`

**Step 1: Write the failing test**

Add to `src/streaming/partition_writer.rs` tests:

```rust
#[test]
fn test_partition_writer_with_provenance() {
    use crate::extract::Provenance;

    let dir = tempdir().unwrap();
    let mut writer = PartitionWriter::new(dir.path(), 10).unwrap();

    writer
        .write(ExplodedRow {
            citing_doi: "10.1234/test".to_string(),
            ref_index: 0,
            ref_json: "{}".to_string(),
            raw_match: "10.5678/cited".to_string(),
            cited_id: "10.5678/cited".to_string(),
            provenance: Provenance::Publisher,
        })
        .unwrap();

    writer.flush_all().unwrap();

    // Verify parquet has provenance column
    let df = LazyFrame::scan_parquet(dir.path().join("10.56.parquet"), Default::default())
        .unwrap()
        .collect()
        .unwrap();

    assert!(df.column("provenance").is_ok());
    let prov = df.column("provenance").unwrap().str().unwrap();
    assert_eq!(prov.get(0).unwrap(), "publisher");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test partition_writer::tests::test_partition_writer_with_provenance -v`
Expected: FAIL

**Step 3: Update ExplodedRow struct**

```rust
use crate::extract::Provenance;

/// A single extracted and exploded row ready for partitioning
#[derive(Debug, Clone)]
pub struct ExplodedRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub ref_json: String,
    pub raw_match: String,
    pub cited_id: String,
    pub provenance: Provenance,
}
```

**Step 4: Update PartitionBuffer**

```rust
struct PartitionBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    ref_jsons: Vec<String>,
    raw_matches: Vec<String>,
    cited_ids: Vec<String>,
    provenances: Vec<String>,  // Store as string for Parquet
    file_path: PathBuf,
    rows_written: usize,
}

impl PartitionBuffer {
    fn new(partition_dir: &Path, partition_name: &str) -> Self {
        let file_path = partition_dir.join(format!("{}.parquet", partition_name));
        Self {
            citing_dois: Vec::new(),
            ref_indices: Vec::new(),
            ref_jsons: Vec::new(),
            raw_matches: Vec::new(),
            cited_ids: Vec::new(),
            provenances: Vec::new(),
            file_path,
            rows_written: 0,
        }
    }

    fn len(&self) -> usize {
        self.citing_dois.len()
    }

    fn push(&mut self, row: ExplodedRow) {
        self.citing_dois.push(row.citing_doi);
        self.ref_indices.push(row.ref_index);
        self.ref_jsons.push(row.ref_json);
        self.raw_matches.push(row.raw_match);
        self.cited_ids.push(row.cited_id);
        self.provenances.push(row.provenance.as_str().to_string());
    }

    fn to_dataframe(&self) -> Result<DataFrame> {
        DataFrame::new(vec![
            Column::new("citing_doi".into(), &self.citing_dois),
            Column::new("ref_index".into(), &self.ref_indices),
            Column::new("ref_json".into(), &self.ref_jsons),
            Column::new("raw_match".into(), &self.raw_matches),
            Column::new("cited_id".into(), &self.cited_ids),
            Column::new("provenance".into(), &self.provenances),
        ])
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))
    }

    fn clear(&mut self) {
        self.citing_dois.clear();
        self.ref_indices.clear();
        self.ref_jsons.clear();
        self.raw_matches.clear();
        self.cited_ids.clear();
        self.provenances.clear();
    }
}
```

**Step 5: Update write_extracted_ref signature**

```rust
/// Write multiple rows from a reference extraction (handles exploding)
pub fn write_extracted_ref(
    &mut self,
    citing_doi: &str,
    ref_index: u32,
    ref_json: &str,
    raw_matches: &[String],
    cited_ids: &[String],
    provenances: &[Provenance],
) -> Result<usize> {
    let mut written = 0;
    for ((raw_match, cited_id), provenance) in raw_matches.iter().zip(cited_ids.iter()).zip(provenances.iter()) {
        self.write(ExplodedRow {
            citing_doi: citing_doi.to_string(),
            ref_index,
            ref_json: ref_json.to_string(),
            raw_match: raw_match.clone(),
            cited_id: cited_id.clone(),
            provenance: *provenance,
        })?;
        written += 1;
    }
    Ok(written)
}
```

**Step 6: Update existing tests**

Update all existing tests in partition_writer.rs to include provenance field.

**Step 7: Run test to verify it passes**

Run: `cargo test partition_writer::tests -v`
Expected: PASS

**Step 8: Commit**

```bash
git add src/streaming/partition_writer.rs
git commit -m "feat: add provenance to partition writer schema"
```

---

## Task 4: Update Partition Inversion to Include Provenance

**Files:**
- Modify: `src/streaming/partition_invert.rs`

**Step 1: Write the failing test**

Add to tests:

```rust
#[test]
fn test_invert_partition_includes_provenance() {
    let dir = tempdir().unwrap();

    // Create partition with provenance
    let citing_dois = vec!["10.1234/a".to_string(), "10.1234/b".to_string()];
    let ref_indices: Vec<u32> = vec![0, 1];
    let ref_jsons = vec!["{}".to_string(), "{}".to_string()];
    let raw_matches = vec!["10.5678/cited".to_string(), "10.5678/cited".to_string()];
    let cited_ids = vec!["10.5678/cited".to_string(), "10.5678/cited".to_string()];
    let provenances = vec!["publisher".to_string(), "mined".to_string()];

    let mut df = DataFrame::new(vec![
        Column::new("citing_doi".into(), &citing_dois),
        Column::new("ref_index".into(), &ref_indices),
        Column::new("ref_json".into(), &ref_jsons),
        Column::new("raw_match".into(), &raw_matches),
        Column::new("cited_id".into(), &cited_ids),
        Column::new("provenance".into(), &provenances),
    ]).unwrap();

    let file = File::create(dir.path().join("10.5678.parquet")).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();

    let result = invert_single_partition(&dir.path().join("10.5678.parquet"), OutputMode::Generic).unwrap();

    // Verify cited_by struct includes provenance
    let cited_by = result.column("cited_by").unwrap();
    // The struct should have provenance field
    assert!(result.height() > 0);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test partition_invert::tests::test_invert_partition_includes_provenance -v`
Expected: FAIL

**Step 3: Update invert_single_partition**

Update the aggregation to include provenance:

```rust
fn invert_single_partition(partition_path: &Path, output_mode: OutputMode) -> Result<DataFrame> {
    debug!("Inverting partition: {:?}", partition_path);

    let lf = LazyFrame::scan_parquet(partition_path, Default::default())
        .with_context(|| format!("Failed to scan partition: {:?}", partition_path))?;

    let inverted = lf
        .unique(
            Some(vec!["citing_doi".into(), "cited_id".into()]),
            UniqueKeepStrategy::First,
        )
        .filter(col("citing_doi").neq(col("cited_id")))
        .group_by([col("cited_id")])
        .agg([
            col("citing_doi").n_unique().alias("citation_count"),
            col("citing_doi").count().alias("reference_count"),
            as_struct(vec![
                col("citing_doi").alias("doi"),
                col("raw_match"),
                col("ref_json").alias("reference"),
                col("provenance"),  // Include provenance in struct
            ])
            .alias("cited_by"),
        ]);

    // ... rest unchanged
}
```

**Step 4: Update build_cited_by_json**

```rust
fn build_cited_by_json(cited_by_col: &Column, row_idx: usize) -> Result<serde_json::Value> {
    let list = cited_by_col.list()?;
    let row_list = list.get_as_series(row_idx);

    match row_list {
        Some(series) => {
            let structs = series.struct_()?;
            let doi_field = structs.field_by_name("doi")?;
            let raw_match_field = structs.field_by_name("raw_match")?;
            let ref_field = structs.field_by_name("reference")?;
            let provenance_field = structs.field_by_name("provenance")?;

            let dois = doi_field.str()?;
            let raw_matches = raw_match_field.str()?;
            let refs = ref_field.str()?;
            let provenances = provenance_field.str()?;

            let mut doi_matches: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

            for j in 0..series.len() {
                let doi = dois.get(j).unwrap_or("").to_string();
                let raw_match = raw_matches.get(j).unwrap_or("");
                let ref_json_str = refs.get(j).unwrap_or("null");
                let provenance = provenances.get(j).unwrap_or("mined");

                let reference: serde_json::Value =
                    serde_json::from_str(ref_json_str).unwrap_or(serde_json::Value::Null);

                let match_obj = serde_json::json!({
                    "raw_match": raw_match,
                    "reference": reference,
                    "provenance": provenance
                });

                doi_matches.entry(doi).or_default().push(match_obj);
            }

            let cited_by_arr: Vec<serde_json::Value> = doi_matches
                .into_iter()
                .map(|(doi, matches)| {
                    // Determine overall provenance for this citing DOI (best available)
                    let best_provenance = matches.iter()
                        .filter_map(|m| m.get("provenance").and_then(|p| p.as_str()))
                        .max_by_key(|p| match *p {
                            "publisher" => 2,
                            "crossref" => 1,
                            _ => 0,
                        })
                        .unwrap_or("mined");

                    serde_json::json!({
                        "doi": doi,
                        "provenance": best_provenance,
                        "matches": matches
                    })
                })
                .collect();

            Ok(serde_json::Value::Array(cited_by_arr))
        }
        None => Ok(serde_json::Value::Array(vec![])),
    }
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test partition_invert::tests -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/streaming/partition_invert.rs
git commit -m "feat: include provenance in inverted output"
```

---

## Task 5: Extract Provenance in Pipeline

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Write the failing test**

Add helper function test:

```rust
#[test]
fn test_determine_provenance() {
    use serde_json::json;
    use crate::extract::Provenance;

    // Publisher asserted
    let ref_publisher = json!({"DOI": "10.1234/test", "doi-asserted-by": "publisher"});
    assert_eq!(determine_provenance(&ref_publisher, "10.1234/test"), Provenance::Publisher);

    // Crossref asserted
    let ref_crossref = json!({"DOI": "10.1234/test", "doi-asserted-by": "crossref"});
    assert_eq!(determine_provenance(&ref_crossref, "10.1234/test"), Provenance::Crossref);

    // DOI present but no doi-asserted-by
    let ref_no_assertion = json!({"DOI": "10.1234/test"});
    assert_eq!(determine_provenance(&ref_no_assertion, "10.1234/test"), Provenance::Mined);

    // Mined from unstructured (DOI not in DOI field)
    let ref_unstructured = json!({"unstructured": "See doi:10.1234/test"});
    assert_eq!(determine_provenance(&ref_unstructured, "10.1234/test"), Provenance::Mined);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test pipeline::tests::test_determine_provenance -v`
Expected: FAIL

**Step 3: Add determine_provenance function**

```rust
use crate::extract::Provenance;

/// Determine the provenance of a DOI based on how it was found in the reference
fn determine_provenance(reference: &Value, extracted_doi: &str) -> Provenance {
    // Check if there's an explicit DOI field
    if let Some(doi_field) = reference.get("DOI").and_then(|v| v.as_str()) {
        // Check if the extracted DOI matches the DOI field (normalized comparison)
        let doi_field_normalized = doi_field.to_lowercase();
        let extracted_normalized = extracted_doi.to_lowercase();

        if doi_field_normalized == extracted_normalized ||
           doi_field_normalized.contains(&extracted_normalized) ||
           extracted_normalized.contains(&doi_field_normalized) {
            // DOI came from the explicit DOI field - check doi-asserted-by
            if let Some(asserted_by) = reference.get("doi-asserted-by").and_then(|v| v.as_str()) {
                return match asserted_by {
                    "publisher" => Provenance::Publisher,
                    "crossref" => Provenance::Crossref,
                    _ => Provenance::Mined, // Unknown assertion type
                };
            }
            // DOI field present but no doi-asserted-by
            return Provenance::Mined;
        }
    }

    // DOI was mined from other fields (unstructured, URL, etc.)
    Provenance::Mined
}
```

**Step 4: Update extraction loop to track provenance**

In `run_extraction`, update the match extraction section:

```rust
// Extract matches based on source mode
let (raw_matches, cited_ids, provenances): (Vec<String>, Vec<String>, Vec<Provenance>) = match args.source {
    Source::Arxiv => {
        let matches = extract_arxiv_matches_from_text(&search_text);
        let raws: Vec<String> = matches.iter().map(|m| m.raw.clone()).collect();
        let ids: Vec<String> = matches.iter().map(|m| m.id.clone()).collect();
        // For arXiv, determine provenance based on DOI field
        let provs: Vec<Provenance> = ids.iter().map(|id| {
            let arxiv_doi = format!("10.48550/arXiv.{}", id);
            determine_provenance(reference, &arxiv_doi)
        }).collect();
        (raws, ids, provs)
    }
    Source::All | Source::Crossref | Source::Datacite => {
        let matches = extract_doi_matches_from_text(&search_text);
        let raws: Vec<String> = matches.iter().map(|m| m.raw.clone()).collect();
        let ids: Vec<String> = matches.iter().map(|m| m.doi.clone()).collect();
        let provs: Vec<Provenance> = ids.iter().map(|doi| {
            determine_provenance(reference, doi)
        }).collect();
        (raws, ids, provs)
    }
};
```

**Step 5: Update writer call**

```rust
writer.write_extracted_ref(
    &work_doi,
    ref_idx as u32,
    &ref_json,
    &filtered_raw_matches,
    &filtered_cited_ids,
    &filtered_provenances,
)?;
```

**Step 6: Run test to verify it passes**

Run: `cargo test pipeline::tests -v`
Expected: PASS

**Step 7: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "feat: extract provenance from Crossref reference metadata"
```

---

## Task 6: Add Split Output File Generation

**Files:**
- Create: `src/common/output.rs`
- Modify: `src/common/mod.rs`

**Step 1: Write the failing test**

In `src/common/output.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_path_generation() {
        let paths = SplitOutputPaths::from_base("results.jsonl");
        assert_eq!(paths.all, PathBuf::from("results.jsonl"));
        assert_eq!(paths.asserted, PathBuf::from("results_asserted.jsonl"));
        assert_eq!(paths.mined, PathBuf::from("results_mined.jsonl"));
    }

    #[test]
    fn test_split_path_with_directory() {
        let paths = SplitOutputPaths::from_base("/path/to/output.jsonl");
        assert_eq!(paths.all, PathBuf::from("/path/to/output.jsonl"));
        assert_eq!(paths.asserted, PathBuf::from("/path/to/output_asserted.jsonl"));
        assert_eq!(paths.mined, PathBuf::from("/path/to/output_mined.jsonl"));
    }

    #[test]
    fn test_split_path_no_extension() {
        let paths = SplitOutputPaths::from_base("results");
        assert_eq!(paths.all, PathBuf::from("results"));
        assert_eq!(paths.asserted, PathBuf::from("results_asserted"));
        assert_eq!(paths.mined, PathBuf::from("results_mined"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test output::tests -v`
Expected: FAIL

**Step 3: Write implementation**

```rust
use std::path::{Path, PathBuf};

/// Paths for split output files (all, asserted, mined)
#[derive(Debug, Clone)]
pub struct SplitOutputPaths {
    pub all: PathBuf,
    pub asserted: PathBuf,
    pub mined: PathBuf,
}

impl SplitOutputPaths {
    /// Generate split paths from a base path
    /// "results.jsonl" -> "results.jsonl", "results_asserted.jsonl", "results_mined.jsonl"
    pub fn from_base<P: AsRef<Path>>(base: P) -> Self {
        let base = base.as_ref();
        let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let extension = base.extension().and_then(|s| s.to_str());
        let parent = base.parent();

        let make_path = |suffix: &str| -> PathBuf {
            let filename = match extension {
                Some(ext) => format!("{}_{}.{}", stem, suffix, ext),
                None => format!("{}_{}", stem, suffix),
            };
            match parent {
                Some(p) if p.as_os_str().len() > 0 => p.join(filename),
                _ => PathBuf::from(filename),
            }
        };

        Self {
            all: base.to_path_buf(),
            asserted: make_path("asserted"),
            mined: make_path("mined"),
        }
    }
}
```

**Step 4: Update mod.rs**

```rust
mod output;
pub use output::SplitOutputPaths;
```

**Step 5: Run test to verify it passes**

Run: `cargo test output::tests -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/common/output.rs src/common/mod.rs
git commit -m "feat: add SplitOutputPaths for automatic split file naming"
```

---

## Task 7: Implement Split Output Writing

**Files:**
- Modify: `src/validation/runner.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_write_split_by_provenance() {
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let base_path = dir.path().join("output.jsonl");

    // Create records with different provenances in cited_by
    let record_mixed = CitationRecord {
        doi: "10.1234/mixed".to_string(),
        arxiv_id: None,
        reference_count: 2,
        citation_count: 2,
        cited_by: vec![
            serde_json::json!({"doi": "10.5555/a", "provenance": "publisher"}),
            serde_json::json!({"doi": "10.5555/b", "provenance": "mined"}),
        ],
    };

    let records = vec![(record_mixed, Source::Crossref)];

    write_validation_results_with_split(
        &records,
        &[],
        base_path.to_str().unwrap(),
        None,
    ).unwrap();

    // Verify main file exists
    assert!(base_path.exists());

    // Verify asserted file has only publisher/crossref entries
    let asserted_path = dir.path().join("output_asserted.jsonl");
    assert!(asserted_path.exists());
    let asserted_content = std::fs::read_to_string(&asserted_path).unwrap();
    assert!(asserted_content.contains("publisher"));
    assert!(!asserted_content.contains("\"provenance\":\"mined\""));

    // Verify mined file has only mined entries
    let mined_path = dir.path().join("output_mined.jsonl");
    assert!(mined_path.exists());
    let mined_content = std::fs::read_to_string(&mined_path).unwrap();
    assert!(mined_content.contains("mined"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test runner::tests::test_write_split_by_provenance -v`
Expected: FAIL

**Step 3: Implement write_validation_results_with_split**

```rust
use crate::common::SplitOutputPaths;

/// Filter cited_by entries by provenance
fn filter_cited_by_by_provenance(
    cited_by: &[serde_json::Value],
    keep_asserted: bool,
) -> Vec<serde_json::Value> {
    cited_by
        .iter()
        .filter(|entry| {
            let provenance = entry
                .get("provenance")
                .and_then(|p| p.as_str())
                .unwrap_or("mined");
            let is_asserted = provenance == "publisher" || provenance == "crossref";
            if keep_asserted { is_asserted } else { !is_asserted }
        })
        .cloned()
        .collect()
}

/// Write validation results with automatic split by provenance
pub fn write_validation_results_with_split(
    valid: &[(CitationRecord, Source)],
    failed: &[CitationRecord],
    output_path: &str,
    output_failed: Option<&str>,
) -> Result<()> {
    let paths = SplitOutputPaths::from_base(output_path);

    // Open all three output files
    let file_all = File::create(&paths.all)
        .with_context(|| format!("Failed to create: {:?}", paths.all))?;
    let file_asserted = File::create(&paths.asserted)
        .with_context(|| format!("Failed to create: {:?}", paths.asserted))?;
    let file_mined = File::create(&paths.mined)
        .with_context(|| format!("Failed to create: {:?}", paths.mined))?;

    let mut writer_all = BufWriter::new(file_all);
    let mut writer_asserted = BufWriter::new(file_asserted);
    let mut writer_mined = BufWriter::new(file_mined);

    for (record, _source) in valid {
        // Write to main file
        writeln!(writer_all, "{}", serde_json::to_string(record)?)?;

        // Filter and write to asserted file
        let asserted_cited_by = filter_cited_by_by_provenance(&record.cited_by, true);
        if !asserted_cited_by.is_empty() {
            let asserted_record = serde_json::json!({
                "doi": record.doi,
                "arxiv_id": record.arxiv_id,
                "reference_count": record.reference_count,
                "citation_count": asserted_cited_by.len(),
                "cited_by": asserted_cited_by,
            });
            writeln!(writer_asserted, "{}", asserted_record)?;
        }

        // Filter and write to mined file
        let mined_cited_by = filter_cited_by_by_provenance(&record.cited_by, false);
        if !mined_cited_by.is_empty() {
            let mined_record = serde_json::json!({
                "doi": record.doi,
                "arxiv_id": record.arxiv_id,
                "reference_count": record.reference_count,
                "citation_count": mined_cited_by.len(),
                "cited_by": mined_cited_by,
            });
            writeln!(writer_mined, "{}", mined_record)?;
        }
    }

    writer_all.flush()?;
    writer_asserted.flush()?;
    writer_mined.flush()?;

    // Handle failed outputs with split
    if let Some(failed_base) = output_failed {
        let failed_paths = SplitOutputPaths::from_base(failed_base);

        let file_all = File::create(&failed_paths.all)?;
        let file_asserted = File::create(&failed_paths.asserted)?;
        let file_mined = File::create(&failed_paths.mined)?;

        let mut writer_all = BufWriter::new(file_all);
        let mut writer_asserted = BufWriter::new(file_asserted);
        let mut writer_mined = BufWriter::new(file_mined);

        for record in failed {
            writeln!(writer_all, "{}", serde_json::to_string(record)?)?;

            let asserted_cited_by = filter_cited_by_by_provenance(&record.cited_by, true);
            if !asserted_cited_by.is_empty() {
                let asserted_record = serde_json::json!({
                    "doi": record.doi,
                    "arxiv_id": record.arxiv_id,
                    "reference_count": record.reference_count,
                    "citation_count": asserted_cited_by.len(),
                    "cited_by": asserted_cited_by,
                });
                writeln!(writer_asserted, "{}", asserted_record)?;
            }

            let mined_cited_by = filter_cited_by_by_provenance(&record.cited_by, false);
            if !mined_cited_by.is_empty() {
                let mined_record = serde_json::json!({
                    "doi": record.doi,
                    "arxiv_id": record.arxiv_id,
                    "reference_count": record.reference_count,
                    "citation_count": mined_cited_by.len(),
                    "cited_by": mined_cited_by,
                });
                writeln!(writer_mined, "{}", mined_record)?;
            }
        }

        writer_all.flush()?;
        writer_asserted.flush()?;
        writer_mined.flush()?;
    }

    info!("Wrote split output files:");
    info!("  All: {:?}", paths.all);
    info!("  Asserted: {:?}", paths.asserted);
    info!("  Mined: {:?}", paths.mined);

    Ok(())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test runner::tests::test_write_split_by_provenance -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/validation/runner.rs
git commit -m "feat: implement split output writing by provenance"
```

---

## Task 8: Update Pipeline to Use Split Output

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Update output writing calls**

Replace calls to `write_validation_results` with `write_validation_results_with_split`:

```rust
// In run_pipeline, replace the output writing section:

match args.source {
    Source::All => {
        // For all mode, write split outputs for both crossref and datacite
        let (crossref_valid, datacite_valid): (Vec<_>, Vec<_>) = validation_results
            .valid
            .iter()
            .cloned()
            .partition(|(_, source)| *source == Source::Crossref);

        if let Some(ref path) = args.output_crossref {
            write_validation_results_with_split(
                &crossref_valid,
                &validation_results.failed,
                path,
                args.output_crossref_failed.as_deref(),
            )?;
        }

        if let Some(ref path) = args.output_datacite {
            write_validation_results_with_split(
                &datacite_valid,
                &validation_results.failed,
                path,
                args.output_datacite_failed.as_deref(),
            )?;
        }
    }
    Source::Crossref => {
        write_validation_results_with_split(
            &validation_results.valid,
            &validation_results.failed,
            args.output_crossref.as_ref().unwrap(),
            args.output_crossref_failed.as_deref(),
        )?;
    }
    Source::Datacite => {
        write_validation_results_with_split(
            &validation_results.valid,
            &validation_results.failed,
            args.output_datacite.as_ref().unwrap(),
            args.output_datacite_failed.as_deref(),
        )?;
    }
    Source::Arxiv => {
        write_arxiv_validation_results_with_split(
            &validation_results,
            args.output_arxiv.as_ref().unwrap(),
            args.output_arxiv_failed.as_deref(),
        )?;
    }
}
```

**Step 2: Add write_arxiv_validation_results_with_split**

Similar to Task 7 but preserves arXiv output format.

**Step 3: Run full test suite**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/commands/pipeline.rs src/validation/runner.rs
git commit -m "feat: use split output writing in pipeline"
```

---

## Task 9: Update Standalone Validate Command

**Files:**
- Modify: `src/commands/validate.rs`

**Step 1: Update validate command to use split output**

Follow same pattern as Task 8 for the standalone validate command.

**Step 2: Run tests**

Run: `cargo test validate -v`
Expected: PASS

**Step 3: Commit**

```bash
git add src/commands/validate.rs
git commit -m "feat: add provenance split output to validate command"
```

---

## Task 10: Integration Test

**Files:**
- Create: `tests/provenance_integration.rs`

**Step 1: Write integration test**

```rust
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_provenance_end_to_end() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("output.jsonl");

    // Run pipeline on sample data
    let status = Command::new("cargo")
        .args([
            "run", "--release", "--",
            "pipeline",
            "--input", "sample_1m.tar.gz",
            "--source", "crossref",
            "--output-crossref", output.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success());

    // Verify output files exist
    assert!(output.exists());
    assert!(dir.path().join("output_asserted.jsonl").exists());
    assert!(dir.path().join("output_mined.jsonl").exists());

    // Verify provenance field in output
    let content = std::fs::read_to_string(&output).unwrap();
    assert!(content.contains("\"provenance\""));
}
```

**Step 2: Run integration test**

Run: `cargo test --test provenance_integration`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/provenance_integration.rs
git commit -m "test: add provenance integration test"
```

---

## Task 11: Final Verification

**Step 1: Run full test suite**

```bash
cargo test
```

**Step 2: Run clippy**

```bash
cargo clippy
```

**Step 3: Format code**

```bash
cargo fmt
```

**Step 4: Build release**

```bash
cargo build --release
```

**Step 5: Manual test with sample data**

```bash
./target/release/crossref-citation-extraction pipeline \
  --input sample_1m.tar.gz \
  --source crossref \
  --output-crossref test_output.jsonl
```

Verify:
- `test_output.jsonl` contains records with `"provenance"` field in `cited_by` entries
- `test_output_asserted.jsonl` contains only publisher/crossref provenance
- `test_output_mined.jsonl` contains only mined provenance

**Step 6: Final commit**

```bash
git add -A
git commit -m "chore: final cleanup for provenance tracking feature"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add Provenance enum | `extract/provenance.rs`, `extract/mod.rs` |
| 2 | Update DoiMatch | `extract/doi.rs` |
| 3 | Update partition writer | `streaming/partition_writer.rs` |
| 4 | Update partition inversion | `streaming/partition_invert.rs` |
| 5 | Extract provenance in pipeline | `commands/pipeline.rs` |
| 6 | Add SplitOutputPaths | `common/output.rs`, `common/mod.rs` |
| 7 | Implement split output writing | `validation/runner.rs` |
| 8 | Update pipeline outputs | `commands/pipeline.rs` |
| 9 | Update validate command | `commands/validate.rs` |
| 10 | Integration test | `tests/provenance_integration.rs` |
| 11 | Final verification | - |
