use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Represents a single arXiv match with normalized ID, raw matched text, and constructed DOI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivMatch {
    pub id: String,        // Normalized arXiv ID (lowercase, no version, no whitespace)
    pub raw: String,       // Original matched substring from text
    pub arxiv_doi: String, // Constructed DOI: 10.48550/arXiv.{id}
}

impl ArxivMatch {
    pub fn new(id: String, raw: String) -> Self {
        let arxiv_doi = format!("10.48550/arXiv.{}", id);
        Self { id, raw, arxiv_doi }
    }
}

/// Output from extract step: a DOI with its arXiv references
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivRefMatch {
    pub doi: String,
    pub arxiv_matches: Vec<ArxivMatch>,
    pub references: Vec<Value>,
}

/// A single reference match within a citing work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceMatch {
    pub raw_match: String,     // Original matched text
    pub reference: Value,      // Full reference object from citing work
}

/// A citing work with all its reference matches to this arXiv work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitingWork {
    pub doi: String,                    // DOI of the citing work
    pub matches: Vec<ReferenceMatch>,   // All references to this arXiv work
}

/// Output from invert step: arXiv work with all its citations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivCitations {
    pub arxiv_doi: String,
    pub arxiv_id: String,
    pub reference_count: usize,    // Total reference instances
    pub citation_count: usize,     // Unique citing works
    pub cited_by: Vec<CitingWork>,
}

/// Simplified ArxivCitations for validate step (doesn't need full CitingWork structure)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivCitationsSimple {
    pub arxiv_doi: String,
    pub arxiv_id: String,
    pub reference_count: usize,
    pub citation_count: usize,
    pub cited_by: Vec<Value>,
}

/// DataCite record - we only need the id (DOI)
#[derive(Debug, Deserialize)]
pub struct DataCiteRecord {
    pub id: String,
}

/// Statistics from extract step (vectorized extraction)
#[derive(Debug, Clone, Default)]
pub struct ExtractStats {
    pub total_references: usize,
    pub references_with_matches: usize,
    pub total_arxiv_ids: usize,
}

/// Statistics from invert step (hash-based aggregation)
#[derive(Debug, Clone, Default)]
pub struct InvertStats {
    pub total_rows_processed: usize,
    pub unique_arxiv_works: usize,
    pub unique_citing_works: usize,
}

/// Statistics from validate step
#[derive(Debug, Clone, Default)]
pub struct ValidateStats {
    pub total_records: usize,
    pub matched_in_datacite: usize,
    pub resolution_resolved: usize,
    pub resolution_failed: usize,
    pub total_valid: usize,
    pub total_failed: usize,
}

/// Statistics from convert step (tar.gz → Parquet)
#[derive(Debug, Clone, Default)]
pub struct ConvertStats {
    pub json_files_processed: usize,
    pub total_records: usize,
    pub total_references: usize,
    pub references_with_hint: usize,
}
