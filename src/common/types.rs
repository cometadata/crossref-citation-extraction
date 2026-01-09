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

/// Simplified ArxivCitations for validate step (doesn't need full CitingWork structure)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ArxivCitationsSimple {
    pub arxiv_doi: String,
    pub arxiv_id: String,
    pub reference_count: usize,
    pub citation_count: usize,
    pub cited_by: Vec<Value>,
}

/// DataCite record - we only need the id (DOI)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DataCiteRecord {
    pub id: String,
}

/// Statistics from validate step
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ValidateStats {
    pub total_records: usize,
    pub matched_in_datacite: usize,
    pub resolution_resolved: usize,
    pub resolution_failed: usize,
    pub total_valid: usize,
    pub total_failed: usize,
}

/// Generic citation record for Crossref/DataCite output
/// Also handles arXiv format which uses arxiv_doi instead of doi
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationRecord {
    #[serde(alias = "arxiv_doi")]
    pub doi: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arxiv_id: Option<String>,
    pub reference_count: usize,
    pub citation_count: usize,
    pub cited_by: Vec<Value>,
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
