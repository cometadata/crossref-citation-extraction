use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;

use crate::common::ArxivMatch;

lazy_static! {
    // Modern arXiv ID format: YYMM.NNNNN with 4-6 digits after decimal
    // Allows punctuation/whitespace after "arxiv": arXiv:, arXiv., arXiv , etc.
    // Captures the full match for raw string
    pub static ref ARXIV_MODERN_PATTERN: Regex = Regex::new(
        r"(?i)(arxiv[.:\s]+(\d{4}\.\d{4,6}(?:v\d+)?))"
    ).unwrap();

    // Old format: category.subcategory/NNNNNNN or category-sub/NNNNNNN
    // Allows dots, hyphens in category, optional whitespace before/within number
    // Examples: arXiv:cs.DM/9910013, arXiv:hep-ph/9901234, arXiv:cs.DM/ 9910013
    pub static ref ARXIV_OLD_FORMAT_PATTERN: Regex = Regex::new(
        r"(?i)(arxiv[.:\s]+([a-z][a-z0-9.-]*/\s*\d{7}(?:v\d+)?))"
    ).unwrap();

    // arXiv DOI format: 10.48550/arXiv.YYMM.NNNNN
    pub static ref ARXIV_DOI_PATTERN: Regex = Regex::new(
        r"(?i)(10\.48550/arxiv\.(\d{4}\.\d{4,6}(?:v\d+)?))"
    ).unwrap();

    // arXiv URL format: arxiv.org/abs/YYMM.NNNNN or arxiv.org/pdf/YYMM.NNNNN
    pub static ref ARXIV_URL_PATTERN: Regex = Regex::new(
        r"(?i)(arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,6}(?:v\d+)?|[a-z][a-z0-9.-]*/\d{7}(?:v\d+)?))"
    ).unwrap();
}

/// Normalize an arXiv ID by converting to lowercase, removing whitespace, and stripping version
pub fn normalize_arxiv_id(id: &str) -> String {
    let mut id = id.to_lowercase();
    id = id.chars().filter(|c| !c.is_whitespace()).collect();

    // Strip version suffix (e.g., "2403.03542v1" -> "2403.03542")
    if let Some(pos) = id.find('v') {
        if pos + 1 < id.len() && id[pos+1..].chars().all(|c| c.is_ascii_digit()) {
            return id[..pos].to_string();
        }
    }
    id
}

/// Extract arXiv matches from text using all pattern types
pub fn extract_arxiv_matches_from_text(text: &str) -> Vec<ArxivMatch> {
    let mut matches: HashMap<String, ArxivMatch> = HashMap::new();

    for cap in ARXIV_MODERN_PATTERN.captures_iter(text) {
        if let (Some(raw), Some(id)) = (cap.get(1), cap.get(2)) {
            let normalized = normalize_arxiv_id(id.as_str());
            matches.entry(normalized.clone()).or_insert_with(|| {
                ArxivMatch::new(normalized, raw.as_str().to_string())
            });
        }
    }

    for cap in ARXIV_OLD_FORMAT_PATTERN.captures_iter(text) {
        if let (Some(raw), Some(id)) = (cap.get(1), cap.get(2)) {
            let normalized = normalize_arxiv_id(id.as_str());
            matches.entry(normalized.clone()).or_insert_with(|| {
                ArxivMatch::new(normalized, raw.as_str().to_string())
            });
        }
    }

    for cap in ARXIV_DOI_PATTERN.captures_iter(text) {
        if let (Some(raw), Some(id)) = (cap.get(1), cap.get(2)) {
            let normalized = normalize_arxiv_id(id.as_str());
            matches.entry(normalized.clone()).or_insert_with(|| {
                ArxivMatch::new(normalized, raw.as_str().to_string())
            });
        }
    }

    for cap in ARXIV_URL_PATTERN.captures_iter(text) {
        if let (Some(raw), Some(id)) = (cap.get(1), cap.get(2)) {
            let normalized = normalize_arxiv_id(id.as_str());
            matches.entry(normalized.clone()).or_insert_with(|| {
                ArxivMatch::new(normalized, raw.as_str().to_string())
            });
        }
    }

    matches.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_arxiv_modern_format() {
        let text = "arXiv:2403.03542";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2403.03542");
        assert_eq!(matches[0].arxiv_doi, "10.48550/arXiv.2403.03542");
        assert!(matches[0].raw.contains("2403.03542"));
    }

    #[test]
    fn test_extract_arxiv_with_version() {
        let text = "arXiv:2403.03542v2";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2403.03542"); // Version stripped
        assert_eq!(matches[0].arxiv_doi, "10.48550/arXiv.2403.03542");
    }

    #[test]
    fn test_extract_arxiv_old_format() {
        let text = "arXiv:hep-ph/9901234";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "hep-ph/9901234");
        assert_eq!(matches[0].arxiv_doi, "10.48550/arXiv.hep-ph/9901234");
    }

    #[test]
    fn test_extract_arxiv_old_format_with_dots() {
        let text = "arXiv:cs.DM/9910013";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "cs.dm/9910013"); // Lowercase
    }

    #[test]
    fn test_extract_arxiv_old_format_with_space() {
        let text = "arXiv:cs.DM/ 9910013";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "cs.dm/9910013"); // Whitespace removed
    }

    #[test]
    fn test_extract_arxiv_six_digit_decimal() {
        let text = "ArXiv. 2206.153252";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2206.153252");
    }

    #[test]
    fn test_extract_arxiv_from_doi() {
        let text = "10.48550/arXiv.2403.03542";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2403.03542");
    }

    #[test]
    fn test_extract_arxiv_from_url() {
        let text = "https://arxiv.org/abs/2403.03542";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2403.03542");
    }

    #[test]
    fn test_no_match_without_arxiv_context() {
        let text = "Some paper 2403.03542";
        let matches = extract_arxiv_matches_from_text(text);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_normalize_arxiv_id() {
        assert_eq!(normalize_arxiv_id("2403.03542"), "2403.03542");
        assert_eq!(normalize_arxiv_id("2403.03542v2"), "2403.03542");
        assert_eq!(normalize_arxiv_id("CS.DM/9910013"), "cs.dm/9910013");
        assert_eq!(normalize_arxiv_id("cs.DM/ 9910013"), "cs.dm/9910013");
    }

    #[test]
    fn test_arxiv_match_doi_construction() {
        let m = ArxivMatch::new("2403.03542".to_string(), "arXiv:2403.03542".to_string());
        assert_eq!(m.arxiv_doi, "10.48550/arXiv.2403.03542");
    }
}
