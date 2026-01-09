use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;

use super::Provenance;

lazy_static! {
    /// DOI pattern - captures DOI from various formats
    /// Matches: bare DOI, doi:prefix, URL forms
    pub static ref DOI_PATTERN: Regex = Regex::new(
        r#"(?i)(?:doi[:\s]*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d{4,}/[^\s\]\)>,;"']+)"#
    ).unwrap();
}

/// Represents a matched DOI with raw match text, normalized form, and provenance
#[derive(Debug, Clone, PartialEq)]
pub struct DoiMatch {
    pub doi: String,           // Normalized DOI (lowercase, cleaned)
    pub raw: String,           // Original matched substring
    pub provenance: Provenance, // How this DOI was obtained
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
                matches.push(DoiMatch::mined(normalized, raw));
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
        assert_eq!(
            doi_prefix("10.48550/arXiv.2403.12345"),
            Some("10.48550".to_string())
        );
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
}
