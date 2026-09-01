use regex::Regex;
use std::sync::LazyLock;

use crate::common::ArxivMatch;

/// Combined pattern matching all arXiv reference formats in a single scan.
///
/// Matches (in order of priority):
/// 1. DOI format: 10.48550/arXiv.YYMM.NNNNN (groups 1,2)
/// 2. URL format: arxiv.org/abs/... or arxiv.org/pdf/... (groups 3,4)
/// 3. Modern format: arXiv:YYMM.NNNNN (groups 5,6)
/// 4. Old format: arXiv:category/NNNNNNN (groups 7,8)
///
/// Using a single alternation pattern reduces text scanning from 4 passes to 1.
pub static ARXIV_COMBINED_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        # DOI format: 10.48550/arXiv.YYMM.NNNNN
        (10\.48550/arxiv\.(\d{4}\.\d{4,6}(?:v\d+)?))
        |
        # URL format: arxiv.org/abs/... or arxiv.org/pdf/...
        (arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,6}(?:v\d+)?|[a-z][a-z0-9.-]*/\d{7}(?:v\d+)?))
        |
        # Modern format: arXiv:YYMM.NNNNN (allows punctuation/whitespace after arxiv)
        (arxiv[.:\s]+(\d{4}\.\d{4,6}(?:v\d+)?))
        |
        # Old format: arXiv:category/NNNNNNN (e.g., hep-ph/9901234, cs.DM/9910013)
        (arxiv[.:\s]+([a-z][a-z0-9.-]*/\s*\d{7}(?:v\d+)?))
        ",
    )
    .unwrap()
});

/// Normalize an arXiv ID by converting to lowercase, removing whitespace, and stripping version
#[inline]
pub fn normalize_arxiv_id(id: &str) -> String {
    let mut id = id.to_lowercase();
    id.retain(|c| !c.is_whitespace());

    if let Some(pos) = id.find('v') {
        if pos + 1 < id.len() && id[pos + 1..].chars().all(|c| c.is_ascii_digit()) {
            id.truncate(pos);
        }
    }
    id
}

/// Extract arXiv matches from text using combined pattern (single scan).
/// Uses Vec + sort + dedup instead of HashMap for small match counts (common case).
pub fn extract_arxiv_matches_from_text(text: &str) -> Vec<ArxivMatch> {
    let mut matches: Vec<ArxivMatch> = Vec::new();

    for cap in ARXIV_COMBINED_PATTERN.captures_iter(text) {
        let (raw, id) = if let (Some(r), Some(i)) = (cap.get(1), cap.get(2)) {
            (r.as_str(), i.as_str())
        } else if let (Some(r), Some(i)) = (cap.get(3), cap.get(4)) {
            (r.as_str(), i.as_str())
        } else if let (Some(r), Some(i)) = (cap.get(5), cap.get(6)) {
            (r.as_str(), i.as_str())
        } else if let (Some(r), Some(i)) = (cap.get(7), cap.get(8)) {
            (r.as_str(), i.as_str())
        } else {
            continue;
        };

        let normalized = normalize_arxiv_id(id);
        matches.push(ArxivMatch::new(normalized, raw.to_string()));
    }

    if matches.len() > 1 {
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        matches.dedup_by(|a, b| a.id == b.id);
    }

    matches
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
        assert_eq!(matches[0].id, "2403.03542");
        assert_eq!(matches[0].arxiv_doi, "10.48550/arXiv.2403.03542");
        assert_eq!(matches[0].raw, "arXiv:2403.03542v2");
    }

    #[test]
    fn test_extract_arxiv_old_format() {
        let text = "arXiv:hep-ph/9901234";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "hep-ph/9901234");
        assert_eq!(matches[0].arxiv_doi, "10.48550/arXiv.hep-ph/9901234");
        assert_eq!(matches[0].raw, "arXiv:hep-ph/9901234");
    }

    #[test]
    fn test_extract_arxiv_old_format_with_dots() {
        let text = "arXiv:cs.DM/9910013";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "cs.dm/9910013");
    }

    #[test]
    fn test_extract_arxiv_old_format_with_space() {
        let text = "arXiv:cs.DM/ 9910013";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "cs.dm/9910013");
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
        assert_eq!(matches[0].raw, "10.48550/arXiv.2403.03542");
    }

    #[test]
    fn test_extract_arxiv_from_url() {
        let text = "https://arxiv.org/abs/2403.03542";
        let matches = extract_arxiv_matches_from_text(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, "2403.03542");
        assert_eq!(matches[0].raw, "arxiv.org/abs/2403.03542");
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
