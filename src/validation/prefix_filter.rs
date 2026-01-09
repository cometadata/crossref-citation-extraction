use crate::extract::doi_prefix;
use crate::index::DoiIndex;

/// Fast prefix-based filter for DOIs
/// Returns true if the DOI's prefix exists in any of the provided indexes
#[allow(dead_code)]
pub fn has_known_prefix(
    doi: &str,
    crossref: Option<&DoiIndex>,
    datacite: Option<&DoiIndex>,
) -> bool {
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
#[allow(dead_code)]
pub enum PrefixMatch {
    None,
    Crossref,
    Datacite,
    Both,
}

#[allow(dead_code)]
pub fn prefix_source(
    doi: &str,
    crossref: Option<&DoiIndex>,
    datacite: Option<&DoiIndex>,
) -> PrefixMatch {
    let prefix = match doi_prefix(doi) {
        Some(p) => p,
        None => return PrefixMatch::None,
    };

    let in_crossref = crossref.is_some_and(|idx| idx.has_prefix(&prefix));
    let in_datacite = datacite.is_some_and(|idx| idx.has_prefix(&prefix));

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

        assert!(has_known_prefix(
            "10.1234/other",
            Some(&crossref),
            Some(&datacite)
        ));
        assert!(has_known_prefix(
            "10.48550/arXiv.9999.99999",
            Some(&crossref),
            Some(&datacite)
        ));
        assert!(!has_known_prefix(
            "10.9999/unknown",
            Some(&crossref),
            Some(&datacite)
        ));
    }

    #[test]
    fn test_prefix_source() {
        let mut crossref = DoiIndex::new();
        crossref.insert("10.1234/example");

        let mut datacite = DoiIndex::new();
        datacite.insert("10.48550/arXiv.2403.12345");

        assert_eq!(
            prefix_source("10.1234/x", Some(&crossref), Some(&datacite)),
            PrefixMatch::Crossref
        );
        assert_eq!(
            prefix_source("10.48550/x", Some(&crossref), Some(&datacite)),
            PrefixMatch::Datacite
        );
        assert_eq!(
            prefix_source("10.9999/x", Some(&crossref), Some(&datacite)),
            PrefixMatch::None
        );
    }
}
