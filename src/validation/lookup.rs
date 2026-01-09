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

        let result = lookup_doi(
            "10.1234/crossref",
            Source::All,
            Some(&crossref),
            Some(&datacite),
        );
        assert_eq!(result, LookupResult::Found(Source::Crossref));

        let result = lookup_doi(
            "10.48550/datacite",
            Source::All,
            Some(&crossref),
            Some(&datacite),
        );
        assert_eq!(result, LookupResult::Found(Source::Datacite));

        let result = lookup_doi(
            "10.9999/unknown",
            Source::All,
            Some(&crossref),
            Some(&datacite),
        );
        assert_eq!(result, LookupResult::NotFound);
    }
}
