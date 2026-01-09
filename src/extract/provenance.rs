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
