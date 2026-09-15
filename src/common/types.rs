use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Represents a single arXiv match with normalized ID and raw matched text
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivMatch {
    pub id: String,
    pub raw: String,
    pub arxiv_doi: String,
}

impl ArxivMatch {
    pub fn new(id: String, raw: String) -> Self {
        let arxiv_doi = format!("10.48550/arXiv.{}", id);
        Self { id, raw, arxiv_doi }
    }
}

/// The kind of evidence behind a confirmed reference match.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchType {
    /// A structured `DOI` field, a bare or `doi:`-wrapped DOI in text, or a
    /// DOI resolver URL.
    Doi,
    /// An arXiv identifier recognized by the arXiv parser.
    Arxiv,
    /// A registered landing-page URL.
    Url,
}

impl MatchType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MatchType::Doi => "doi",
            MatchType::Arxiv => "arxiv",
            MatchType::Url => "url",
        }
    }

    /// Parse exactly `doi`, `arxiv`, or `url`.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "doi" => Ok(MatchType::Doi),
            "arxiv" => Ok(MatchType::Arxiv),
            "url" => Ok(MatchType::Url),
            other => bail!("unknown match type {other:?}; expected doi, arxiv, or url"),
        }
    }
}

impl std::fmt::Display for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_type_round_trips_lowercase_values() {
        for (kind, text) in [
            (MatchType::Doi, "doi"),
            (MatchType::Arxiv, "arxiv"),
            (MatchType::Url, "url"),
        ] {
            assert_eq!(kind.as_str(), text);
            assert_eq!(kind.to_string(), text);
            assert_eq!(MatchType::parse(text).unwrap(), kind);
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{text}\""));
            assert_eq!(
                serde_json::from_str::<MatchType>(&format!("\"{text}\"")).unwrap(),
                kind
            );
        }
        assert!(MatchType::parse("DOI").is_err());
        assert!(MatchType::parse("").is_err());
        assert!(serde_json::from_str::<MatchType>("\"Doi\"").is_err());
    }
}
