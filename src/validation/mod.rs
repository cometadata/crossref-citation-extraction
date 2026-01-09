pub mod http;
pub mod lookup;
pub mod prefix_filter;
pub mod runner;

pub use http::*;
pub use lookup::*;
pub use prefix_filter::*;
pub use runner::*;

use crate::cli::Source;
use crate::index::DoiIndex;

/// Combined validation context for multi-source validation
pub struct ValidationContext {
    pub crossref_index: Option<DoiIndex>,
    pub datacite_index: Option<DoiIndex>,
    pub http_fallback_crossref: bool,
    pub http_fallback_datacite: bool,
    pub concurrency: usize,
    pub timeout_secs: u64,
}

impl ValidationContext {
    pub fn new() -> Self {
        Self {
            crossref_index: None,
            datacite_index: None,
            http_fallback_crossref: false,
            http_fallback_datacite: false,
            concurrency: 50,
            timeout_secs: 5,
        }
    }

    /// Check if we have the necessary indexes for a given source
    pub fn can_validate(&self, source: Source) -> bool {
        match source {
            Source::All => self.crossref_index.is_some() || self.datacite_index.is_some(),
            Source::Crossref => self.crossref_index.is_some() || self.http_fallback_crossref,
            Source::Datacite | Source::Arxiv => {
                self.datacite_index.is_some() || self.http_fallback_datacite
            }
        }
    }
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self::new()
    }
}
