//! Input detection and reading for Crossref and DataCite sources.

mod crossref;
mod datacite;
pub mod datacite_release;
mod detection;

pub use crossref::{visit_crossref_entries, visit_crossref_entries_limited, CrossrefEntry};
pub use datacite::open_datacite_source;
pub use detection::{detect_crossref_input, detect_datacite_input, CrossrefInput, DataciteInput};
