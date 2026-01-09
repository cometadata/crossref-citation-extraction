pub mod builder;
pub mod persistence;

pub use builder::*;
pub use persistence::*;

use std::collections::HashSet;

/// DOI index containing DOIs and their prefixes for fast lookup
#[derive(Debug, Clone, Default)]
pub struct DoiIndex {
    /// Set of all DOIs (lowercase)
    pub dois: HashSet<String>,
    /// Set of all DOI prefixes (e.g., "10.1234")
    pub prefixes: HashSet<String>,
}

impl DoiIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(doi_capacity: usize, prefix_capacity: usize) -> Self {
        Self {
            dois: HashSet::with_capacity(doi_capacity),
            prefixes: HashSet::with_capacity(prefix_capacity),
        }
    }

    /// Add a DOI to the index, also tracking its prefix
    pub fn insert(&mut self, doi: &str) {
        let doi_lower = doi.to_lowercase();
        if let Some(prefix) = crate::extract::doi_prefix(&doi_lower) {
            self.prefixes.insert(prefix);
        }
        self.dois.insert(doi_lower);
    }

    /// Check if a DOI exists in the index
    pub fn contains(&self, doi: &str) -> bool {
        self.dois.contains(&doi.to_lowercase())
    }

    /// Check if a prefix exists in the index
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.prefixes.contains(&prefix.to_lowercase())
    }

    /// Get count of DOIs
    pub fn len(&self) -> usize {
        self.dois.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.dois.is_empty()
    }

    /// Get count of unique prefixes
    pub fn prefix_count(&self) -> usize {
        self.prefixes.len()
    }

    /// Merge another index into this one
    pub fn merge(&mut self, other: DoiIndex) {
        self.dois.extend(other.dois);
        self.prefixes.extend(other.prefixes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doi_index_insert_and_contains() {
        let mut index = DoiIndex::new();
        index.insert("10.1234/example");

        assert!(index.contains("10.1234/example"));
        assert!(index.contains("10.1234/EXAMPLE")); // Case insensitive
        assert!(!index.contains("10.5678/other"));
    }

    #[test]
    fn test_doi_index_prefix_tracking() {
        let mut index = DoiIndex::new();
        index.insert("10.1234/example1");
        index.insert("10.1234/example2");
        index.insert("10.5678/other");

        assert!(index.has_prefix("10.1234"));
        assert!(index.has_prefix("10.5678"));
        assert!(!index.has_prefix("10.9999"));
        assert_eq!(index.prefix_count(), 2);
    }

    #[test]
    fn test_doi_index_merge() {
        let mut index1 = DoiIndex::new();
        index1.insert("10.1234/a");

        let mut index2 = DoiIndex::new();
        index2.insert("10.5678/b");

        index1.merge(index2);

        assert!(index1.contains("10.1234/a"));
        assert!(index1.contains("10.5678/b"));
        assert_eq!(index1.len(), 2);
    }
}
