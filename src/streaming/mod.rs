pub mod checkpoint;
pub mod partition_invert;
pub mod partition_writer;

pub use checkpoint::*;
pub use partition_invert::*;
pub use partition_writer::*;

/// Extract partition key from an arXiv ID (first 4 chars, lowercased, filesystem-safe).
pub fn partition_key(arxiv_id: &str) -> String {
    arxiv_id
        .to_lowercase()
        .chars()
        .take(4)
        .map(|c| if c == '/' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_modern_format() {
        assert_eq!(partition_key("2403.12345"), "2403");
        assert_eq!(partition_key("2312.00001"), "2312");
        assert_eq!(partition_key("0704.0001"), "0704");
    }

    #[test]
    fn test_partition_key_old_format() {
        assert_eq!(partition_key("hep-ph/9901234"), "hep-");
        assert_eq!(partition_key("cs.dm/9910013"), "cs.d");
        assert_eq!(partition_key("astro-ph/0001001"), "astr");
        assert_eq!(partition_key("cs/9901234"), "cs_9");
        assert_eq!(partition_key("q-bio/0401001"), "q-bi");
    }

    #[test]
    fn test_partition_key_short_id() {
        assert_eq!(partition_key("abc"), "abc");
        assert_eq!(partition_key("a"), "a");
    }
}
