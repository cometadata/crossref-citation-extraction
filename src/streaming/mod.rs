pub mod aggregator;
pub mod ambiguity;
pub mod checkpoint;
pub mod datacite_aggregator;
pub mod kway_merge;
pub mod limits;
pub mod partition_commit;
pub mod partitioning;
pub mod segmented_writer;

pub use aggregator::aggregate_and_validate;
pub use ambiguity::aggregate_ambiguities;
pub use checkpoint::ExtractionCheckpoint;
pub use datacite_aggregator::aggregate_datacite;
pub use limits::{AggregationLimits, WriterLimits};
pub use partition_commit::PartitionCommit;
pub use partitioning::{hashed_partition_key, PartitionMode};
pub use segmented_writer::{PartitionRow, SegmentedPartitionWriter};

/// Extract partition key from a DOI or arXiv ID.
/// arXiv DOIs (10.48550/arXiv.<id>) key on the arXiv ID portion so the
/// corpus spreads across many partitions instead of one.
/// Other DOIs use their prefix; bare arXiv IDs use their first 4 chars.
pub fn partition_key(id: &str) -> String {
    const ARXIV_DOI_PREFIX: &str = "10.48550/arxiv.";
    let lower = id.to_lowercase();
    let effective = lower.strip_prefix(ARXIV_DOI_PREFIX).unwrap_or(&lower);

    if effective.starts_with("10.") {
        if let Some(slash_pos) = effective.find('/') {
            return effective[..slash_pos].to_string();
        }
    }

    effective
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

    #[test]
    fn test_partition_key_arxiv_doi_uses_id_suffix() {
        assert_eq!(partition_key("10.48550/arXiv.2403.12345"), "2403");
        assert_eq!(partition_key("10.48550/arxiv.2403.12345"), "2403");
        assert_eq!(partition_key("10.48550/arXiv.hep-ph/9901234"), "hep-");
        assert_eq!(partition_key("10.48550/arXiv.cs/9901234"), "cs_9");
    }

    #[test]
    fn test_partition_key_non_arxiv_doi_keeps_prefix() {
        assert_eq!(partition_key("10.1234/example"), "10.1234");
        assert_eq!(partition_key("10.5555/abcd.1234"), "10.5555");
    }

    #[test]
    fn test_partition_key_doi_no_slash() {
        assert_eq!(partition_key("10.1"), "10.1");
    }
}
