//! Partition selection for segmented storage.
//!
//! Legacy mode keys rows by DOI prefix (or arXiv identifier prefix) through
//! [`partition_key`], so partition size follows prefix popularity. Hashed mode
//! spreads rows over a fixed number of partitions by a stable FNV-1a hash of
//! the complete canonical cited DOI, so one large prefix cannot dominate a
//! partition and the layout is reproducible across runs and machines.

use anyhow::{ensure, Result};

use super::partition_key;

/// Name of the hashing scheme behind [`hashed_partition_key`], recorded in the
/// extraction identity so a resumed run can prove it uses the same layout.
pub const HASH_ALGORITHM: &str = "fnv1a64-v1";

/// How rows are assigned to partition directories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionMode {
    /// DOI-prefix keys from [`partition_key`]; the arXiv pipeline's layout.
    Legacy,
    /// `count` partitions named `h<hex>` by [`hashed_partition_key`].
    Hashed {
        /// Number of partitions; must be a positive power of two.
        count: u32,
    },
}

impl PartitionMode {
    /// Reject configurations the hash cannot serve.
    pub fn validate(&self) -> Result<()> {
        match self {
            PartitionMode::Legacy => Ok(()),
            PartitionMode::Hashed { count } => {
                ensure!(
                    count.is_power_of_two(),
                    "partitions must be a positive power of two (got {count})"
                );
                Ok(())
            }
        }
    }

    /// Partition directory name for a cited identifier.
    pub fn key_for(&self, cited_id: &str) -> Result<String> {
        match self {
            PartitionMode::Legacy => Ok(partition_key(cited_id)),
            PartitionMode::Hashed { count } => hashed_partition_key(cited_id, *count),
        }
    }
}

/// Partition name for a canonical DOI under `count` partitions: `h` followed
/// by the low bits of the 64-bit FNV-1a hash of the DOI bytes, in hex.
///
/// The hash is the documented `fnv1a64-v1` scheme. Inputs are already
/// canonical DOIs; callers must not pass unnormalized identifiers.
pub fn hashed_partition_key(doi: &str, count: u32) -> anyhow::Result<String> {
    anyhow::ensure!(
        count.is_power_of_two(),
        "partitions must be a positive power of two"
    );
    let hash = doi
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |state, byte| {
            (state ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    Ok(format!("h{:04x}", hash & u64::from(count - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published FNV-1a 64-bit vectors: "" -> cbf29ce484222325, "a" ->
    /// af63dc4c8601ec8c. The key keeps the low `log2(count)` bits.
    #[test]
    fn matches_fnv1a_reference_vectors() {
        assert_eq!(hashed_partition_key("", 256).unwrap(), "h0025");
        assert_eq!(hashed_partition_key("a", 256).unwrap(), "h008c");
        assert_eq!(hashed_partition_key("a", 65_536).unwrap(), "hec8c");
        assert_eq!(hashed_partition_key("a", 1).unwrap(), "h0000");
        assert_eq!(hashed_partition_key("", 1).unwrap(), "h0000");
    }

    #[test]
    fn key_is_stable_and_case_sensitive() {
        let first = hashed_partition_key("10.5281/zenodo.123", 1024).unwrap();
        let second = hashed_partition_key("10.5281/zenodo.123", 1024).unwrap();
        assert_eq!(first, second);
        assert_ne!(
            hashed_partition_key("10.5281/zenodo.123", 1024).unwrap(),
            hashed_partition_key("10.5281/zenodo.124", 1024).unwrap()
        );
        assert_ne!(
            hashed_partition_key("10.5281/zenodo.abc", 1024).unwrap(),
            hashed_partition_key("10.5281/zenodo.ABC", 1024).unwrap()
        );
    }

    #[test]
    fn rejects_counts_that_are_not_powers_of_two() {
        for count in [0, 3, 6, 100] {
            assert!(hashed_partition_key("10.1234/a", count).is_err());
            assert!(PartitionMode::Hashed { count }.validate().is_err());
        }
        assert!(PartitionMode::Hashed { count: 1 }.validate().is_ok());
        assert!(PartitionMode::Legacy.validate().is_ok());
    }

    #[test]
    fn legacy_mode_keys_by_prefix() {
        let mode = PartitionMode::Legacy;
        assert_eq!(mode.key_for("10.1234/example").unwrap(), "10.1234");
        assert_eq!(mode.key_for("10.48550/arXiv.2403.12345").unwrap(), "2403");
        let hashed = PartitionMode::Hashed { count: 16 };
        assert_eq!(
            hashed.key_for("10.1234/example").unwrap(),
            hashed_partition_key("10.1234/example", 16).unwrap()
        );
    }

    #[test]
    fn algorithm_name_is_recorded() {
        assert_eq!(HASH_ALGORITHM, "fnv1a64-v1");
    }
}
