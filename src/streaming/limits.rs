//! Resource limits for the streaming storage layer.

use anyhow::{ensure, Result};

use super::partitioning::PartitionMode;

/// Limits for [`super::segmented_writer::SegmentedPartitionWriter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterLimits {
    /// Budget for every buffered row across all partitions, in accounted
    /// bytes (string capacities plus column-slot overhead). A row that does
    /// not fit forces the largest buffers to disk first; a row that cannot fit
    /// in an empty writer is an error.
    pub memory_bytes: usize,
    /// Rows one partition buffers before it is flushed as a segment.
    pub segment_rows: usize,
    /// How rows are assigned to partitions.
    pub mode: PartitionMode,
}

impl WriterLimits {
    /// Reject limits under which the writer could never admit a row or would
    /// name partitions it cannot hash.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.memory_bytes > 0,
            "writer memory budget must be positive"
        );
        ensure!(self.segment_rows > 0, "segment_rows must be positive");
        self.mode.validate()
    }
}

/// Limits for full-mode aggregation
/// ([`super::aggregator::aggregate_datacite`] and
/// [`super::ambiguity::aggregate_ambiguities`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationLimits {
    /// Budget for decoded rows held in memory at once: a whole partition
    /// when it is processed in memory, one sorted run, the merge cursors,
    /// or the rows of one cited work while it is emitted. A cited work that
    /// does not fit is an explicit error, never truncated. Must be at least
    /// the extraction writer's budget so every segment can be sorted.
    pub memory_bytes: usize,
    /// Most sorted runs merged in one pass; more runs merge in passes.
    pub merge_fan_in: usize,
    /// Threads sorting segments into runs concurrently, in a dedicated pool.
    pub sort_workers: usize,
}

impl AggregationLimits {
    /// Reject limits under which no row could be held, no merge could make
    /// progress, or no segment could be sorted.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.memory_bytes > 0,
            "aggregation memory budget must be positive"
        );
        ensure!(self.merge_fan_in >= 2, "merge_fan_in must be at least 2");
        ensure!(self.sort_workers >= 1, "sort_workers must be at least 1");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregation_limits_reject_degenerate_values() {
        let good = AggregationLimits {
            memory_bytes: 1,
            merge_fan_in: 2,
            sort_workers: 1,
        };
        assert!(good.validate().is_ok());
        assert!(AggregationLimits {
            memory_bytes: 0,
            ..good
        }
        .validate()
        .is_err());
        assert!(AggregationLimits {
            merge_fan_in: 1,
            ..good
        }
        .validate()
        .is_err());
        assert!(AggregationLimits {
            sort_workers: 0,
            ..good
        }
        .validate()
        .is_err());
    }

    #[test]
    fn validate_rejects_zero_budgets_and_bad_modes() {
        let good = WriterLimits {
            memory_bytes: 1,
            segment_rows: 1,
            mode: PartitionMode::Hashed { count: 2 },
        };
        assert!(good.validate().is_ok());
        assert!(WriterLimits {
            memory_bytes: 0,
            ..good
        }
        .validate()
        .is_err());
        assert!(WriterLimits {
            segment_rows: 0,
            ..good
        }
        .validate()
        .is_err());
        assert!(WriterLimits {
            mode: PartitionMode::Hashed { count: 12 },
            ..good
        }
        .validate()
        .is_err());
    }
}
