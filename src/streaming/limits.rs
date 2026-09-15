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

#[cfg(test)]
mod tests {
    use super::*;

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
