use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputType {
    Valid,
    Failed,
    Publisher,
    Crossref,
    Mined,
}

impl FromStr for OutputType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "valid" => Ok(OutputType::Valid),
            "failed" => Ok(OutputType::Failed),
            "publisher" => Ok(OutputType::Publisher),
            "crossref" => Ok(OutputType::Crossref),
            "mined" => Ok(OutputType::Mined),
            _ => Err(format!(
                "Invalid output type: '{}'. Valid options: valid, failed, publisher, crossref, mined",
                s
            )),
        }
    }
}

#[derive(Parser)]
#[command(name = "crossref-citation-extraction")]
#[command(about = "Extract, invert, and validate DOI references from Crossref data")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the pipeline: extract arXiv IDs, aggregate by cited work, validate
    ///
    /// Streams through the Crossref tar.gz archive, extracts arXiv references,
    /// partitions by arXiv ID prefix, aggregates in parallel, and validates against
    /// arXiv index.
    Pipeline(Box<PipelineArgs>),

    /// Build FST index from arXiv (DataCite) records
    BuildIndex {
        /// Input path (tar.gz, directory, or JSONL file)
        #[arg(short, long)]
        input: String,

        /// Output FST file path
        #[arg(short, long)]
        output: String,

        /// Log level
        #[arg(long, default_value = "INFO")]
        log_level: String,
    },

    /// Build the memory-mapped DataCite DOI and landing-page URL index bundle
    ///
    /// Scans a complete, verified DataCite monthly release once, externally
    /// sorts its DOI, host and URL/DOI keys, and writes an immutable bundle
    /// (`dois.fst`, `hosts.fst`, `urls.fst`, `url-targets.bin`,
    /// `_INDEX_COMPLETE.json`) into a new output directory.
    BuildDataciteIndex {
        /// DataCite monthly release root (holds MANIFEST.json, STATUS.json and dois/)
        #[arg(short, long)]
        input: String,

        /// Bundle directory to create; must not already hold a completed bundle
        #[arg(short, long)]
        output: String,

        /// Memory budget for the external sort, in MiB
        #[arg(
            long,
            default_value_t = 4096,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        sort_memory_mb: usize,

        /// Log level
        #[arg(long, default_value = "INFO")]
        log_level: String,
    },
}

#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// arXiv records.jsonl.gz file for validation
    #[arg(long)]
    pub arxiv_records: Option<String>,

    /// Provenance filter for extraction (comma-separated: publisher,crossref,mined)
    /// Only extract DOIs with matching provenance. Default: all provenances if not specified.
    #[arg(long, value_delimiter = ',')]
    pub provenance: Vec<String>,

    /// Output types to generate (comma-separated: valid,failed,publisher,crossref,mined)
    /// Default: all outputs if not specified
    #[arg(long, value_delimiter = ',')]
    pub outputs: Vec<String>,

    /// Directory for output files (default: current directory)
    #[arg(long)]
    pub output_dir: Option<String>,

    /// Path to existing partition directory (skips extraction, runs aggregation only)
    /// Use this to re-run aggregation with different output selections
    #[arg(long)]
    pub partitions_dir: Option<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Keep intermediate files (partitions, temp parquet)
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate partition files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,

    /// Batch size for memory management during streaming
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,

    /// Path to pre-built arXiv FST index
    #[arg(long)]
    pub arxiv_fst: Option<String>,

    /// Resume from checkpoint if available
    #[arg(long, default_value = "false")]
    pub resume: bool,

    /// Checkpoint interval: save progress every N partitions during aggregation.
    /// Values > 1 widen the window in which a crash duplicates already-written
    /// partitions on resume; 0 disables checkpointing.
    #[arg(long, default_value = "1")]
    pub checkpoint_interval: usize,

    /// DataCite index bundle directory (full DOI and landing-page mining).
    /// Selects DataCite mode; cannot be combined with --arxiv-fst or
    /// --arxiv-records.
    #[arg(long)]
    pub datacite_index: Option<String>,

    /// JSON file holding the run identity object (DataCite mode). Its
    /// canonical digest names every resumable artifact; without it a
    /// derived identity is used for local runs.
    #[arg(long)]
    pub run_identity: Option<String>,

    /// Hashed partition count in DataCite mode (a positive power of two)
    #[arg(
        long,
        default_value_t = 256,
        value_parser = clap::builder::RangedU64ValueParser::<u32>::new().range(1..)
    )]
    pub partitions: u32,

    /// Partition writer buffer budget in MiB (DataCite mode)
    #[arg(
        long,
        default_value_t = 4096,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub writer_memory_mb: usize,

    /// Largest worker result batch in MiB (DataCite mode)
    #[arg(
        long,
        default_value_t = 16,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub result_batch_mb: usize,

    /// Result batches buffered between workers and the writer (DataCite mode)
    #[arg(
        long,
        default_value_t = 8,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub queue_batches: usize,

    /// Aggregation memory budget in MiB (DataCite mode); at least
    /// --writer-memory-mb
    #[arg(
        long,
        default_value_t = 16384,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub aggregation_memory_mb: usize,

    /// Most sorted runs merged in one pass during aggregation (DataCite mode)
    #[arg(
        long,
        default_value_t = 32,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(2..)
    )]
    pub merge_fan_in: usize,

    /// Threads sorting segments concurrently during aggregation (DataCite mode)
    #[arg(
        long,
        default_value_t = 4,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub sort_workers: usize,

    /// Budget for one landing-page candidate lookup in MiB (DataCite mode)
    #[arg(
        long,
        default_value_t = 64,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub lookup_memory_mb: usize,

    /// Largest Crossref member accepted in MiB (DataCite mode); a larger
    /// member is a fatal source error
    #[arg(
        long,
        default_value_t = 512,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    pub max_entry_mb: usize,
}

impl PipelineArgs {
    /// Parse outputs into a HashSet. Supports the "all" alias; empty means all.
    pub fn parse_outputs(&self) -> Result<HashSet<OutputType>, String> {
        let all = [
            OutputType::Valid,
            OutputType::Failed,
            OutputType::Publisher,
            OutputType::Crossref,
            OutputType::Mined,
        ];
        if self.outputs.is_empty() || self.outputs.iter().any(|s| s.eq_ignore_ascii_case("all")) {
            return Ok(all.into_iter().collect());
        }
        self.outputs
            .iter()
            .map(|s| s.parse::<OutputType>())
            .collect()
    }

    /// Check if a provenance should be included based on the filter
    pub fn should_include_provenance(&self, provenance: &str) -> bool {
        should_include_provenance_filter(&self.provenance, provenance)
    }
}

/// Check if a provenance passes the (possibly empty) filter list.
///
/// Free-function twin of [`PipelineArgs::should_include_provenance`], usable
/// from worker threads that hold only a cloned filter list, not the args.
pub fn should_include_provenance_filter(filter: &[String], provenance: &str) -> bool {
    filter.is_empty() || filter.iter().any(|p| p.eq_ignore_ascii_case(provenance))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_type_from_str() {
        assert_eq!("valid".parse::<OutputType>().unwrap(), OutputType::Valid);
        assert_eq!("failed".parse::<OutputType>().unwrap(), OutputType::Failed);
        assert_eq!(
            "publisher".parse::<OutputType>().unwrap(),
            OutputType::Publisher
        );
        assert_eq!(
            "crossref".parse::<OutputType>().unwrap(),
            OutputType::Crossref
        );
        assert_eq!("mined".parse::<OutputType>().unwrap(), OutputType::Mined);
        assert!("invalid".parse::<OutputType>().is_err());
    }

    #[test]
    fn test_output_type_case_insensitive() {
        assert_eq!("VALID".parse::<OutputType>().unwrap(), OutputType::Valid);
        assert_eq!("Valid".parse::<OutputType>().unwrap(), OutputType::Valid);
    }

    fn args_with_outputs(outputs: Vec<&str>) -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            arxiv_records: None,
            arxiv_fst: None,
            provenance: vec![],
            outputs: outputs.into_iter().map(String::from).collect(),
            output_dir: None,
            partitions_dir: None,
            log_level: "INFO".to_string(),
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
            resume: false,
            checkpoint_interval: 1,
            datacite_index: None,
            run_identity: None,
            partitions: 256,
            writer_memory_mb: 4096,
            result_batch_mb: 16,
            queue_batches: 8,
            aggregation_memory_mb: 16384,
            merge_fan_in: 32,
            sort_workers: 4,
            lookup_memory_mb: 64,
            max_entry_mb: 512,
        }
    }

    #[test]
    fn test_datacite_mode_flags_default_and_reject_zero() {
        use clap::Parser;
        let args = PipelineArgs::parse_from(["prog", "--input", "x.tar.gz"]);
        assert!(args.datacite_index.is_none());
        assert_eq!(args.partitions, 256);
        assert_eq!(args.writer_memory_mb, 4096);
        assert_eq!(args.result_batch_mb, 16);
        assert_eq!(args.queue_batches, 8);
        assert_eq!(args.aggregation_memory_mb, 16384);
        assert_eq!(args.merge_fan_in, 32);
        assert_eq!(args.sort_workers, 4);
        assert_eq!(args.lookup_memory_mb, 64);
        assert_eq!(args.max_entry_mb, 512);
        for flag in [
            "--partitions",
            "--writer-memory-mb",
            "--result-batch-mb",
            "--queue-batches",
            "--aggregation-memory-mb",
            "--sort-workers",
            "--lookup-memory-mb",
            "--max-entry-mb",
        ] {
            assert!(
                PipelineArgs::try_parse_from(["prog", "--input", "x", flag, "0"]).is_err(),
                "{flag} must reject zero"
            );
        }
        assert!(
            PipelineArgs::try_parse_from(["prog", "--input", "x", "--merge-fan-in", "1"]).is_err()
        );
    }

    #[test]
    fn test_outputs_all_alias() {
        let outputs = args_with_outputs(vec!["all"]).parse_outputs().unwrap();
        assert!(outputs.contains(&OutputType::Valid));
        assert!(outputs.contains(&OutputType::Failed));
        assert!(outputs.contains(&OutputType::Publisher));
        assert!(outputs.contains(&OutputType::Crossref));
        assert!(outputs.contains(&OutputType::Mined));
    }

    #[test]
    fn test_outputs_specific_selection_is_honored() {
        let outputs = args_with_outputs(vec!["valid"]).parse_outputs().unwrap();
        assert_eq!(outputs.len(), 1);
        assert!(outputs.contains(&OutputType::Valid));

        let outputs = args_with_outputs(vec!["valid", "mined"])
            .parse_outputs()
            .unwrap();
        assert_eq!(outputs.len(), 2);
        assert!(outputs.contains(&OutputType::Valid));
        assert!(outputs.contains(&OutputType::Mined));
    }

    #[test]
    fn test_outputs_invalid_value_errors() {
        let err = args_with_outputs(vec!["valid", "bogus"])
            .parse_outputs()
            .unwrap_err();
        assert!(err.contains("bogus"));
    }

    #[test]
    fn test_checkpoint_interval_defaults_to_one() {
        use clap::Parser;
        let args = PipelineArgs::parse_from(["prog", "--input", "x.tar.gz"]);
        assert_eq!(args.checkpoint_interval, 1);
    }
}
