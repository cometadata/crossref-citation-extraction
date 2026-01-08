use clap::{Parser, Subcommand};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Source {
    #[default]
    All,
    Crossref,
    Datacite,
    Arxiv,
}

impl FromStr for Source {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" => Ok(Source::All),
            "crossref" => Ok(Source::Crossref),
            "datacite" => Ok(Source::Datacite),
            "arxiv" => Ok(Source::Arxiv),
            _ => Err(format!(
                "Invalid source: {}. Valid options: all, crossref, datacite, arxiv",
                s
            )),
        }
    }
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::All => write!(f, "all"),
            Source::Crossref => write!(f, "crossref"),
            Source::Datacite => write!(f, "datacite"),
            Source::Arxiv => write!(f, "arxiv"),
        }
    }
}

#[derive(Parser)]
#[command(name = "crossref-arxiv-citation-extraction")]
#[command(about = "Extract, invert, and validate arXiv references from Crossref data using fused streaming")]
#[command(version = "2.0.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the full pipeline: fused convert+extract+invert -> validate
    ///
    /// This command streams through the Crossref tar.gz archive, extracts arXiv
    /// references inline, partitions by arXiv ID, and inverts in parallel.
    /// Finally validates against DataCite records.
    Pipeline(PipelineArgs),

    /// Validate arXiv citations against DataCite records and DOI resolution
    ///
    /// Use this to validate a previously generated JSONL file without
    /// re-running the full pipeline.
    Validate(ValidateArgs),
}

#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file for validation
    #[arg(short, long, required = true)]
    pub records: String,

    /// Output file for valid arXiv citations (JSONL)
    #[arg(short, long, default_value = "arxiv_citations_valid.jsonl")]
    pub output: String,

    /// Output file for failed validations (optional)
    #[arg(long)]
    pub output_failed: Option<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,

    /// Keep intermediate files (partitions, temp parquet) instead of deleting them
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate partition files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,

    /// Batch size for memory management during streaming (affects flush frequency)
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,
}

#[derive(Parser, Clone)]
pub struct ValidateArgs {
    /// Input arxiv_citations.jsonl file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file
    #[arg(short, long, required = true)]
    pub records: String,

    /// Output file for valid DOIs
    #[arg(long, default_value = "arxiv_citations_valid.jsonl")]
    pub output_valid: String,

    /// Output file for failed DOIs
    #[arg(long, default_value = "arxiv_citations_failed.jsonl")]
    pub output_failed: String,

    /// Concurrent HTTP requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per request
    #[arg(short, long, default_value = "5")]
    pub timeout: u64,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}
