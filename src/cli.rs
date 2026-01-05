use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "crossref-arxiv-citation-extraction")]
#[command(about = "Unified CLI for extracting, inverting, and validating arXiv references from Crossref data")]
#[command(version = "1.0.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Extract DOIs that reference arXiv works from Crossref snapshot tar.gz files
    Extract(ExtractArgs),

    /// Invert arXiv reference relationships: map arXiv DOIs to citing DOIs
    Invert(InvertArgs),

    /// Validate arXiv citations against DataCite records and DOI resolution
    Validate(ValidateArgs),

    /// Run the full pipeline: extract -> invert -> validate
    Pipeline(PipelineArgs),
}

#[derive(Parser, Clone)]
pub struct ExtractArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// Output JSONL file
    #[arg(short, long, default_value = "arxiv_references.jsonl")]
    pub output: String,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Number of threads to use (0 for auto)
    #[arg(short, long, default_value = "0")]
    pub threads: usize,

    /// Batch size for parallel processing
    #[arg(short, long, default_value = "1000")]
    pub batch_size: usize,

    /// Interval in seconds to log statistics
    #[arg(short, long, default_value = "60")]
    pub stats_interval: u64,
}

#[derive(Parser, Clone)]
pub struct InvertArgs {
    /// Input JSONL file from extract step
    #[arg(short, long, required = true)]
    pub input: String,

    /// Output JSONL file
    #[arg(short, long, default_value = "arxiv_citations.jsonl")]
    pub output: String,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
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

#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file for validation
    #[arg(short, long, required = true)]
    pub records: String,

    /// Output file for valid arXiv citations
    #[arg(short, long, default_value = "arxiv_citations_valid.jsonl")]
    pub output: String,

    /// Also output failed validations to this file (optional)
    #[arg(long)]
    pub output_failed: Option<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Number of threads for extraction (0 for auto)
    #[arg(short, long, default_value = "0")]
    pub threads: usize,

    /// Batch size for parallel extraction
    #[arg(short, long, default_value = "1000")]
    pub batch_size: usize,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,

    /// Keep intermediate files instead of deleting them
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,
}
